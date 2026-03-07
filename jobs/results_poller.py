from __future__ import annotations

import asyncio
import logging

from telegram.ext import CallbackContext

from config import settings
from data.api_client import F1DataService
from data.database import Database
from data.models import RaceResult, RaceResultsBundle
from services.budget import get_driver_name
from services.scoring import calculate_team_score
from services.transfers import TransferService
from utils.formatters import format_standings_table

logger = logging.getLogger(__name__)

MAX_POLL_ATTEMPTS = 40  # ~3.5 hours at 5 min intervals
POLL_INTERVAL = 300  # 5 minutes


# ── Phase 1: Fast results (OpenF1, race_datetime + 35 min) ──


async def poll_fast_results(context: CallbackContext) -> None:
    """Phase 1: Poll OpenF1 for fast results after race."""
    job_data = context.job.data or {}
    race_round = job_data.get("race_round")
    attempt = job_data.get("attempt", 0)
    session_name = job_data.get("session_name", "Race")  # "Race" or "Sprint"

    if race_round is None:
        logger.error("poll_fast_results called without race_round")
        return

    db: Database = context.bot_data["db"]
    await db.ensure_connected()
    f1_data: F1DataService = context.bot_data.get("f1_data")
    if not f1_data:
        f1_data = F1DataService()
        context.bot_data["f1_data"] = f1_data
    year = settings.SEASON_YEAR

    # Try to get fast results from OpenF1
    bundle = await f1_data.get_fast_race_results(year, race_round)

    if bundle is None:
        if attempt >= MAX_POLL_ATTEMPTS:
            # Fallback: notify group that results are delayed
            logger.warning("Max poll attempts for round %d, falling back to Jolpica Monday", race_round)
            if settings.GROUP_CHAT_ID:
                await context.bot.send_message(
                    chat_id=settings.GROUP_CHAT_ID,
                    text="\u23f3 \u0420\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442\u044b \u0437\u0430\u0434\u0435\u0440\u0436\u0438\u0432\u0430\u044e\u0442\u0441\u044f. \u041f\u043e\u043b\u043d\u044b\u0435 \u043e\u0447\u043a\u0438 \u0431\u0443\u0434\u0443\u0442 \u0432 \u043f\u043e\u043d\u0435\u0434\u0435\u043b\u044c\u043d\u0438\u043a.",
                )
            return

        logger.info("No OpenF1 results for round %d (attempt %d/%d)", race_round, attempt + 1, MAX_POLL_ATTEMPTS)
        context.job_queue.run_once(
            poll_fast_results,
            when=POLL_INTERVAL,
            data={**job_data, "attempt": attempt + 1},
            name=f"poll_r{race_round}_{session_name}",
        )
        return

    # Got results! Process and publish
    logger.info("Got OpenF1 results for round %d (%s), processing...", race_round, session_name)
    await process_and_publish(context, race_round, bundle)


# ── Shared scoring & publishing logic ──


async def process_and_publish(context: CallbackContext, race_round: int, bundle: RaceResultsBundle) -> None:
    """Score all teams and publish results to group."""
    db: Database = context.bot_data["db"]

    # 1. Convert bundle results to legacy format and save to DB
    results_dicts = []
    for dr in bundle.results:
        results_dicts.append({
            "round": race_round,
            "driver_id": dr.driver_id,
            "grid_position": dr.grid,
            "finish_position": dr.position,
            "dnf": dr.status not in ("Finished", "") and not dr.status.startswith("+"),
            "fastest_lap": dr.fastest_lap_rank == 1,
            "points_scored": 0.0,
        })
    await db.save_race_results(results_dicts)

    # 2. Convert to RaceResult list for scoring engine
    race_results = [
        RaceResult(
            round=race_round,
            driver_id=dr.driver_id,
            grid_position=dr.grid,
            finish_position=dr.position,
            dnf=dr.status not in ("Finished", "") and not dr.status.startswith("+"),
            fastest_lap=dr.fastest_lap_rank == 1,
        )
        for dr in bundle.results
    ]

    # 3. Convert qualifying to dict format for scoring
    quali_results = [
        {"driver_id": q.driver_id, "position": q.position, "q3": q.q3}
        for q in bundle.qualifying
    ]

    # 4. Convert sprint to dict format
    sprint_results = None
    if bundle.sprint:
        sprint_results = [
            {"driver_id": s.driver_id, "position": s.position, "grid": s.grid, "status": s.status}
            for s in bundle.sprint
        ]

    # 5. Convert pit stops to dict format
    pit_stops = [
        {"driver_id": p.driver_id, "stop": p.stop_number, "duration": str(p.duration_seconds)}
        for p in bundle.pit_stops
    ]

    # 6. Score each team
    teams = await db.get_all_teams_for_round(race_round)
    if not teams:
        # Try previous round teams as carryover
        # (users who didn't update get previous round's team)
        logger.info("No teams found for round %d", race_round)
        pass  # Just log, no teams = no scores

    transfer_svc = TransferService(db)

    user_scores = []
    for team in teams:
        active_chip = await db.get_active_chip(team.user_id, race_round)
        penalty = await transfer_svc.get_transfer_penalty(team.user_id, race_round)

        breakdown = calculate_team_score(
            team, race_results, quali_results, sprint_results, pit_stops,
            active_chip=active_chip,
            transfer_penalty=penalty,
        )

        await db.save_score(team.user_id, race_round, breakdown["total"], breakdown)
        user_scores.append({
            "user_id": team.user_id,
            "username": team.username,
            "fantasy_points": breakdown["total"],
            "breakdown": breakdown,
        })

    user_scores.sort(key=lambda x: x["fantasy_points"], reverse=True)

    # 7. Post to group
    if settings.GROUP_CHAT_ID:
        await _post_results_to_group(context, race_round, bundle, user_scores)


# ── Phase 2: Cross-validation (Jolpica, Monday 12:00 UTC) ──


async def validate_results(context: CallbackContext) -> None:
    """Phase 2: Cross-validate with Jolpica data on Monday."""
    job_data = context.job.data or {}
    race_round = job_data.get("race_round")
    if not race_round:
        return

    db: Database = context.bot_data["db"]
    f1_data: F1DataService = context.bot_data.get("f1_data")
    if not f1_data:
        f1_data = F1DataService()
        context.bot_data["f1_data"] = f1_data
    year = settings.SEASON_YEAR

    # Check if we already have results for this round
    existing = await db.get_race_results(race_round)

    # Get Jolpica results
    bundle = await f1_data.get_validated_results(year, race_round)

    if bundle is None and not existing:
        # No OpenF1 results AND no Jolpica yet - try again later
        logger.info("No results from either source for round %d", race_round)
        return

    if bundle is None:
        # No discrepancies or Jolpica not ready
        logger.info("Round %d validated OK (or Jolpica not ready)", race_round)
        return

    if not existing:
        # OpenF1 failed, but Jolpica has data now - do full scoring
        logger.info("Using Jolpica as primary source for round %d", race_round)
        await process_and_publish(context, race_round, bundle)
        return

    if bundle.needs_rescore:
        # Discrepancies found - rescore
        logger.warning("Discrepancies found for round %d, rescoring", race_round)
        await process_and_publish(context, race_round, bundle)

        if settings.GROUP_CHAT_ID:
            await context.bot.send_message(
                chat_id=settings.GROUP_CHAT_ID,
                text=(
                    f"\u26a0\ufe0f *\u041e\u0411\u041d\u041e\u0412\u041b\u0415\u041d\u0418\u0415 Round {race_round}*\n\n"
                    "\u0421\u0442\u044e\u0430\u0440\u0434\u044b \u043f\u0440\u0438\u043d\u044f\u043b\u0438 \u0440\u0435\u0448\u0435\u043d\u0438\u0435 \u2014 \u043e\u0447\u043a\u0438 \u043f\u0435\u0440\u0435\u0441\u0447\u0438\u0442\u0430\u043d\u044b.\n"
                    "\U0001f4ca /standings \u0434\u043b\u044f \u043e\u0431\u043d\u043e\u0432\u043b\u0451\u043d\u043d\u043e\u0439 \u0442\u0430\u0431\u043b\u0438\u0446\u044b"
                ),
                parse_mode="Markdown",
            )


# ── Group chat posting ──


async def _post_results_to_group(
    context: CallbackContext,
    race_round: int,
    bundle: RaceResultsBundle,
    user_scores: list[dict],
) -> None:
    """Post formatted results to group chat."""
    chat_id = settings.GROUP_CHAT_ID
    db: Database = context.bot_data["db"]
    race = await db.get_race(race_round)
    race_name = race.name if race else f"Round {race_round}"

    # Race results top-10
    race_lines = [f"\U0001f3c1 *{race_name} \u2014 Results*\n"]
    race_lines.append("```")
    for dr in bundle.results[:10]:
        pos = dr.position if dr.position else "DNF"
        name = get_driver_name(dr.driver_id)
        fl = " FL" if dr.fastest_lap_rank == 1 else ""
        race_lines.append(f"P{pos:<3} {name}{fl}")
    race_lines.append("```")

    try:
        await context.bot.send_message(
            chat_id=chat_id,
            text="\n".join(race_lines),
            parse_mode="Markdown",
        )
    except Exception:
        logger.exception("Failed to post race results")

    await asyncio.sleep(3)

    # Fantasy scores
    score_lines = [f"\U0001f4ca *Fantasy Scores \u2014 Round {race_round}*\n"]
    score_lines.append("```")
    score_lines.append(f"{'#':>2} {'Manager':<14} {'Pts':>6}")
    score_lines.append("-" * 24)
    for i, s in enumerate(user_scores, 1):
        username = s["username"] or str(s["user_id"])
        score_lines.append(f"{i:>2} @{username:<13} {s['fantasy_points']:>6.0f}")
    score_lines.append("```")

    try:
        await context.bot.send_message(
            chat_id=chat_id,
            text="\n".join(score_lines),
            parse_mode="Markdown",
        )
    except Exception:
        logger.exception("Failed to post fantasy scores")

    await asyncio.sleep(3)

    # Updated season standings
    standings = await db.get_standings()
    standings_text = format_standings_table(standings)

    try:
        await context.bot.send_message(
            chat_id=chat_id,
            text=standings_text,
            parse_mode="Markdown",
        )
    except Exception:
        logger.exception("Failed to post standings")
