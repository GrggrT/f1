from __future__ import annotations

import logging

from telegram.ext import CallbackContext

from config import settings
from data.database import Database
from services.awards import AwardsEngine
from services.predictions import PredictionService
from services.budget import get_driver_name
from services.survivor_logic import SurvivorService
from utils.formatters import format_awards, format_standings_table

logger = logging.getLogger(__name__)


async def _send_group(context: CallbackContext, text: str) -> None:
    for chat_id in settings.GROUP_CHAT_IDS:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode="Markdown",
            )
        except Exception:
            logger.exception("Failed to send group message to %s", chat_id)


async def post_race_summary(context: CallbackContext, race_round: int) -> None:
    """Post comprehensive post-race summary to group."""
    db: Database = context.bot_data["db"]
    await db.ensure_connected()
    race = await db.get_race(race_round)
    race_name = race.name if race else f"Round {race_round}"

    # 1. Season standings
    standings = await db.get_standings()
    if standings:
        standings_text = format_standings_table(standings)
        await _send_group(context, standings_text)

    # 2. Awards
    awards_engine = AwardsEngine(db)
    awards = await awards_engine.calculate_round_awards(race_round)
    if awards:
        awards_text = f"\U0001f3c6 *{race_name} \u2014 Awards*\n\n"
        awards_text += format_awards(awards)
        await _send_group(context, awards_text)

    # 3. H2H update
    h2h_text = await awards_engine.generate_h2h_update()
    if h2h_text:
        await _send_group(context, h2h_text)

    # 4. Prediction game results
    await _post_prediction_results(context, race_round)

    # 5. Survivor update
    await _post_survivor_update(context, race_round)

    # 6. Update driver prices
    await _update_and_post_prices(context, race_round)


async def _post_prediction_results(context: CallbackContext, race_round: int) -> None:
    """Score and post prediction results."""
    db: Database = context.bot_data["db"]
    predictions = await db.get_predictions(race_round)
    if not predictions:
        return

    # Get race results for resolving questions
    race_results_raw = await db.get_race_results(race_round)
    if not race_results_raw:
        return

    from data.models import RaceResult
    race_results = [
        RaceResult(
            round=r["round"],
            driver_id=r["driver_id"],
            grid_position=r["grid_position"],
            finish_position=r.get("finish_position"),
            dnf=bool(r.get("dnf", False)),
            fastest_lap=bool(r.get("fastest_lap", False)),
        )
        for r in race_results_raw
    ]

    # Get pit stop data for prediction resolution
    pit_stops_raw = []
    f1_data = context.bot_data.get("f1_data")
    if f1_data:
        from config import settings
        try:
            pit_stops_raw = await f1_data.jolpica.get_pit_stops(settings.SEASON_YEAR, race_round)
        except Exception:
            logger.warning("Could not fetch pit stops for prediction resolution")

    pred_service = PredictionService()
    race = await db.get_race(race_round)
    questions = pred_service.generate_questions(race, race_round)
    actuals = pred_service.resolve_questions(questions, race_results, pit_stops=pit_stops_raw)

    results = []
    for pred in predictions:
        correct, total, _ = pred_service.score_predictions(
            pred.questions, actuals
        )
        await db.save_prediction_score(pred.user_id, race_round, correct, total)
        user = await db.get_user(pred.user_id)
        username = user["username"] if user and user.get("username") else str(pred.user_id)
        results.append((username, correct, total))

    results.sort(key=lambda x: -x[2])

    lines = ["\U0001f3af *Prediction Results*\n"]
    lines.append("```")
    lines.append(f"{'#':>2} {'Manager':<14} {'Correct':>7} {'Pts':>4}")
    lines.append("-" * 30)
    for i, (name, correct, total) in enumerate(results, 1):
        lines.append(f"{i:>2} @{name:<13} {correct:>5}/7 {total:>4}")
    lines.append("```")

    await _send_group(context, "\n".join(lines))

    # Post per-question actuals
    if questions:
        actual_lines = ["\U0001f4cb *Prediction Answers*\n"]
        for q in questions:
            qid = q["id"]
            actual = actuals.get(qid, False)
            emoji = "\u2705" if actual else "\u274c"
            actual_lines.append(f"{emoji} Q{qid}: {q.get('text', '?')}")
        await _send_group(context, "\n".join(actual_lines))


async def _post_survivor_update(context: CallbackContext, race_round: int) -> None:
    """Evaluate and post survivor results."""
    db: Database = context.bot_data["db"]

    race_results_raw = await db.get_race_results(race_round)
    if not race_results_raw:
        return

    from data.models import RaceResult

    race_results = [
        RaceResult(
            round=r["round"],
            driver_id=r["driver_id"],
            grid_position=r["grid_position"],
            finish_position=r.get("finish_position"),
            dnf=bool(r.get("dnf", False)),
            fastest_lap=bool(r.get("fastest_lap", False)),
        )
        for r in race_results_raw
    ]

    svc = SurvivorService(db)
    updates = await svc.evaluate_picks(race_round, race_results)
    if not updates:
        return

    lines = ["\U0001f480 *Survivor Update*\n"]
    for u in updates:
        username = f"@{u['username']}" if u["username"] else str(u["user_id"])
        driver_name = get_driver_name(u["driver_id"])
        pos = u["finish_position"]
        pos_str = f"P{pos}" if pos else "DNF"

        if u["is_zombie"]:
            emoji = "\U0001f47b"
            status = f"zombie picked {driver_name} {pos_str}"
        elif u["survived"]:
            emoji = "\u2705"
            status = f"survived ({driver_name} {pos_str})"
        else:
            emoji = "\u274c"
            status = f"ELIMINATED ({driver_name} {pos_str})"

        lines.append(f"{emoji} {username} {status}")

    await _send_group(context, "\n".join(lines))


async def _update_and_post_prices(context: CallbackContext, race_round: int) -> None:
    """Update prices based on performance and post changes."""
    db: Database = context.bot_data["db"]

    try:
        from jobs.price_updater import update_prices_after_race
        changes = await update_prices_after_race(db, race_round)

        if changes:
            lines = ["\U0001f4b0 *Price Changes*\n"]
            for driver_id, new_price in sorted(changes.items(), key=lambda x: x[1], reverse=True):
                name = get_driver_name(driver_id)
                lines.append(f"  {name}: ${new_price:.1f}M")
            await _send_group(context, "\n".join(lines))
    except Exception:
        logger.exception("Failed to update prices after round %d", race_round)


async def post_midweek_content(context: CallbackContext) -> None:
    """Wednesday midweek content post."""
    db: Database = context.bot_data["db"]

    # Get latest scores
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT MAX(race_round) as max_round FROM scores")
    if not row or not row["max_round"]:
        return

    last_round = row["max_round"]
    standings = await db.get_standings()
    if not standings:
        return

    race = await db.get_next_race()
    next_info = ""
    if race:
        next_info = f"\n\n\U0001f3c1 Next: *{race.name}* \u2014 /pickteam | /predict | /survivor"

    # Find most undervalued driver (best points/price ratio)
    from services.budget import get_all_drivers, get_driver_name
    scores = await db.get_race_scores(last_round)
    if scores:
        # Simple stat
        top_score = scores[0] if scores else None
        user = await db.get_user(top_score.user_id) if top_score else None
        top_name = f"@{user['username']}" if user and user.get("username") else "???"

        text = (
            f"\U0001f4ca *Midweek Stats*\n\n"
            f"\U0001f3c6 Last round winner: {top_name} ({top_score.fantasy_points:.0f} pts)\n"
            f"\U0001f4c8 Season leader: @{standings[0][1]} ({standings[0][2]:.0f} pts)"
            f"{next_info}"
        )
        await _send_group(context, text)
