from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from telegram.ext import CallbackContext

from config import settings
from data.database import Database
from data.models import Race
from jobs.results_poller import poll_fast_results, validate_results
from services.budget import get_driver_name
from utils.formatters import format_race_info, format_team_summary

logger = logging.getLogger(__name__)


async def _send_group(context: CallbackContext, text: str) -> None:
    if settings.GROUP_CHAT_ID:
        try:
            await context.bot.send_message(
                chat_id=settings.GROUP_CHAT_ID,
                text=text,
                parse_mode="Markdown",
            )
        except Exception:
            logger.exception("Failed to send group message")


def schedule_race_weekend(context_or_queue, race: Race) -> None:
    """Register all scheduled jobs for a race weekend."""
    # Accept either a job_queue or context with job_queue
    if hasattr(context_or_queue, "job_queue"):
        jq = context_or_queue.job_queue
    else:
        jq = context_or_queue

    quali_dt = datetime.fromisoformat(race.qualifying_datetime)
    race_dt = datetime.fromisoformat(race.race_datetime)
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    prefix = f"r{race.round}"

    # 1. Thursday preview (3 days before race, 10:00 UTC)
    thursday = race_dt - timedelta(days=3)
    thursday = thursday.replace(hour=10, minute=0, second=0)
    if thursday > now:
        jq.run_once(
            _thursday_preview,
            when=(thursday - now).total_seconds(),
            data={"race_round": race.round},
            name=f"{prefix}_thu",
        )

    # 2. Friday prediction open (2 days before race, 18:00 UTC)
    friday = race_dt - timedelta(days=2)
    friday = friday.replace(hour=18, minute=0, second=0)
    if friday > now:
        jq.run_once(
            _friday_predictions,
            when=(friday - now).total_seconds(),
            data={"race_round": race.round},
            name=f"{prefix}_fri",
        )

    # 3. 24h before qualifying
    warn_24h = quali_dt - timedelta(hours=24)
    if warn_24h > now:
        jq.run_once(
            _deadline_24h,
            when=(warn_24h - now).total_seconds(),
            data={"race_round": race.round},
            name=f"{prefix}_24h",
        )

    # 4a. 3h before qualifying — personal DM reminders
    warn_3h = quali_dt - timedelta(hours=3)
    if warn_3h > now:
        jq.run_once(
            _deadline_dm_reminders,
            when=(warn_3h - now).total_seconds(),
            data={"race_round": race.round},
            name=f"{prefix}_3h_dm",
        )

    # 4b. 1h before qualifying
    warn_1h = quali_dt - timedelta(hours=1)
    if warn_1h > now:
        jq.run_once(
            _deadline_1h,
            when=(warn_1h - now).total_seconds(),
            data={"race_round": race.round},
            name=f"{prefix}_1h",
        )

    # 5. Qualifying deadline — lock + reveal teams
    if quali_dt > now:
        jq.run_once(
            _deadline_lock,
            when=(quali_dt - now).total_seconds(),
            data={"race_round": race.round},
            name=f"{prefix}_lock",
        )

    # 6. 1h before race
    race_1h = race_dt - timedelta(hours=1)
    if race_1h > now:
        jq.run_once(
            _race_reminder,
            when=(race_1h - now).total_seconds(),
            data={"race_round": race.round},
            name=f"{prefix}_race1h",
        )

    # 7. 35 min after race — start polling fast results (OpenF1)
    poll_start = race_dt + timedelta(minutes=35)
    if poll_start > now:
        jq.run_once(
            poll_fast_results,
            when=(poll_start - now).total_seconds(),
            data={"race_round": race.round, "attempt": 0, "session_name": "Race"},
            name=f"{prefix}_poll",
        )

    # 7b. For sprint weekends: also poll sprint results
    job_count = 8
    if race.sprint:
        # Sprint is typically Saturday, estimate as race_dt - 1 day
        sprint_dt = race_dt - timedelta(days=1)
        sprint_poll = sprint_dt + timedelta(minutes=35)
        if sprint_poll > now:
            jq.run_once(
                poll_fast_results,
                when=(sprint_poll - now).total_seconds(),
                data={"race_round": race.round, "attempt": 0, "session_name": "Sprint"},
                name=f"{prefix}_sprint_poll",
            )
            job_count += 1

    # 8. Monday morning summary
    monday = race_dt + timedelta(days=1)
    monday = monday.replace(hour=10, minute=0, second=0)
    if monday > now:
        jq.run_once(
            _monday_summary,
            when=(monday - now).total_seconds(),
            data={"race_round": race.round},
            name=f"{prefix}_mon",
        )

    # 9. Monday 12:00 UTC — cross-validation via Jolpica
    monday_validate = race_dt + timedelta(days=1)
    monday_validate = monday_validate.replace(hour=12, minute=0, second=0)
    if monday_validate > now:
        jq.run_once(
            validate_results,
            when=(monday_validate - now).total_seconds(),
            data={"race_round": race.round},
            name=f"{prefix}_validate",
        )
        job_count += 1

    logger.info("Scheduled %d jobs for Round %d (%s)", job_count, race.round, race.name)


# ── Job callbacks ──

async def _thursday_preview(context: CallbackContext) -> None:
    race_round = context.job.data["race_round"]
    db: Database = context.bot_data["db"]
    race = await db.get_race(race_round)
    if not race:
        return

    text = (
        f"\U0001f3c1 *Race Weekend!*\n\n"
        f"{format_race_info(race)}"
    )
    await _send_group(context, text)


async def _friday_predictions(context: CallbackContext) -> None:
    race_round = context.job.data["race_round"]
    db: Database = context.bot_data["db"]
    race = await db.get_race(race_round)
    if not race:
        return

    text = (
        f"\U0001f4ca *Prediction game open!*\n\n"
        f"{race.name} \u2014 7 questions await.\n"
        f"\U0001f449 /predict"
    )
    await _send_group(context, text)


async def _deadline_24h(context: CallbackContext) -> None:
    race_round = context.job.data["race_round"]
    db: Database = context.bot_data["db"]
    race = await db.get_race(race_round)
    if not race:
        return

    text = (
        f"\u23f0 *24 hours until team deadline!*\n\n"
        f"{race.name}\n"
        f"Deadline: qualifying start\n"
        f"\U0001f449 /pickteam"
    )
    await _send_group(context, text)


async def _deadline_1h(context: CallbackContext) -> None:
    race_round = context.job.data["race_round"]
    db: Database = context.bot_data["db"]
    race = await db.get_race(race_round)
    if not race:
        return

    # Shame list — users without a team for this round
    users = await db.get_all_users()
    teams = await db.get_all_teams_for_round(race_round)
    team_user_ids = {t.user_id for t in teams}

    missing = [
        f"@{u['username']}" if u.get("username") else u.get("display_name", "???")
        for u in users
        if u["telegram_id"] not in team_user_ids
    ]

    shame = ""
    if missing:
        shame = f"\n\n\u26a0\ufe0f Ещё не обновили команду:\n{', '.join(missing)}"

    text = (
        f"\U0001f6a8 *DEADLINE THROUGH 1 HOUR!*\n"
        f"{race.name}{shame}\n\n"
        f"\U0001f449 /pickteam NOW!"
    )
    await _send_group(context, text)


async def _deadline_lock(context: CallbackContext) -> None:
    race_round = context.job.data["race_round"]
    db: Database = context.bot_data["db"]
    race = await db.get_race(race_round)
    if not race:
        return

    teams = await db.get_all_teams_for_round(race_round)
    if not teams:
        await _send_group(context, f"\U0001f512 Deadline! No teams submitted for {race.name}.")
        return

    lines = [f"\U0001f512 *Deadline! Teams locked for {race.name}*\n"]

    for t in teams:
        username = f"@{t.username}" if t.username else str(t.user_id)
        drivers = ", ".join(get_driver_name(d) for d in t.drivers)
        turbo = get_driver_name(t.turbo_driver)
        lines.append(f"\n*{username}:*")
        lines.append(f"  \U0001f3ce {drivers}")
        lines.append(f"  \u26a1 Turbo: {turbo}")

    await _send_group(context, "\n".join(lines))


async def _race_reminder(context: CallbackContext) -> None:
    race_round = context.job.data["race_round"]
    db: Database = context.bot_data["db"]
    race = await db.get_race(race_round)
    if not race:
        return

    text = (
        f"\U0001f3ce *Race in 1 hour!*\n\n"
        f"{race.name}\n"
        f"\U0001f525 Good luck everyone!"
    )
    await _send_group(context, text)


async def _deadline_dm_reminders(context: CallbackContext) -> None:
    """Send personal DM reminders to users who haven't submitted team/predictions."""
    race_round = context.job.data["race_round"]
    db: Database = context.bot_data["db"]
    await db.ensure_connected()
    race = await db.get_race(race_round)
    if not race:
        return

    # Users without team
    no_team = await db.get_users_without_team(race_round)
    for user in no_team:
        try:
            await context.bot.send_message(
                chat_id=user["telegram_id"],
                text=(
                    f"\u23f0 *Reminder: 3 hours until deadline!*\n\n"
                    f"{race.name} \u2014 you haven't submitted a team yet.\n"
                    f"\U0001f449 /pickteam"
                ),
                parse_mode="Markdown",
            )
        except Exception:
            logger.debug("Could not DM user %s", user["telegram_id"])

    # Users without predictions
    no_pred = await db.get_users_without_prediction(race_round)
    for user in no_pred:
        try:
            await context.bot.send_message(
                chat_id=user["telegram_id"],
                text=(
                    f"\U0001f3af *Don't forget predictions!*\n\n"
                    f"{race.name} \u2014 predictions close at race start.\n"
                    f"\U0001f449 /predict"
                ),
                parse_mode="Markdown",
            )
        except Exception:
            logger.debug("Could not DM user %s", user["telegram_id"])

    sent_count = len(no_team) + len(no_pred)
    if sent_count > 0:
        logger.info("Sent %d deadline DM reminders for round %d", sent_count, race_round)


async def _monday_summary(context: CallbackContext) -> None:
    """Post comprehensive weekly summary."""
    from jobs.weekly_content import post_race_summary
    race_round = context.job.data["race_round"]
    await post_race_summary(context, race_round)
