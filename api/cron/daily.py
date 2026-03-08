from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler

from config import settings

logger = logging.getLogger(__name__)


async def _run():
    from telegram import Bot
    from data.database import Database

    db = Database(settings.DATABASE_URL)
    await db.connect()
    bot = Bot(token=settings.BOT_TOKEN)

    try:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        hour = now.hour

        async with db.pool.acquire() as conn:
            races = await conn.fetch(
                "SELECT * FROM races ORDER BY round ASC"
            )

        if not races:
            return

        for race_row in races:
            race_round = race_row["round"]
            race_name = race_row["name"]
            race_dt = datetime.fromisoformat(race_row["race_datetime"])

            # Thursday preview (10:00 UTC, 3 days before race)
            if hour == 10:
                thu = race_dt - timedelta(days=3)
                if thu.date() == now.date():
                    await _fire_if_new(db, bot, "thu_daily", race_round, race_name)

            # Friday predictions (18:00 UTC, 2 days before race)
            if hour == 18:
                fri = race_dt - timedelta(days=2)
                if fri.date() == now.date():
                    await _fire_pred_open(db, bot, race_round, race_name)

            # Monday summary (10:00 UTC, day after race)
            if hour == 10:
                mon = race_dt + timedelta(days=1)
                if mon.date() == now.date():
                    await _fire_monday_summary(db, bot, race_round)

            # Monday validate (12:00 UTC, day after race)
            if hour == 12:
                mon = race_dt + timedelta(days=1)
                if mon.date() == now.date():
                    await _fire_monday_validate(db, bot, race_round)

            # Wednesday midweek (10:00 UTC, 3 days after race)
            if hour == 10:
                wed = race_dt + timedelta(days=3)
                if wed.date() == now.date():
                    await _fire_midweek(db, bot, race_round, race_dt)

    finally:
        await db.close()
        await bot.shutdown()


async def _send_groups(bot, text):
    for chat_id in settings.GROUP_CHAT_IDS:
        try:
            await bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
        except Exception:
            logger.exception("Failed to send to group %s", chat_id)


async def _check_fired(db, event_type, race_round) -> bool:
    async with db.pool.acquire() as conn:
        existing = await conn.fetchrow(
            "SELECT id FROM cron_events_log WHERE event_type = $1 AND race_round = $2",
            event_type, race_round,
        )
        if existing:
            return True
        await conn.execute(
            "INSERT INTO cron_events_log (event_type, race_round) VALUES ($1, $2)",
            event_type, race_round,
        )
        return False


async def _fire_if_new(db, bot, event_type, race_round, race_name):
    if await _check_fired(db, event_type, race_round):
        return
    from utils.formatters import format_race_info
    race = await db.get_race(race_round)
    if race:
        text = f"\U0001f3c1 *Race Weekend!*\n\n{format_race_info(race)}"
        await _send_groups(bot, text)


async def _fire_pred_open(db, bot, race_round, race_name):
    if await _check_fired(db, "fri_predictions", race_round):
        return
    text = (
        f"\U0001f4ca *Prediction game open!*\n\n"
        f"{race_name} \u2014 7 questions await.\n\U0001f449 /predict"
    )
    await _send_groups(bot, text)


async def _fire_monday_summary(db, bot, race_round):
    if await _check_fired(db, "mon_summary", race_round):
        return
    from jobs.weekly_content import generate_post_race_summary
    text = await generate_post_race_summary(db, race_round)
    if text:
        await _send_groups(bot, text)


async def _fire_monday_validate(db, bot, race_round):
    if await _check_fired(db, "mon_validate", race_round):
        return
    from data.api_client import F1DataService
    from jobs.results_poller import validate_and_report
    f1_data = F1DataService()
    try:
        report = await validate_and_report(db, f1_data, race_round)
        if report:
            await _send_groups(bot, report)
    finally:
        await f1_data.close()


async def _fire_midweek(db, bot, race_round, race_dt):
    if await _check_fired(db, "wed_midweek", race_round):
        return
    # Skip if next race is less than 5 days away
    next_race = await db.get_next_race()
    if next_race:
        next_dt = datetime.fromisoformat(next_race.race_datetime)
        if (next_dt - race_dt).days < 5:
            return
    from jobs.weekly_content import generate_midweek_content
    text = await generate_midweek_content(db)
    if text:
        await _send_groups(bot, text)


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        auth = self.headers.get("Authorization", "")
        if settings.CRON_SECRET and auth != f"Bearer {settings.CRON_SECRET}":
            self.send_response(401)
            self.end_headers()
            return

        import asyncio
        try:
            asyncio.get_event_loop().run_until_complete(_run())
        except Exception:
            logger.exception("Cron daily failed")

        self.send_response(200)
        self.end_headers()
