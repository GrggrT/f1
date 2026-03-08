from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler

from config import settings

logger = logging.getLogger(__name__)


async def _run():
    from telegram import Bot
    from data.database import Database
    from services.budget import get_driver_name

    db = Database(settings.DATABASE_URL)
    await db.connect()
    bot = Bot(token=settings.BOT_TOKEN)

    try:
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        # Get all upcoming races
        async with db.pool.acquire() as conn:
            races = await conn.fetch(
                "SELECT * FROM races WHERE race_datetime > $1 ORDER BY round ASC LIMIT 2",
                (now - timedelta(days=2)).isoformat(),
            )

        if not races:
            return

        for race_row in races:
            race_round = race_row["round"]
            race_name = race_row["name"]
            quali_dt = datetime.fromisoformat(race_row["qualifying_datetime"])
            race_dt = datetime.fromisoformat(race_row["race_datetime"])

            events_to_check = [
                ("thu_preview", race_dt - timedelta(days=3, hours=-10), race_dt - timedelta(days=3, hours=-10, minutes=-15)),
                ("24h_warning", quali_dt - timedelta(hours=24), quali_dt - timedelta(hours=24) + timedelta(minutes=15)),
                ("3h_dm", quali_dt - timedelta(hours=3), quali_dt - timedelta(hours=3) + timedelta(minutes=15)),
                ("1h_warning", quali_dt - timedelta(hours=1), quali_dt - timedelta(hours=1) + timedelta(minutes=15)),
                ("lock", quali_dt, quali_dt + timedelta(minutes=15)),
                ("race_1h", race_dt - timedelta(hours=1), race_dt - timedelta(hours=1) + timedelta(minutes=15)),
            ]

            for event_type, window_start, window_end in events_to_check:
                if window_start <= now < window_end:
                    # Check if already fired
                    async with db.pool.acquire() as conn:
                        existing = await conn.fetchrow(
                            "SELECT id FROM cron_events_log WHERE event_type = $1 AND race_round = $2",
                            event_type, race_round,
                        )
                        if existing:
                            continue
                        # Mark as fired
                        await conn.execute(
                            "INSERT INTO cron_events_log (event_type, race_round) VALUES ($1, $2)",
                            event_type, race_round,
                        )

                    # Fire the event
                    await _fire_event(bot, db, event_type, race_round, race_name, quali_dt, race_dt)

    finally:
        await db.close()
        await bot.shutdown()


async def _fire_event(bot, db, event_type, race_round, race_name, quali_dt, race_dt):
    from services.budget import get_driver_name
    from utils.formatters import format_race_info

    async def send_groups(text):
        for chat_id in settings.GROUP_CHAT_IDS:
            try:
                await bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
            except Exception:
                logger.exception("Failed to send to group %s", chat_id)

    if event_type == "thu_preview":
        race = await db.get_race(race_round)
        if race:
            text = f"\U0001f3c1 *Race Weekend!*\n\n{format_race_info(race)}"
            await send_groups(text)

    elif event_type == "24h_warning":
        text = (
            f"\u23f0 *24 hours until team deadline!*\n\n"
            f"{race_name}\nDeadline: qualifying start\n\U0001f449 /pickteam"
        )
        await send_groups(text)

    elif event_type == "3h_dm":
        # Send personal DM reminders
        no_team = await db.get_users_without_team(race_round)
        for user in no_team:
            try:
                await bot.send_message(
                    chat_id=user["telegram_id"],
                    text=(
                        f"\u23f0 *Reminder: 3 hours until deadline!*\n\n"
                        f"{race_name} \u2014 you haven't submitted a team yet.\n"
                        f"\U0001f449 /pickteam"
                    ),
                    parse_mode="Markdown",
                )
            except Exception:
                pass

        no_pred = await db.get_users_without_prediction(race_round)
        for user in no_pred:
            try:
                await bot.send_message(
                    chat_id=user["telegram_id"],
                    text=(
                        f"\U0001f3af *Don't forget predictions!*\n\n"
                        f"{race_name} \u2014 predictions close at race start.\n"
                        f"\U0001f449 /predict"
                    ),
                    parse_mode="Markdown",
                )
            except Exception:
                pass

    elif event_type == "1h_warning":
        users = await db.get_all_users()
        teams = await db.get_all_teams_for_round(race_round)
        team_user_ids = {t.user_id for t in teams}
        missing = [
            f"@{u['username']}" if u.get("username") else u.get("display_name", "???")
            for u in users if u["telegram_id"] not in team_user_ids
        ]
        shame = ""
        if missing:
            shame = f"\n\n\u26a0\ufe0f \u0415\u0449\u0451 \u043d\u0435 \u043e\u0431\u043d\u043e\u0432\u0438\u043b\u0438 \u043a\u043e\u043c\u0430\u043d\u0434\u0443:\n{', '.join(missing)}"
        text = (
            f"\U0001f6a8 *DEADLINE THROUGH 1 HOUR!*\n"
            f"{race_name}{shame}\n\n\U0001f449 /pickteam NOW!"
        )
        await send_groups(text)

    elif event_type == "lock":
        teams = await db.get_all_teams_for_round(race_round)
        if not teams:
            await send_groups(f"\U0001f512 Deadline! No teams submitted for {race_name}.")
            return
        lines = [f"\U0001f512 *Deadline! Teams locked for {race_name}*\n"]
        for t in teams:
            username = f"@{t.username}" if t.username else str(t.user_id)
            drivers = ", ".join(get_driver_name(d) for d in t.drivers)
            turbo = get_driver_name(t.turbo_driver)
            lines.append(f"\n*{username}:*")
            lines.append(f"  \U0001f3ce {drivers}")
            lines.append(f"  \u26a1 Turbo: {turbo}")
        await send_groups("\n".join(lines))

    elif event_type == "race_1h":
        text = (
            f"\U0001f3ce *Race in 1 hour!*\n\n"
            f"{race_name}\n\U0001f525 Good luck everyone!"
        )
        await send_groups(text)


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        # Verify cron secret
        auth = self.headers.get("Authorization", "")
        if settings.CRON_SECRET and auth != f"Bearer {settings.CRON_SECRET}":
            self.send_response(401)
            self.end_headers()
            return

        import asyncio
        try:
            asyncio.get_event_loop().run_until_complete(_run())
        except Exception:
            logger.exception("Cron race_events failed")

        self.send_response(200)
        self.end_headers()
