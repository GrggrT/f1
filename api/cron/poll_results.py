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
    from data.api_client import F1DataService
    from jobs.results_poller import poll_and_score

    db = Database(settings.DATABASE_URL)
    await db.connect()
    bot = Bot(token=settings.BOT_TOKEN)
    f1_data = F1DataService()

    try:
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        # Find races that ended 35min-4h ago and haven't been scored
        async with db.pool.acquire() as conn:
            races = await conn.fetch(
                """SELECT r.round, r.name, r.race_datetime, r.sprint
                   FROM races r
                   WHERE r.race_datetime < $1
                     AND r.race_datetime > $2
                   ORDER BY r.round DESC""",
                (now - timedelta(minutes=35)).isoformat(),
                (now - timedelta(hours=6)).isoformat(),
            )

        for race_row in races:
            race_round = race_row["round"]
            # Check if already scored
            async with db.pool.acquire() as conn:
                scored = await conn.fetchrow(
                    "SELECT id FROM scores WHERE race_round = $1 LIMIT 1",
                    race_round,
                )
                if scored:
                    continue

                # Check poll attempt count
                attempt_row = await conn.fetchrow(
                    "SELECT attempt_count FROM poll_state WHERE race_round = $1 AND session_name = $2",
                    race_round, "Race",
                )
                attempt = attempt_row["attempt_count"] if attempt_row else 0

                if attempt >= settings.MAX_POLL_ATTEMPTS:
                    continue

                # Increment attempt
                await conn.execute(
                    """INSERT INTO poll_state (race_round, session_name, attempt_count)
                       VALUES ($1, $2, $3)
                       ON CONFLICT(race_round, session_name) DO UPDATE SET
                         attempt_count = poll_state.attempt_count + 1""",
                    race_round, "Race", attempt + 1,
                )

            # Try to poll results
            try:
                await poll_and_score(bot, db, f1_data, race_round, "Race")
            except Exception:
                logger.exception("Failed to poll results for round %d", race_round)

    finally:
        await db.close()
        await f1_data.close()
        await bot.shutdown()


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
            logger.exception("Cron poll_results failed")

        self.send_response(200)
        self.end_headers()
