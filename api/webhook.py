from __future__ import annotations

import asyncio
import json
import logging
import traceback

from http.server import BaseHTTPRequestHandler

from config import settings

logger = logging.getLogger(__name__)

# Persistent event loop and Application singleton (survive across warm invocations)
_loop = None
_app = None


def _get_loop():
    """Get or create a persistent event loop for the lifetime of this worker."""
    global _loop
    if _loop is None or _loop.is_closed():
        _loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_loop)
    return _loop


async def _get_app():
    """Lazily initialize the Telegram Application with all handlers."""
    global _app
    if _app is not None:
        return _app

    from datetime import datetime, timezone
    from telegram.ext import Application
    from data.database import Database
    from data.api_client import F1DataService
    from handlers.admin import setup_admin_handlers
    from handlers.chips import setup_chips_handlers
    from handlers.extras import setup_extras_handlers
    from handlers.h2h import setup_h2h_handlers
    from handlers.nextrace import setup_nextrace_handlers
    from handlers.predict import setup_predict_handlers
    from handlers.results import setup_results_handlers
    from handlers.share import setup_share_handlers
    from handlers.standings import setup_standings_handlers
    from handlers.start import setup_start_handlers
    from handlers.survivor import setup_survivor_handlers
    from handlers.team import setup_team_handlers

    app = Application.builder().token(settings.BOT_TOKEN).build()

    # Database
    db = Database(settings.DATABASE_URL)
    await db.connect()
    app.bot_data["db"] = db
    app.bot_data["start_time"] = datetime.now(timezone.utc)

    # F1 Data Service
    f1_data = F1DataService()
    app.bot_data["f1_data"] = f1_data

    # Load race calendar
    from data.f1_calendar import load_calendar
    try:
        await load_calendar(db, f1_data)
    except Exception:
        logger.exception("Failed to load calendar on init")

    # Set bot commands for Telegram menu
    from telegram import BotCommand
    try:
        await app.bot.set_my_commands([
            BotCommand("start", "Главное меню"),
            BotCommand("pickteam", "Собрать команду"),
            BotCommand("myteam", "Моя команда"),
            BotCommand("standings", "Таблица лиги"),
            BotCommand("nextrace", "Следующая гонка"),
            BotCommand("predict", "Прогнозы"),
            BotCommand("survivor", "Survivor Pool"),
            BotCommand("survivor_standings", "Таблица Survivor"),
            BotCommand("rival", "H2H дуэли"),
            BotCommand("h2h", "Статистика H2H"),
            BotCommand("driver", "Статистика пилота"),
            BotCommand("chips", "Чипы"),
            BotCommand("transfer", "Трансфер"),
            BotCommand("results", "Результаты"),
            BotCommand("prices", "Цены пилотов"),
            BotCommand("predstandings", "Таблица прогнозов"),
            BotCommand("history", "История"),
            BotCommand("chart", "График очков"),
            BotCommand("menu", "Быстрое меню"),
            BotCommand("rules", "Правила"),
            BotCommand("help", "Помощь"),
        ])
    except Exception:
        logger.exception("Failed to set bot commands")

    # Register all handlers
    setup_start_handlers(app)
    setup_team_handlers(app)
    setup_chips_handlers(app)
    setup_standings_handlers(app)
    setup_results_handlers(app)
    setup_nextrace_handlers(app)
    setup_predict_handlers(app)
    setup_survivor_handlers(app)
    setup_h2h_handlers(app)
    setup_extras_handlers(app)
    setup_share_handlers(app)
    setup_admin_handlers(app)

    # Global error handler — log and notify user
    async def error_handler(update, context):
        logger.exception("Unhandled exception: %s", context.error)
        if update and update.effective_chat:
            try:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="⚠️ Произошла ошибка. Попробуй ещё раз.",
                )
            except Exception:
                pass

    app.add_error_handler(error_handler)

    await app.initialize()
    _app = app
    logger.info("Application initialized for webhook mode")
    return app


async def _check_pending_results(app) -> None:
    """Check if any recent race needs results polled. Runs in background."""
    from datetime import datetime, timezone, timedelta
    db = app.bot_data["db"]
    f1_data = app.bot_data["f1_data"]

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    async with db.pool.acquire() as conn:
        races = await conn.fetch(
            """SELECT round, name, race_datetime FROM races
               WHERE race_datetime < $1 AND race_datetime > $2
               ORDER BY round DESC LIMIT 1""",
            (now - timedelta(hours=1)).isoformat(),
            (now - timedelta(days=3)).isoformat(),
        )
    for race_row in races:
        race_round = race_row["round"]
        async with db.pool.acquire() as conn:
            scored = await conn.fetchrow(
                "SELECT id FROM scores WHERE race_round = $1 LIMIT 1", race_round
            )
        if scored:
            continue
        # Check at most once per 10 minutes (use cron_events_log)
        check_key = f"webhook_poll_{race_round}"
        async with db.pool.acquire() as conn:
            recent = await conn.fetchrow(
                "SELECT fired_at FROM cron_events_log WHERE event_type = $1 AND race_round = $2",
                check_key, race_round,
            )
            if recent:
                fired = recent["fired_at"]
                if fired and (now - fired.replace(tzinfo=None)).total_seconds() < 600:
                    continue
                # Update timestamp
                await conn.execute(
                    "UPDATE cron_events_log SET fired_at = NOW() WHERE event_type = $1 AND race_round = $2",
                    check_key, race_round,
                )
            else:
                await conn.execute(
                    "INSERT INTO cron_events_log (event_type, race_round) VALUES ($1, $2)",
                    check_key, race_round,
                )

        logger.info("Auto-checking results for round %d via webhook", race_round)
        from jobs.results_poller import poll_and_score
        try:
            await poll_and_score(app.bot, db, f1_data, race_round)
        except Exception:
            logger.exception("Webhook auto-poll failed for round %d", race_round)


async def _process(body: bytes) -> None:
    from telegram import Update
    app = await _get_app()

    # Ensure DB connection is healthy (reconnect if stale)
    db = app.bot_data["db"]
    await db.ensure_connected()

    data = json.loads(body)
    update = Update.de_json(data, app.bot)
    await app.process_update(update)

    # Check for pending results in background (non-blocking for user)
    try:
        await _check_pending_results(app)
    except Exception:
        logger.exception("Background results check failed")


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        # Verify webhook secret
        secret = self.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if settings.WEBHOOK_SECRET and secret != settings.WEBHOOK_SECRET:
            self.send_response(403)
            self.end_headers()
            return

        # Read body
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)

        try:
            loop = _get_loop()
            loop.run_until_complete(_process(body))
        except Exception:
            logger.exception("Webhook processing failed")
            traceback.print_exc()

        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        """Health check endpoint."""
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"status": "ok"}).encode())
