from __future__ import annotations

import asyncio
import json
import logging
import traceback

from http.server import BaseHTTPRequestHandler

from config import settings

logger = logging.getLogger(__name__)

# Lazy-initialized Application singleton (persists across warm invocations)
_app = None


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
    setup_admin_handlers(app)

    await app.initialize()
    _app = app
    logger.info("Application initialized for webhook mode")
    return app


async def _process(body: bytes) -> None:
    from telegram import Update
    app = await _get_app()
    data = json.loads(body)
    update = Update.de_json(data, app.bot)
    await app.process_update(update)


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
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(_process(body))
            loop.close()
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
