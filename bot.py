from datetime import datetime, timezone

from telegram import BotCommand, Update
from telegram.ext import Application

from config import settings
from utils.logging_config import get_logger, setup_logging
from data.api_client import F1DataService
from data.database import Database
from data.f1_calendar import load_calendar
from handlers.admin import setup_admin_handlers
from handlers.chips import setup_chips_handlers
from handlers.extras import setup_extras_handlers
from handlers.nextrace import setup_nextrace_handlers
from handlers.predict import setup_predict_handlers
from handlers.results import setup_results_handlers
from handlers.standings import setup_standings_handlers
from handlers.start import setup_start_handlers
from handlers.survivor import setup_survivor_handlers
from handlers.team import setup_team_handlers
from jobs.reminders import schedule_race_weekend

setup_logging()
logger = get_logger(__name__)


async def post_init(app: Application) -> None:
    # Database
    db = Database(settings.DB_PATH)
    await db.connect()
    app.bot_data["db"] = db
    app.bot_data["start_time"] = datetime.now(timezone.utc)
    logger.info("Database initialized at %s", settings.DB_PATH)

    # F1 Data Service
    f1_data = F1DataService()
    app.bot_data["f1_data"] = f1_data

    # Set bot commands for Telegram menu button
    await app.bot.set_my_commands([
        BotCommand("start", "\u0413\u043b\u0430\u0432\u043d\u043e\u0435 \u043c\u0435\u043d\u044e"),
        BotCommand("pickteam", "\u0421\u043e\u0431\u0440\u0430\u0442\u044c \u043a\u043e\u043c\u0430\u043d\u0434\u0443"),
        BotCommand("myteam", "\u041c\u043e\u044f \u043a\u043e\u043c\u0430\u043d\u0434\u0430"),
        BotCommand("standings", "\u0422\u0430\u0431\u043b\u0438\u0446\u0430 \u043b\u0438\u0433\u0438"),
        BotCommand("nextrace", "\u0421\u043b\u0435\u0434\u0443\u044e\u0449\u0430\u044f \u0433\u043e\u043d\u043a\u0430"),
        BotCommand("predict", "\u041f\u0440\u043e\u0433\u043d\u043e\u0437\u044b"),
        BotCommand("survivor", "Survivor Pool"),
        BotCommand("chips", "\u0427\u0438\u043f\u044b"),
        BotCommand("results", "\u0420\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442\u044b"),
        BotCommand("prices", "\u0426\u0435\u043d\u044b \u043f\u0438\u043b\u043e\u0442\u043e\u0432"),
        BotCommand("predstandings", "\u0422\u0430\u0431\u043b\u0438\u0446\u0430 \u043f\u0440\u043e\u0433\u043d\u043e\u0437\u043e\u0432"),
        BotCommand("history", "\u0418\u0441\u0442\u043e\u0440\u0438\u044f"),
        BotCommand("chart", "\u0413\u0440\u0430\u0444\u0438\u043a \u043e\u0447\u043a\u043e\u0432"),
        BotCommand("menu", "\u0411\u044b\u0441\u0442\u0440\u043e\u0435 \u043c\u0435\u043d\u044e"),
        BotCommand("rules", "\u041f\u0440\u0430\u0432\u0438\u043b\u0430"),
        BotCommand("help", "\u041f\u043e\u043c\u043e\u0449\u044c"),
    ])
    logger.info("Bot commands set")

    # Load calendar and schedule jobs
    try:
        races = await load_calendar(db, f1_data)
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        scheduled = 0
        for race in races:
            race_dt = datetime.fromisoformat(race.race_datetime)
            if race_dt > now:
                schedule_race_weekend(app.job_queue, race)
                scheduled += 1
        logger.info("Scheduled jobs for %d upcoming races", scheduled)
    except Exception:
        logger.exception("Failed to load calendar — will retry on first command")


async def post_shutdown(app: Application) -> None:
    db: Database | None = app.bot_data.get("db")
    if db:
        await db.close()
        logger.info("Database connection closed")

    f1_data: F1DataService | None = app.bot_data.get("f1_data")
    if f1_data:
        await f1_data.close()
        logger.info("F1 API clients closed")


def main() -> None:
    app = (
        Application.builder()
        .token(settings.BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    setup_start_handlers(app)
    setup_team_handlers(app)
    setup_chips_handlers(app)
    setup_standings_handlers(app)
    setup_results_handlers(app)
    setup_nextrace_handlers(app)
    setup_predict_handlers(app)
    setup_survivor_handlers(app)
    setup_extras_handlers(app)
    setup_admin_handlers(app)

    logger.info("F1 Fantasy Bot starting...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
