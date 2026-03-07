from __future__ import annotations

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from data.database import Database
from utils.formatters import format_race_info


def _get_db(context: ContextTypes.DEFAULT_TYPE) -> Database:
    return context.bot_data["db"]


async def nextrace_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db = _get_db(context)
    race = await db.get_next_race()

    if race is None:
        await update.message.reply_text("Нет запланированных гонок.")
        return

    text = format_race_info(race)
    await update.message.reply_text(text, parse_mode="Markdown")


def setup_nextrace_handlers(app: Application) -> None:
    app.add_handler(CommandHandler("nextrace", nextrace_command))
