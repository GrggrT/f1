from __future__ import annotations

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from data.database import Database
from utils.formatters import format_standings_table


def _get_db(context: ContextTypes.DEFAULT_TYPE) -> Database:
    return context.bot_data["db"]


async def standings_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db = _get_db(context)
    standings = await db.get_standings()

    if not standings:
        await update.message.reply_text("Ещё нет результатов. Standings появятся после первой гонки.")
        return

    text = format_standings_table(standings, highlight_user_id=update.effective_user.id)
    await update.message.reply_text(text, parse_mode="Markdown")


def setup_standings_handlers(app: Application) -> None:
    app.add_handler(CommandHandler("standings", standings_command))
