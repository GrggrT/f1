from __future__ import annotations

import json
import logging

from telegram import Update
from telegram.ext import Application, CallbackQueryHandler, ContextTypes

from config import settings
from data.database import Database
from services.budget import get_driver_name, get_constructor_name

logger = logging.getLogger(__name__)


def _get_db(context: ContextTypes.DEFAULT_TYPE) -> Database:
    return context.bot_data["db"]


async def share_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle share-to-group button presses."""
    query = update.callback_query
    await query.answer()

    parts = query.data.split(":")
    if len(parts) < 3:
        return

    share_type = parts[1]  # team, predict, results
    try:
        race_round = int(parts[2])
    except ValueError:
        return

    db = _get_db(context)
    user_id = update.effective_user.id
    username = update.effective_user.username
    display = f"@{username}" if username else update.effective_user.full_name

    text = None

    if share_type == "team":
        team = await db.get_team(user_id, race_round)
        if not team:
            team = await db.get_latest_team(user_id)
        if team:
            drivers_str = ", ".join(get_driver_name(d) for d in team.drivers)
            turbo = get_driver_name(team.turbo_driver)
            constructor = get_constructor_name(team.constructor)
            text = (
                f"🏎 *{display} — команда Round {team.race_round}*\n\n"
                f"👨‍✈️ {drivers_str}\n"
                f"🏗 {constructor}\n"
                f"⚡ DRS: {turbo}\n"
                f"💰 ${team.budget_remaining:.1f}M"
            )

    elif share_type == "predict":
        pred = await db.get_prediction(user_id, race_round)
        if pred and pred.questions:
            questions_data = pred.questions
            if isinstance(questions_data, str):
                questions_data = json.loads(questions_data)
            count = len(questions_data)
            text = f"🎯 *{display} сделал прогнозы на Round {race_round}!*\n{count} вопросов отвечено"

    elif share_type == "results":
        scores = await db.get_race_scores(race_round)
        user_score = None
        for s in scores:
            if s.user_id == user_id:
                user_score = s
                break
        if user_score:
            race = await db.get_race(race_round)
            race_name = race.name if race else f"Round {race_round}"
            # Find rank
            rank = next((i+1 for i, s in enumerate(scores) if s.user_id == user_id), "?")
            text = (
                f"📊 *{display} — {race_name}*\n\n"
                f"🏆 Место: #{rank}\n"
                f"📈 Очки: {user_score.fantasy_points:.0f}"
            )

    if text is None:
        await query.edit_message_text("Нет данных для отправки в группу.")
        return

    # Send to all groups
    logger.info("Sharing %s to %d groups: %s", share_type, len(settings.GROUP_CHAT_IDS), settings.GROUP_CHAT_IDS)
    sent = False
    for chat_id in settings.GROUP_CHAT_IDS:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode="Markdown",
            )
            sent = True
        except Exception:
            logger.exception("Could not share to group %s", chat_id)

    if sent:
        # Update the original message to show it was shared
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="✅ Отправлено в группу!",
        )
    else:
        await query.edit_message_text("❌ Не удалось отправить в группу.")


def setup_share_handlers(app: Application) -> None:
    app.add_handler(CallbackQueryHandler(share_callback, pattern=r"^share:"))
