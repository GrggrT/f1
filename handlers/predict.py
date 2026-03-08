from __future__ import annotations

import logging
from datetime import datetime, timezone

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from data.database import Database
from data.models import Prediction
from services.predictions import PredictionService
from utils.keyboards import (
    ALL_MENU_TEXTS,
    MENU_PREDICTIONS,
    build_confidence_keyboard,
    build_prediction_keyboard,
)

logger = logging.getLogger(__name__)

ASK_ANSWER, ASK_CONFIDENCE = range(2)
prediction_service = PredictionService()

OTHER_MENU_TEXTS = [t for t in ALL_MENU_TEXTS if t != MENU_PREDICTIONS]


def _get_db(context: ContextTypes.DEFAULT_TYPE) -> Database:
    return context.bot_data["db"]


async def predict_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Redirect to DM in group chat."""
    if update.effective_chat.type == "private":
        return await predict_start(update, context)

    bot_username = (await context.bot.get_me()).username
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "\U0001f3af Prediction Game",
            url=f"https://t.me/{bot_username}?start=predict",
        )]
    ])
    await update.message.reply_text(
        "\u0421\u0434\u0435\u043b\u0430\u0439 \u043f\u0440\u043e\u0433\u043d\u043e\u0437\u044b \u0432 \u043b\u0438\u0447\u043a\u0435 \U0001f447",
        reply_markup=keyboard,
    )


async def predict_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start prediction flow in DM."""
    db = _get_db(context)
    race = await db.get_next_race()

    if not race:
        await update.message.reply_text("\u041d\u0435\u0442 \u0437\u0430\u043f\u043b\u0430\u043d\u0438\u0440\u043e\u0432\u0430\u043d\u043d\u044b\u0445 \u0433\u043e\u043d\u043e\u043a.")
        return ConversationHandler.END

    # Deadline = race start (not quali)
    deadline = datetime.fromisoformat(race.race_datetime)
    if datetime.now(timezone.utc).replace(tzinfo=None) > deadline:
        await update.message.reply_text(
            "\u26a0\ufe0f \u0414\u0435\u0434\u043b\u0430\u0439\u043d \u043f\u0440\u043e\u0433\u043d\u043e\u0437\u043e\u0432 \u043f\u0440\u043e\u0448\u0451\u043b (\u0441\u0442\u0430\u0440\u0442 \u0433\u043e\u043d\u043a\u0438)!"
        )
        return ConversationHandler.END

    # Auto-register if needed
    user = await db.get_user(update.effective_user.id)
    if not user:
        await db.register_user(
            update.effective_user.id,
            update.effective_user.username,
            update.effective_user.full_name,
        )

    # Check if user already has predictions for this round
    existing = await db.get_prediction(update.effective_user.id, race.round)
    if existing:
        # Show existing predictions with details
        questions = prediction_service.generate_questions(race, race.round)
        answers = existing.questions
        if isinstance(answers, str):
            import json
            answers = json.loads(answers)

        lines = [f"🎯 *Твои прогнозы — Round {race.round} ({race.name})*\n"]
        for q in questions:
            qid = q["id"]
            a = answers.get(qid, {})
            if a:
                ans_text = "Да ✅" if a.get("answer") else "Нет ❌"
                conf = a.get("confidence", 0)
                lines.append(f"*Q{qid}.* {q['text']}")
                lines.append(f"   {ans_text} (уверенность: {'⭐' * conf})\n")
            else:
                lines.append(f"*Q{qid}.* {q['text']}")
                lines.append(f"   — нет ответа\n")

        lines.append("Хочешь перезаписать?")

        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🔄 Да, заново", callback_data="pred_overwrite"),
                InlineKeyboardButton("✅ Оставить", callback_data="pred_keep"),
            ],
            [
                InlineKeyboardButton("📢 Поделиться в группе", callback_data=f"share:predict:{race.round}"),
            ],
        ])
        await update.message.reply_text("\n".join(lines), reply_markup=keyboard, parse_mode="Markdown")
        context.user_data["pred_round"] = race.round
        return ASK_ANSWER

    # Generate questions
    questions = prediction_service.generate_questions(race, race.round)
    context.user_data["pred_questions"] = questions
    context.user_data["pred_answers"] = {}
    context.user_data["pred_current"] = 0
    context.user_data["pred_round"] = race.round

    return await _show_question(update, context)


async def _show_question(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Show current prediction question."""
    questions = context.user_data["pred_questions"]
    current = context.user_data["pred_current"]

    if current >= len(questions):
        return await _show_summary(update, context)

    q = questions[current]
    total = len(questions)
    text = (
        f"\U0001f3af *Prediction {current + 1}/{total}*\n\n"
        f"{q['text']}"
    )

    keyboard = build_prediction_keyboard()

    if update.callback_query:
        await update.callback_query.edit_message_text(
            text, reply_markup=keyboard, parse_mode="Markdown"
        )
    else:
        await update.message.reply_text(
            text, reply_markup=keyboard, parse_mode="Markdown"
        )

    return ASK_ANSWER


async def answer_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle Yes/No answer."""
    query = update.callback_query
    await query.answer()

    answer = query.data == "pred_yes"
    current = context.user_data["pred_current"]
    questions = context.user_data["pred_questions"]
    qid = questions[current]["id"]

    context.user_data.setdefault("pred_current_answer", {})
    context.user_data["pred_current_answer"] = {"qid": qid, "answer": answer}

    text = (
        f"\U0001f3af *Prediction {current + 1}/{len(questions)}*\n\n"
        f"{questions[current]['text']}\n\n"
        f"\u0422\u0432\u043e\u0439 \u043e\u0442\u0432\u0435\u0442: {'\u0414\u0430 \u2705' if answer else '\u041d\u0435\u0442 \u274c'}\n\n"
        f"\u0423\u0432\u0435\u0440\u0435\u043d\u043d\u043e\u0441\u0442\u044c (1-5):"
    )

    keyboard = build_confidence_keyboard()
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode="Markdown")

    return ASK_CONFIDENCE


async def confidence_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle confidence selection."""
    query = update.callback_query
    await query.answer()

    confidence = int(query.data.replace("conf_", ""))
    current_answer = context.user_data["pred_current_answer"]
    qid = current_answer["qid"]

    context.user_data["pred_answers"][qid] = {
        "answer": current_answer["answer"],
        "confidence": confidence,
    }

    context.user_data["pred_current"] += 1
    return await _show_question(update, context)


async def _show_summary(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Show prediction summary and confirm."""
    questions = context.user_data["pred_questions"]
    answers = context.user_data["pred_answers"]
    race_round = context.user_data["pred_round"]

    lines = [f"\U0001f4cb *Prediction Summary \u2014 Round {race_round}*\n"]

    for q in questions:
        qid = q["id"]
        a = answers.get(qid, {})
        ans_text = "\u0414\u0430 \u2705" if a.get("answer") else "\u041d\u0435\u0442 \u274c"
        conf = a.get("confidence", 0)
        lines.append(f"Q{qid}: {q['text']}")
        lines.append(f"   {ans_text} (conf: {'*' * conf})\n")

    # Show deadline countdown
    db = _get_db(context)
    race = await db.get_next_race()
    if race:
        deadline = datetime.fromisoformat(race.race_datetime)
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        remaining = deadline - now
        if remaining.total_seconds() > 0:
            hours = int(remaining.total_seconds() // 3600)
            minutes = int((remaining.total_seconds() % 3600) // 60)
            lines.append(f"\u23f0 \u0414\u0435\u0434\u043b\u0430\u0439\u043d: {hours}\u0447 {minutes}\u043c\u0438\u043d\n")

    text = "\n".join(lines)
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("\u2705 \u041f\u043e\u0434\u0442\u0432\u0435\u0440\u0434\u0438\u0442\u044c", callback_data="pred_confirm"),
            InlineKeyboardButton("\U0001f504 \u0417\u0430\u043d\u043e\u0432\u043e", callback_data="pred_restart"),
        ]
    ])

    if update.callback_query:
        await update.callback_query.edit_message_text(
            text, reply_markup=keyboard, parse_mode="Markdown"
        )
    else:
        await update.message.reply_text(
            text, reply_markup=keyboard, parse_mode="Markdown"
        )

    return ASK_ANSWER  # Reuse state for confirm buttons


async def confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle confirm/restart for predictions."""
    query = update.callback_query
    await query.answer()

    if query.data == "pred_keep":
        await query.edit_message_text("\u2705 \u041f\u0440\u043e\u0433\u043d\u043e\u0437\u044b \u0431\u0435\u0437 \u0438\u0437\u043c\u0435\u043d\u0435\u043d\u0438\u0439.")
        return ConversationHandler.END

    if query.data == "pred_overwrite":
        # Start fresh prediction flow
        db = _get_db(context)
        race = await db.get_next_race()
        questions = prediction_service.generate_questions(race, race.round)
        context.user_data["pred_questions"] = questions
        context.user_data["pred_answers"] = {}
        context.user_data["pred_current"] = 0
        return await _show_question(update, context)

    if query.data == "pred_restart":
        context.user_data["pred_answers"] = {}
        context.user_data["pred_current"] = 0
        return await _show_question(update, context)

    # pred_confirm
    db = _get_db(context)
    race_round = context.user_data["pred_round"]
    answers = context.user_data["pred_answers"]

    prediction = Prediction(
        user_id=update.effective_user.id,
        race_round=race_round,
        questions=answers,
    )
    await db.save_prediction(prediction)

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("\U0001f4e2 \u041f\u043e\u0434\u0435\u043b\u0438\u0442\u044c\u0441\u044f \u0432 \u0433\u0440\u0443\u043f\u043f\u0435", callback_data=f"share:predict:{race_round}")],
    ])
    await query.edit_message_text(
        "\u2705 \u041f\u0440\u043e\u0433\u043d\u043e\u0437\u044b \u0441\u043e\u0445\u0440\u0430\u043d\u0435\u043d\u044b! \u0420\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442\u044b \u043f\u043e\u0441\u043b\u0435 \u0433\u043e\u043d\u043a\u0438. \u0423\u0434\u0430\u0447\u0438 \U0001f340",
        reply_markup=keyboard,
    )

    return ConversationHandler.END


async def predict_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("\u041f\u0440\u043e\u0433\u043d\u043e\u0437\u044b \u043e\u0442\u043c\u0435\u043d\u0435\u043d\u044b.")
    return ConversationHandler.END


async def _menu_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Silently end conversation when user switches to another menu section."""
    return ConversationHandler.END


def setup_predict_handlers(app: Application) -> None:
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("predict", predict_start),
            MessageHandler(
                filters.Text([MENU_PREDICTIONS]) & filters.ChatType.PRIVATE,
                predict_start,
            ),
        ],
        states={
            ASK_ANSWER: [
                CallbackQueryHandler(confirm_callback, pattern=r"^pred_(confirm|restart|overwrite|keep)$"),
                CallbackQueryHandler(answer_callback, pattern=r"^pred_(yes|no)$"),
            ],
            ASK_CONFIDENCE: [
                CallbackQueryHandler(confidence_callback, pattern=r"^conf_\d$"),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", predict_cancel),
            MessageHandler(
                filters.Text(OTHER_MENU_TEXTS) & filters.ChatType.PRIVATE,
                _menu_cancel,
            ),
        ],
        per_message=False,
        per_chat=True,
        allow_reentry=True,
    )
    app.add_handler(conv_handler, group=2)
    app.add_handler(CommandHandler("predict", predict_group), group=0)
