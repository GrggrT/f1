from __future__ import annotations

import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

from config import settings
from data.database import Database
from services.transfers import TransferService
from utils.keyboards import build_chips_keyboard

logger = logging.getLogger(__name__)

CHIP_NAMES = {
    "WILDCARD": "\U0001f0cf Wildcard — неограниченные трансферы",
    "TRIPLE_BOOST": "\U0001f4a5 Triple Boost — 3x очки одного пилота",
    "NO_NEGATIVE": "\U0001f6e1 No Negative — отрицательные очки обнуляются",
}


def _get_db(context: ContextTypes.DEFAULT_TYPE) -> Database:
    return context.bot_data["db"]


async def chips_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type != "private":
        bot_username = (await context.bot.get_me()).username
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                "\u26a1 Мои чипы",
                url=f"https://t.me/{bot_username}?start=chips",
            )]
        ])
        await update.message.reply_text(
            "Управление чипами в личке \U0001f447",
            reply_markup=keyboard,
        )
        return

    db = _get_db(context)
    transfer_svc = TransferService(db)
    available = await transfer_svc.get_available_chips(update.effective_user.id)

    race = await db.get_next_race()
    if not race:
        await update.message.reply_text("Нет запланированных гонок.")
        return

    used = await db.get_used_chips(update.effective_user.id)
    used_text = ""
    if used:
        used_text = "\n\n\u274c *Использованные:*\n" + "\n".join(
            f"  {CHIP_NAMES.get(c['chip_type'], c['chip_type'])} (Round {c['race_round_used']})"
            for c in used
        )

    text = (
        f"\u26a1 *Чипы — Round {race.round} ({race.name})*\n\n"
        f"\u2705 *Доступные:* {len(available)}/3"
        f"{used_text}\n\n"
        f"Выбери чип для активации:"
    )

    keyboard = build_chips_keyboard(available)
    await update.message.reply_text(text, reply_markup=keyboard, parse_mode="Markdown")


async def chip_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    if query.data == "chip_none":
        return

    chip_type = query.data.replace("chip_", "")
    from services.transfers import ALL_CHIPS
    if chip_type not in ALL_CHIPS:
        await query.answer("Unknown chip type", show_alert=True)
        return

    db = _get_db(context)
    race = await db.get_next_race()
    if not race:
        await query.edit_message_text("Нет запланированных гонок.")
        return

    # Confirmation step
    name = CHIP_NAMES.get(chip_type, chip_type)
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "\u2705 Да, активировать!",
                callback_data=f"chip_confirm_{chip_type}",
            ),
            InlineKeyboardButton(
                "\u274c Отмена",
                callback_data="chip_cancel",
            ),
        ]
    ])

    await query.edit_message_text(
        f"\u26a0\ufe0f *Активировать чип?*\n\n"
        f"{name}\n"
        f"На: Round {race.round} — {race.name}\n\n"
        f"\u26a0\ufe0f Чип одноразовый — отменить нельзя!",
        reply_markup=keyboard,
        parse_mode="Markdown",
    )


async def chip_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    if query.data == "chip_cancel":
        await query.edit_message_text("Активация чипа отменена.")
        return

    chip_type = query.data.replace("chip_confirm_", "")
    from services.transfers import ALL_CHIPS
    if chip_type not in ALL_CHIPS:
        await query.answer("Unknown chip type", show_alert=True)
        return
    db = _get_db(context)
    race = await db.get_next_race()
    if not race:
        await query.edit_message_text("Нет запланированных гонок.")
        return

    transfer_svc = TransferService(db)
    ok, msg = await transfer_svc.activate_chip(
        update.effective_user.id, race.round, chip_type
    )

    if not ok:
        await query.edit_message_text(f"\u274c {msg}")
        return

    name = CHIP_NAMES.get(chip_type, chip_type)
    await query.edit_message_text(
        f"\u2705 Чип активирован!\n\n{name}\nRound {race.round} — {race.name}"
    )

    # Notify group
    if settings.GROUP_CHAT_ID:
        username = update.effective_user.username
        display = f"@{username}" if username else update.effective_user.full_name
        try:
            await context.bot.send_message(
                chat_id=settings.GROUP_CHAT_ID,
                text=f"\u26a1 {display} активировал {name.split(' — ')[0]} на {race.name}!",
            )
        except Exception:
            logger.warning("Could not send group chip notification")


def setup_chips_handlers(app: Application) -> None:
    app.add_handler(CommandHandler("chips", chips_command))
    app.add_handler(CallbackQueryHandler(chip_callback, pattern=r"^chip_(?!confirm|cancel)"))
    app.add_handler(CallbackQueryHandler(chip_confirm_callback, pattern=r"^chip_(confirm_|cancel)"))
