from __future__ import annotations

import logging
from datetime import datetime, timezone

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

from config import settings
from data.database import Database
from services.budget import get_driver_name
from services.survivor_logic import SurvivorService
from utils.keyboards import build_survivor_keyboard

logger = logging.getLogger(__name__)


def _get_db(context: ContextTypes.DEFAULT_TYPE) -> Database:
    return context.bot_data["db"]


async def survivor_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Redirect to DM in group chat."""
    if update.effective_chat.type == "private":
        return await survivor_dm(update, context)

    bot_username = (await context.bot.get_me()).username
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "\U0001f3af Survivor Pick",
            url=f"https://t.me/{bot_username}?start=survivor",
        )]
    ])
    await update.message.reply_text(
        "Сделай survivor pick в личке \U0001f447",
        reply_markup=keyboard,
    )


async def survivor_dm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show survivor pick interface in DM."""
    db = _get_db(context)
    race = await db.get_next_race()

    if not race:
        await update.message.reply_text("Нет запланированных гонок.")
        return

    # Deadline = qualifying start
    deadline = datetime.fromisoformat(race.qualifying_datetime)
    if datetime.now(timezone.utc).replace(tzinfo=None) > deadline:
        await update.message.reply_text(
            "\u26a0\ufe0f Дедлайн survivor pick прошёл!"
        )
        return

    # Auto-register
    user = await db.get_user(update.effective_user.id)
    if not user:
        await db.register_user(
            update.effective_user.id,
            update.effective_user.username,
            update.effective_user.full_name,
        )

    svc = SurvivorService(db)
    all_drivers, used_ids = await svc.get_available_drivers(update.effective_user.id)
    is_elim = await svc.is_eliminated(update.effective_user.id)

    status = "\U0001f47b Zombie mode" if is_elim else "\U0001f7e2 Alive"
    available_count = len([d for d in all_drivers if d.id not in used_ids])

    text = (
        f"\U0001f3af *Survivor Pool \u2014 Round {race.round}*\n"
        f"{race.name}\n\n"
        f"Status: {status}\n"
        f"Available drivers: {available_count}/{len(all_drivers)}\n\n"
        f"Выбери пилота. Он должен финишировать в очках (P1-P10).\n"
        f"\u26a0\ufe0f Нельзя повторять пилотов!"
    )

    keyboard = build_survivor_keyboard(all_drivers, used_ids)
    await update.message.reply_text(text, reply_markup=keyboard, parse_mode="Markdown")


async def survivor_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle survivor driver selection."""
    query = update.callback_query
    await query.answer()

    if query.data == "surv_used":
        await query.answer("Этот пилот уже использован!", show_alert=True)
        return

    driver_id = query.data.replace("sv_", "")
    from services.budget import get_all_drivers
    all_driver_ids = {d.id for d in get_all_drivers()}
    if driver_id not in all_driver_ids:
        await query.edit_message_text("Unknown driver.")
        return
    db = _get_db(context)
    race = await db.get_next_race()

    if not race:
        await query.edit_message_text("Нет запланированных гонок.")
        return

    driver_name = get_driver_name(driver_id)

    # Confirmation
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                f"\u2705 Да, {driver_name}!",
                callback_data=f"svc_{driver_id}",
            ),
            InlineKeyboardButton(
                "\u274c Отмена",
                callback_data="sv_cancel",
            ),
        ]
    ])

    await query.edit_message_text(
        f"\U0001f3af Выбрать *{driver_name}* на {race.name}?\n\n"
        f"\u26a0\ufe0f Отменить нельзя!",
        reply_markup=keyboard,
        parse_mode="Markdown",
    )


async def survivor_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle survivor pick confirmation."""
    query = update.callback_query
    await query.answer()

    if query.data == "sv_cancel":
        # Re-show selection
        await query.edit_message_text("Survivor pick отменён. Используй /survivor снова.")
        return

    driver_id = query.data.replace("svc_", "")
    from services.budget import get_all_drivers
    all_driver_ids = {d.id for d in get_all_drivers()}
    if driver_id not in all_driver_ids:
        await query.edit_message_text("Unknown driver.")
        return
    db = _get_db(context)
    race = await db.get_next_race()

    if not race:
        await query.edit_message_text("Нет запланированных гонок.")
        return

    svc = SurvivorService(db)
    ok, msg = await svc.make_pick(update.effective_user.id, race.round, driver_id)

    if not ok:
        await query.edit_message_text(f"\u274c {msg}")
        return

    driver_name = get_driver_name(driver_id)
    if msg == "ZOMBIE_PICK":
        await query.edit_message_text(
            f"\U0001f47b Zombie pick сохранён!\n\n"
            f"\U0001f3ce {driver_name} на {race.name}\n"
            f"\u26a0\ufe0f Ты выбываешь, но продолжаешь как зомби."
        )
    else:
        await query.edit_message_text(
            f"\u2705 Survivor pick сохранён!\n\n"
            f"\U0001f3ce {driver_name} на {race.name}\n"
            f"\U0001f91e Удачи!"
        )

    # Notify group
    for chat_id in settings.GROUP_CHAT_IDS:
        username = update.effective_user.username
        display = f"@{username}" if username else update.effective_user.full_name
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"\U0001f3af {display} сделал survivor pick!",
            )
        except Exception as e:
            logger.warning("Could not send group survivor notification to %s: %s", chat_id, e)


async def survivor_standings_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show survivor standings."""
    db = _get_db(context)
    svc = SurvivorService(db)
    standings = await svc.get_survivor_standings()

    if not standings:
        await update.message.reply_text("Ещё нет survivor picks.")
        return

    lines = ["\U0001f3af *Survivor Standings*\n"]
    lines.append("```")
    lines.append(f"{'#':>2} {'Manager':<14} {'W':>2} {'Status':<8}")
    lines.append("-" * 30)

    for i, s in enumerate(standings, 1):
        username = s["username"] or str(s["user_id"])
        wins = s["survived_count"]
        if s["status"] == "zombie":
            status = f"R{s['eliminated_round']}"
        else:
            status = "alive"
        emoji = "\U0001f7e2" if s["status"] == "alive" else "\U0001f47b"
        lines.append(f"{i:>2} {emoji}{username:<13} {wins:>2} {status:<8}")

    lines.append("```")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


def setup_survivor_handlers(app: Application) -> None:
    app.add_handler(CommandHandler("survivor", survivor_group))
    app.add_handler(CommandHandler("survivor_standings", survivor_standings_command))
    app.add_handler(CallbackQueryHandler(survivor_confirm_callback, pattern=r"^sv(c_|_cancel)"))
    app.add_handler(CallbackQueryHandler(survivor_callback, pattern=r"^sv_"))
