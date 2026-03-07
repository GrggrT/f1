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

from config import settings
from data.database import Database
from data.models import UserTeam
from services.budget import (
    TOTAL_BUDGET,
    calculate_team_cost,
    get_all_constructors,
    get_all_drivers,
    get_constructor_name,
    get_constructor_price,
    get_driver_name,
    get_driver_price,
    validate_team,
)
from utils.keyboards import (
    ALL_MENU_TEXTS,
    MENU_TEAM,
    build_confirmation_keyboard,
    build_constructor_keyboard,
    build_driver_selection_keyboard,
    build_turbo_keyboard,
)

logger = logging.getLogger(__name__)

SELECT_DRIVERS, SELECT_CONSTRUCTOR, SELECT_TURBO, CONFIRM = range(4)

OTHER_MENU_TEXTS = [t for t in ALL_MENU_TEXTS if t != MENU_TEAM]


def _get_db(context: ContextTypes.DEFAULT_TYPE) -> Database:
    return context.bot_data["db"]


async def _get_next_round(context: ContextTypes.DEFAULT_TYPE) -> int | None:
    db = _get_db(context)
    race = await db.get_next_race()
    return race.round if race else None


async def _check_deadline(context: ContextTypes.DEFAULT_TYPE) -> bool:
    db = _get_db(context)
    race = await db.get_next_race()
    if race is None:
        return False
    deadline = datetime.fromisoformat(race.race_datetime)
    return datetime.now(timezone.utc).replace(tzinfo=None) < deadline


# ── /pickteam in group -> redirect to DM ──

async def pickteam_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type == "private":
        return await pickteam_start(update, context)

    bot_username = (await context.bot.get_me()).username
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "\U0001f3ce \u0421\u043e\u0431\u0440\u0430\u0442\u044c \u043a\u043e\u043c\u0430\u043d\u0434\u0443",
            url=f"https://t.me/{bot_username}?start=pickteam",
        )]
    ])
    await update.message.reply_text(
        "\u041f\u0435\u0440\u0435\u0439\u0434\u0438 \u0432 \u043b\u0438\u0447\u043a\u0443 \u0434\u043b\u044f \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438 \u043a\u043e\u043c\u0430\u043d\u0434\u044b \U0001f447",
        reply_markup=keyboard,
    )


# ── DM ConversationHandler ──

async def pickteam_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # Only handle /start if it's a deep link with "pickteam" arg
    if update.message and update.message.text and update.message.text.startswith("/start"):
        if not context.args or context.args[0] != "pickteam":
            return ConversationHandler.END

    if not await _check_deadline(context):
        await update.message.reply_text(
            "\u26a0\ufe0f \u0414\u0435\u0434\u043b\u0430\u0439\u043d \u043f\u0440\u043e\u0448\u0451\u043b \u0438\u043b\u0438 \u043d\u0435\u0442 \u0437\u0430\u043f\u043b\u0430\u043d\u0438\u0440\u043e\u0432\u0430\u043d\u043d\u044b\u0445 \u0433\u043e\u043d\u043e\u043a!"
        )
        return ConversationHandler.END

    race_round = await _get_next_round(context)
    db = _get_db(context)

    # Check if user is registered
    user = await db.get_user(update.effective_user.id)
    if user is None:
        await db.register_user(
            update.effective_user.id,
            update.effective_user.username,
            update.effective_user.full_name,
        )

    # Check existing team
    existing = await db.get_team(update.effective_user.id, race_round)
    if existing:
        context.user_data["selected_drivers"] = list(existing.drivers)
        context.user_data["selected_constructor"] = existing.constructor
        context.user_data["turbo_driver"] = existing.turbo_driver
    else:
        # Try to carry over from previous round
        latest = await db.get_latest_team(update.effective_user.id)
        if latest:
            context.user_data["selected_drivers"] = list(latest.drivers)
            context.user_data["selected_constructor"] = latest.constructor
            context.user_data["turbo_driver"] = latest.turbo_driver
        else:
            context.user_data["selected_drivers"] = []
            context.user_data["selected_constructor"] = None
            context.user_data["turbo_driver"] = None

    context.user_data["race_round"] = race_round

    return await _show_driver_selection(update, context, is_edit=True)


async def _show_driver_selection(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    is_edit: bool = False,
) -> int:
    selected = context.user_data.get("selected_drivers", [])
    drivers = get_all_drivers()

    drivers_cost = sum(get_driver_price(d) for d in selected)
    budget_left = TOTAL_BUDGET - drivers_cost

    text = (
        f"\U0001f3ce *\u0412\u044b\u0431\u0435\u0440\u0438 5 \u043f\u0438\u043b\u043e\u0442\u043e\u0432*\n"
        f"\u0412\u044b\u0431\u0440\u0430\u043d\u043e: {len(selected)}/5\n"
        f"\U0001f4b0 \u0411\u044e\u0434\u0436\u0435\u0442: ${budget_left:.1f}M \u0438\u0437 ${TOTAL_BUDGET:.0f}M"
    )

    keyboard = build_driver_selection_keyboard(drivers, selected, budget_left)

    if is_edit and update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=keyboard, parse_mode="Markdown")
    else:
        await update.message.reply_text(text, reply_markup=keyboard, parse_mode="Markdown")

    return SELECT_DRIVERS


async def driver_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "pd_next":
        selected = context.user_data.get("selected_drivers", [])
        if len(selected) != 5:
            await query.answer("\u0412\u044b\u0431\u0435\u0440\u0438 \u0440\u043e\u0432\u043d\u043e 5 \u043f\u0438\u043b\u043e\u0442\u043e\u0432!", show_alert=True)
            return SELECT_DRIVERS
        return await _show_constructor_selection(update, context)

    if data == "pd_reset":
        context.user_data["selected_drivers"] = []
        return await _show_driver_selection(update, context, is_edit=True)

    if data == "pd_count":
        await query.answer("\u0412\u044b\u0431\u0435\u0440\u0438 5 \u043f\u0438\u043b\u043e\u0442\u043e\u0432, \u0447\u0442\u043e\u0431\u044b \u043f\u0440\u043e\u0434\u043e\u043b\u0436\u0438\u0442\u044c")
        return SELECT_DRIVERS

    driver_id = data.replace("pd_", "")
    # Validate driver exists
    from services.budget import load_prices
    all_driver_ids = {d["id"] for d in load_prices()["drivers"]}
    if driver_id not in all_driver_ids:
        await query.answer("Unknown driver", show_alert=True)
        return SELECT_DRIVERS
    selected = context.user_data.get("selected_drivers", [])

    if driver_id in selected:
        selected.remove(driver_id)
    else:
        if len(selected) >= 5:
            await query.answer("\u0423\u0436\u0435 \u0432\u044b\u0431\u0440\u0430\u043d\u043e 5 \u043f\u0438\u043b\u043e\u0442\u043e\u0432! \u0421\u043d\u0438\u043c\u0438 \u043a\u043e\u0433\u043e-\u0442\u043e.", show_alert=True)
            return SELECT_DRIVERS

        drivers_cost = sum(get_driver_price(d) for d in selected) + get_driver_price(driver_id)
        if drivers_cost > TOTAL_BUDGET:
            await query.answer("\u041d\u0435\u0434\u043e\u0441\u0442\u0430\u0442\u043e\u0447\u043d\u043e \u0431\u044e\u0434\u0436\u0435\u0442\u0430!", show_alert=True)
            return SELECT_DRIVERS

        selected.append(driver_id)

    context.user_data["selected_drivers"] = selected
    return await _show_driver_selection(update, context, is_edit=True)


async def _show_constructor_selection(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> int:
    selected_drivers = context.user_data.get("selected_drivers", [])
    drivers_cost = sum(get_driver_price(d) for d in selected_drivers)
    budget_left = TOTAL_BUDGET - drivers_cost

    constructors = get_all_constructors()
    text = (
        f"\U0001f3d7 *\u0412\u044b\u0431\u0435\u0440\u0438 1 \u043a\u043e\u043d\u0441\u0442\u0440\u0443\u043a\u0442\u043e\u0440\u0430*\n"
        f"\U0001f4b0 \u041e\u0441\u0442\u0430\u0442\u043e\u043a \u0431\u044e\u0434\u0436\u0435\u0442\u0430: ${budget_left:.1f}M"
    )

    keyboard = build_constructor_keyboard(constructors, budget_left)

    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=keyboard, parse_mode="Markdown")
    else:
        await update.message.reply_text(text, reply_markup=keyboard, parse_mode="Markdown")

    return SELECT_CONSTRUCTOR


async def constructor_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    if query.data == "pc_back":
        return await _show_driver_selection(update, context, is_edit=True)

    constructor_id = query.data.replace("pc_", "")
    from services.budget import load_prices
    all_constructor_ids = {c["id"] for c in load_prices()["constructors"]}
    if constructor_id not in all_constructor_ids:
        await query.answer("Unknown constructor", show_alert=True)
        return SELECT_CONSTRUCTOR
    selected_drivers = context.user_data.get("selected_drivers", [])
    drivers_cost = sum(get_driver_price(d) for d in selected_drivers)
    budget_left = TOTAL_BUDGET - drivers_cost

    c_price = get_constructor_price(constructor_id)
    if c_price > budget_left:
        await query.answer("\u041d\u0435\u0434\u043e\u0441\u0442\u0430\u0442\u043e\u0447\u043d\u043e \u0431\u044e\u0434\u0436\u0435\u0442\u0430! \u0412\u0435\u0440\u043d\u0438\u0441\u044c \u043d\u0430\u0437\u0430\u0434 \u0438 \u043f\u043e\u043c\u0435\u043d\u044f\u0439 \u043f\u0438\u043b\u043e\u0442\u043e\u0432.", show_alert=True)
        return SELECT_CONSTRUCTOR

    context.user_data["selected_constructor"] = constructor_id
    return await _show_turbo_selection(update, context)


async def _show_turbo_selection(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> int:
    selected_drivers = context.user_data.get("selected_drivers", [])
    drivers = [d for d in get_all_drivers() if d.id in selected_drivers]

    text = "\u26a1 *\u0412\u044b\u0431\u0435\u0440\u0438 DRS Boost (2x)* \u2014 \u043e\u0434\u0438\u043d \u0438\u0437 \u0442\u0432\u043e\u0438\u0445 \u043f\u0438\u043b\u043e\u0442\u043e\u0432:"
    keyboard = build_turbo_keyboard(drivers)

    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=keyboard, parse_mode="Markdown")
    else:
        await update.message.reply_text(text, reply_markup=keyboard, parse_mode="Markdown")

    return SELECT_TURBO


async def turbo_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    if query.data == "tb_back":
        return await _show_constructor_selection(update, context)

    turbo_id = query.data.replace("tb_", "")
    selected = context.user_data.get("selected_drivers", [])
    if turbo_id not in selected:
        await query.answer("This driver is not in your team", show_alert=True)
        return SELECT_TURBO
    context.user_data["turbo_driver"] = turbo_id
    return await _show_confirmation(update, context)


async def _show_confirmation(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> int:
    selected_drivers = context.user_data.get("selected_drivers", [])
    constructor_id = context.user_data.get("selected_constructor", "")
    turbo_id = context.user_data.get("turbo_driver", "")
    race_round = context.user_data.get("race_round", 0)

    drivers_str = "\n".join(
        f"  {'\u26a1 ' if d == turbo_id else '  '}{get_driver_name(d)} (${get_driver_price(d):.0f}M)"
        for d in selected_drivers
    )
    constructor_str = f"{get_constructor_name(constructor_id)} (${get_constructor_price(constructor_id):.0f}M)"

    cost = calculate_team_cost(selected_drivers, constructor_id)
    remaining = TOTAL_BUDGET - cost

    text = (
        f"\U0001f3ce *\u0422\u0432\u043e\u044f \u043a\u043e\u043c\u0430\u043d\u0434\u0430 (Round {race_round}):*\n\n"
        f"\U0001f3ce *\u041f\u0438\u043b\u043e\u0442\u044b:*\n{drivers_str}\n\n"
        f"\U0001f3d7 *\u041a\u043e\u043d\u0441\u0442\u0440\u0443\u043a\u0442\u043e\u0440:* {constructor_str}\n"
        f"\u26a1 *DRS Boost:* {get_driver_name(turbo_id)}\n"
        f"\U0001f4b0 *\u0411\u044e\u0434\u0436\u0435\u0442:* ${remaining:.1f}M \u043e\u0441\u0442\u0430\u043b\u043e\u0441\u044c"
    )

    keyboard = build_confirmation_keyboard()

    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=keyboard, parse_mode="Markdown")
    else:
        await update.message.reply_text(text, reply_markup=keyboard, parse_mode="Markdown")

    return CONFIRM


async def confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    if query.data == "team_restart":
        context.user_data["selected_drivers"] = []
        context.user_data["selected_constructor"] = None
        context.user_data["turbo_driver"] = None
        return await _show_driver_selection(update, context, is_edit=True)

    if query.data == "team_back_turbo":
        return await _show_turbo_selection(update, context)

    # team_confirm
    selected_drivers = context.user_data["selected_drivers"]
    constructor_id = context.user_data["selected_constructor"]
    turbo_id = context.user_data["turbo_driver"]
    race_round = context.user_data["race_round"]

    valid, msg = validate_team(selected_drivers, constructor_id)
    if not valid:
        await query.answer(msg, show_alert=True)
        return CONFIRM

    cost = calculate_team_cost(selected_drivers, constructor_id)
    team = UserTeam(
        user_id=update.effective_user.id,
        username=update.effective_user.username or "",
        race_round=race_round,
        drivers=selected_drivers,
        constructor=constructor_id,
        turbo_driver=turbo_id,
        budget_remaining=TOTAL_BUDGET - cost,
    )

    db = _get_db(context)
    await db.save_team(update.effective_user.id, race_round, team)

    await query.edit_message_text(
        "\u2705 \u041a\u043e\u043c\u0430\u043d\u0434\u0430 \u0441\u043e\u0445\u0440\u0430\u043d\u0435\u043d\u0430! \u0423\u0434\u0430\u0447\u0438 \u0432 \u044d\u0442\u043e\u043c \u0440\u0430\u0443\u043d\u0434\u0435 \U0001f3c1"
    )

    # Notify group
    if settings.GROUP_CHAT_ID:
        username = update.effective_user.username
        display = f"@{username}" if username else update.effective_user.full_name
        try:
            await context.bot.send_message(
                chat_id=settings.GROUP_CHAT_ID,
                text=f"\u2705 {display} \u043e\u0431\u043d\u043e\u0432\u0438\u043b \u043a\u043e\u043c\u0430\u043d\u0434\u0443!",
            )
        except Exception:
            logger.warning("Could not send group notification")

    return ConversationHandler.END


async def myteam_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type != "private":
        bot_username = (await context.bot.get_me()).username
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                "\U0001f441 \u041c\u043e\u044f \u043a\u043e\u043c\u0430\u043d\u0434\u0430",
                url=f"https://t.me/{bot_username}?start=myteam",
            )]
        ])
        await update.message.reply_text(
            "\u041f\u043e\u0441\u043c\u043e\u0442\u0440\u0438 \u0441\u0432\u043e\u044e \u043a\u043e\u043c\u0430\u043d\u0434\u0443 \u0432 \u043b\u0438\u0447\u043a\u0435 \U0001f447",
            reply_markup=keyboard,
        )
        return

    db = _get_db(context)
    team = await db.get_latest_team(update.effective_user.id)
    if team is None:
        await update.message.reply_text(
            "\u0423 \u0442\u0435\u0431\u044f \u0435\u0449\u0451 \u043d\u0435\u0442 \u043a\u043e\u043c\u0430\u043d\u0434\u044b. \u0418\u0441\u043f\u043e\u043b\u044c\u0437\u0443\u0439 /pickteam"
        )
        return

    turbo = team.turbo_driver
    drivers_str = "\n".join(
        f"  {'\u26a1 ' if d == turbo else '  '}{get_driver_name(d)} (${get_driver_price(d):.0f}M)"
        for d in team.drivers
    )
    constructor_str = f"{get_constructor_name(team.constructor)} (${get_constructor_price(team.constructor):.0f}M)"

    text = (
        f"\U0001f3ce *\u0422\u0432\u043e\u044f \u043a\u043e\u043c\u0430\u043d\u0434\u0430 (Round {team.race_round}):*\n\n"
        f"\U0001f3ce *\u041f\u0438\u043b\u043e\u0442\u044b:*\n{drivers_str}\n\n"
        f"\U0001f3d7 *\u041a\u043e\u043d\u0441\u0442\u0440\u0443\u043a\u0442\u043e\u0440:* {constructor_str}\n"
        f"\u26a1 *DRS Boost:* {get_driver_name(turbo)}\n"
        f"\U0001f4b0 *\u041e\u0441\u0442\u0430\u0442\u043e\u043a:* ${team.budget_remaining:.1f}M"
    )

    # Score history summary
    scores = await db.get_user_score_history(update.effective_user.id)
    if scores:
        pts_list = [s["fantasy_points"] for s in scores]
        total = sum(pts_list)
        avg = total / len(pts_list)
        last_pts = pts_list[-1] if pts_list else 0
        text += (
            f"\n\n\U0001f4ca *\u0421\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430:*\n"
            f"  Total: {total:.0f} pts | Avg: {avg:.1f} | Last: {last_pts:.0f}"
        )

    await update.message.reply_text(text, parse_mode="Markdown")


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("\u0421\u0431\u043e\u0440\u043a\u0430 \u043a\u043e\u043c\u0430\u043d\u0434\u044b \u043e\u0442\u043c\u0435\u043d\u0435\u043d\u0430.")
    return ConversationHandler.END


async def _menu_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Silently end conversation when user switches to another menu section."""
    return ConversationHandler.END


def setup_team_handlers(app: Application) -> None:
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("pickteam", pickteam_start),
            CommandHandler("start", pickteam_start),
            MessageHandler(
                filters.Text([MENU_TEAM]) & filters.ChatType.PRIVATE,
                pickteam_start,
            ),
        ],
        states={
            SELECT_DRIVERS: [CallbackQueryHandler(driver_callback, pattern=r"^pd_")],
            SELECT_CONSTRUCTOR: [CallbackQueryHandler(constructor_callback, pattern=r"^pc_")],
            SELECT_TURBO: [CallbackQueryHandler(turbo_callback, pattern=r"^tb_")],
            CONFIRM: [CallbackQueryHandler(confirm_callback, pattern=r"^team_")],
        },
        fallbacks=[
            CommandHandler("cancel", cancel_command),
            MessageHandler(
                filters.Text(OTHER_MENU_TEXTS) & filters.ChatType.PRIVATE,
                _menu_cancel,
            ),
        ],
        per_message=False,
        per_chat=True,
        allow_reentry=True,
    )
    app.add_handler(conv_handler, group=1)
    app.add_handler(CommandHandler("pickteam", pickteam_group), group=0)
    app.add_handler(CommandHandler("myteam", myteam_command))
