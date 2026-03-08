from __future__ import annotations

import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

from data.database import Database
from services.budget import get_all_drivers, get_driver_name

logger = logging.getLogger(__name__)


def _get_db(context: ContextTypes.DEFAULT_TYPE) -> Database:
    return context.bot_data["db"]


# ── /rival — set/view rival ──

async def rival_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Set or view H2H rival. Usage: /rival or /rival @username"""
    if update.effective_chat.type != "private":
        await update.message.reply_text("Используй /rival в личке.")
        return

    db = _get_db(context)
    user_id = update.effective_user.id

    # If argument given, set rival
    if context.args:
        target = context.args[0].lstrip("@")
        # Find user by username
        users = await db.get_all_users()
        rival = next((u for u in users if u.get("username", "").lower() == target.lower()), None)
        if not rival:
            await update.message.reply_text(f"Пользователь @{target} не найден в лиге.")
            return
        if rival["telegram_id"] == user_id:
            await update.message.reply_text("Нельзя добавить себя в соперники!")
            return

        ok = await db.set_rival(user_id, rival["telegram_id"])
        if ok:
            rival_name = f"@{rival['username']}" if rival.get("username") else rival.get("display_name", "???")
            await update.message.reply_text(f"\U0001f91c Rival добавлен: {rival_name}")
        else:
            await update.message.reply_text("Этот rival уже добавлен.")
        return

    # No args — show current rivals with inline buttons
    rivals = await db.get_rivals(user_id)
    if not rivals:
        # Show user picker
        users = await db.get_all_users()
        others = [u for u in users if u["telegram_id"] != user_id]
        if not others:
            await update.message.reply_text("В лиге пока нет других участников.")
            return

        buttons = []
        row = []
        for u in others:
            name = f"@{u['username']}" if u.get("username") else u.get("display_name", "???")
            row.append(InlineKeyboardButton(name, callback_data=f"rival_add_{u['telegram_id']}"))
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)

        await update.message.reply_text(
            "\U0001f91c *Выбери rival для H2H дуэли:*",
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode="Markdown",
        )
        return

    # Show rivals list
    lines = ["\U0001f91c *Твои rivals:*\n"]
    for r in rivals:
        name = f"@{r['username']}" if r.get("username") else r.get("display_name", "???")
        record = await db.get_h2h_record(user_id, r["rival_id"])
        lines.append(f"{name}: {record['wins']}-{record['losses']}-{record['draws']} (W-L-D)")

    lines.append("\n/h2h \u2014 подробная статистика")
    lines.append("Добавить ещё: /rival @username")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def rival_add_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle rival selection via inline button."""
    query = update.callback_query
    await query.answer()

    rival_id = int(query.data.replace("rival_add_", ""))
    db = _get_db(context)
    user_id = update.effective_user.id

    if rival_id == user_id:
        await query.edit_message_text("Нельзя добавить себя в соперники!")
        return

    rival_user = await db.get_user(rival_id)
    if not rival_user:
        await query.edit_message_text("Пользователь не найден.")
        return

    ok = await db.set_rival(user_id, rival_id)
    rival_name = f"@{rival_user['username']}" if rival_user.get("username") else rival_user.get("display_name", "???")

    if ok:
        await query.edit_message_text(f"\U0001f91c Rival добавлен: {rival_name}\n\nИспользуй /h2h для статистики.")
    else:
        await query.edit_message_text(f"{rival_name} уже твой rival.")


# ── /h2h — detailed H2H stats ──

async def h2h_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show detailed H2H stats with rivals."""
    db = _get_db(context)
    user_id = update.effective_user.id

    rivals = await db.get_rivals(user_id)
    if not rivals:
        await update.message.reply_text(
            "У тебя нет rivals. Добавь через /rival @username"
        )
        return

    for r in rivals:
        rival_name = f"@{r['username']}" if r.get("username") else r.get("display_name", "???")
        record = await db.get_h2h_record(user_id, r["rival_id"])

        lines = [f"\U0001f91c *H2H vs {rival_name}*\n"]
        lines.append(f"Счёт: *{record['wins']}-{record['losses']}-{record['draws']}*\n")

        if record["rounds"]:
            lines.append("```")
            lines.append(f"{'R':>2} {'Race':<14} {'You':>5} {'Opp':>5} {'W'}")
            lines.append("-" * 32)
            for rd in record["rounds"]:
                race_name = rd.get("race_name", f"R{rd['race_round']}")
                if len(race_name) > 14:
                    race_name = race_name[:13] + "."
                user_pts = rd["user_pts"]
                rival_pts = rd["rival_pts"]
                if user_pts > rival_pts:
                    result = "\u2713"
                elif user_pts < rival_pts:
                    result = "\u2717"
                else:
                    result = "="
                lines.append(f"{rd['race_round']:>2} {race_name:<14} {user_pts:>5.0f} {rival_pts:>5.0f}  {result}")
            lines.append("```")

        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ── /driver <id> — driver fantasy stats ──

async def driver_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show driver fantasy stats. Usage: /driver verstappen"""
    if not context.args:
        # Show list of drivers
        drivers = get_all_drivers()
        lines = ["\U0001f3ce *Доступные пилоты:*\n"]
        for d in sorted(drivers, key=lambda x: x.name):
            lines.append(f"`{d.id}` \u2014 {d.name} (${d.price}M)")
        lines.append("\nИспользуй: /driver <id>")
        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")
        return

    driver_id = context.args[0].lower()
    # Try to find by partial match
    drivers = get_all_drivers()
    match = None
    for d in drivers:
        if d.id == driver_id or driver_id in d.id or driver_id in d.name.lower():
            match = d
            break

    if not match:
        await update.message.reply_text(f"Пилот `{driver_id}` не найден. Используй /driver для списка.", parse_mode="Markdown")
        return

    db = _get_db(context)

    # Race results
    stats = await db.get_driver_fantasy_stats(match.id)
    pick_stats = await db.get_driver_pick_stats(match.id)

    lines = [f"\U0001f3ce *{match.name}* \u2014 ${match.price}M\n"]

    if stats:
        lines.append("*Результаты гонок:*")
        lines.append("```")
        lines.append(f"{'R':>2} {'Race':<14} {'Grid':>4} {'Fin':>4}")
        lines.append("-" * 28)
        points_finishes = 0
        dnfs = 0
        for s in stats:
            race_name = s.get("race_name", f"R{s['round']}")
            if len(race_name) > 14:
                race_name = race_name[:13] + "."
            grid = s["grid_position"]
            fin = "DNF" if s["dnf"] else str(s["finish_position"] or "?")
            fl = "*" if s["fastest_lap"] else " "
            if not s["dnf"] and s["finish_position"] and s["finish_position"] <= 10:
                points_finishes += 1
            if s["dnf"]:
                dnfs += 1
            lines.append(f"{s['round']:>2} {race_name:<14} P{grid:>2}  {fin:>3}{fl}")
        lines.append("```")

        lines.append(f"\n\U0001f4ca Races: {len(stats)} | Points finishes: {points_finishes} | DNFs: {dnfs}")
    else:
        lines.append("Нет данных о результатах гонок.")

    if pick_stats:
        lines.append(f"\n\U0001f465 *Популярность в fantasy:*")
        for p in pick_stats:
            pct = (p["pick_count"] / p["total_teams"] * 100) if p["total_teams"] > 0 else 0
            lines.append(f"  R{p['race_round']}: {p['pick_count']}/{p['total_teams']} ({pct:.0f}%)")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


def setup_h2h_handlers(app: Application) -> None:
    app.add_handler(CommandHandler("rival", rival_command))
    app.add_handler(CommandHandler("h2h", h2h_command))
    app.add_handler(CommandHandler("driver", driver_command))
    app.add_handler(CallbackQueryHandler(rival_add_callback, pattern=r"^rival_add_"))
