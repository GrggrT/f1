from __future__ import annotations

import logging
from collections import defaultdict

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CommandHandler, ContextTypes

from data.database import Database
from services.budget import get_driver_name

logger = logging.getLogger(__name__)


def _get_db(context: ContextTypes.DEFAULT_TYPE) -> Database:
    return context.bot_data["db"]


# ── /predstandings ──

async def predstandings_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db = _get_db(context)
    standings = await db.get_prediction_standings()

    if not standings:
        await update.message.reply_text("Prediction standings will appear after the first race.")
        return

    lines = ["\U0001f3af *Prediction Standings*\n"]
    lines.append("```")
    lines.append(f"{'#':>2} {'Manager':<12} {'Corr':>4} {'Pts':>4} {'R':>2}")
    lines.append("-" * 28)

    for i, row in enumerate(standings, 1):
        row = dict(row) if not isinstance(row, dict) else row
        username = row.get("username", "???")
        correct = row.get("total_correct", 0)
        score = row.get("total_score", 0)
        rounds = row.get("rounds_played", 0)
        lines.append(f"{i:>2} {username:<12} {correct:>4} {score:>4} {rounds:>2}")

    lines.append("```")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ── /history ──

async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type != "private":
        await update.message.reply_text("Use /history in DM.")
        return

    db = _get_db(context)
    user_id = update.effective_user.id

    # Score history
    scores = await db.get_user_score_history(user_id)
    transfers = await db.get_user_transfers(user_id)
    pred_history = await db.get_user_prediction_history(user_id)

    if not scores and not transfers and not pred_history:
        await update.message.reply_text("No history yet. Play some rounds first!")
        return

    # Message 1: Score history
    if scores:
        lines = ["\U0001f4ca *Score History*\n"]
        lines.append("```")
        lines.append(f"{'R':>2} {'Race':<16} {'Pts':>6}")
        lines.append("-" * 26)
        total = 0
        for s in scores:
            race_name = s.get("race_name", f"Round {s['race_round']}")
            if len(race_name) > 16:
                race_name = race_name[:15] + "."
            pts = s["fantasy_points"]
            total += pts
            lines.append(f"{s['race_round']:>2} {race_name:<16} {pts:>6.0f}")
        lines.append("-" * 26)
        lines.append(f"   {'TOTAL':<16} {total:>6.0f}")
        lines.append("```")

        # Avg and best/worst
        pts_list = [s["fantasy_points"] for s in scores]
        avg = sum(pts_list) / len(pts_list)
        best_round = max(scores, key=lambda s: s["fantasy_points"])
        worst_round = min(scores, key=lambda s: s["fantasy_points"])
        lines.append(f"\n\U0001f4c8 Avg: {avg:.1f} | Best: {best_round['fantasy_points']:.0f} (R{best_round['race_round']}) | Worst: {worst_round['fantasy_points']:.0f} (R{worst_round['race_round']})")

        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    # Message 2: Prediction history
    if pred_history:
        lines = ["\U0001f3af *Prediction History*\n"]
        lines.append("```")
        lines.append(f"{'R':>2} {'Race':<16} {'Hit':>3} {'Pts':>4}")
        lines.append("-" * 28)
        for p in pred_history:
            race_name = p.get("race_name", f"Round {p['race_round']}")
            if len(race_name) > 16:
                race_name = race_name[:15] + "."
            lines.append(f"{p['race_round']:>2} {race_name:<16} {p['correct_count']:>2}/7 {p['total_score']:>4}")
        lines.append("```")
        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    # Message 3: Recent transfers
    if transfers:
        lines = ["\U0001f504 *Recent Transfers*\n"]
        for t in transfers[:10]:
            free = "\u2705" if t["is_free"] else "\U0001f4b8 -10pts"
            lines.append(
                f"R{t['race_round']}: {get_driver_name(t['driver_out'])} \u2192 "
                f"{get_driver_name(t['driver_in'])} {free}"
            )
        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ── /chart (text-based standings progression) ──

async def chart_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db = _get_db(context)
    all_scores = await db.get_all_scores_by_round()

    if not all_scores:
        await update.message.reply_text("No data for chart yet.")
        return

    # Build cumulative scores per user per round
    user_cumulative: dict[str, dict[int, float]] = defaultdict(dict)
    user_running: dict[str, float] = defaultdict(float)
    all_rounds = sorted(set(s["race_round"] for s in all_scores))

    for s in all_scores:
        username = s["username"] or str(s["user_id"])
        user_running[username] += s["fantasy_points"]
        user_cumulative[username][s["race_round"]] = user_running[username]

    # Text-based chart
    lines = ["\U0001f4c8 *Standings Progression*\n"]
    lines.append("```")

    # Header
    header = f"{'Manager':<12}"
    for r in all_rounds:
        header += f" R{r:>2}"
    header += "  Total"
    lines.append(header)
    lines.append("-" * len(header))

    # Sort by final total
    final_totals = {u: max(rounds.values()) for u, rounds in user_cumulative.items()}
    sorted_users = sorted(final_totals, key=lambda u: -final_totals[u])

    for username in sorted_users:
        row = f"{username:<12}"
        for r in all_rounds:
            pts = user_cumulative[username].get(r)
            if pts is not None:
                row += f" {pts:>3.0f}"
            else:
                row += "   -"
        row += f"  {final_totals[username]:>5.0f}"
        lines.append(row)

    lines.append("```")

    # Bar chart for current standings
    if final_totals:
        max_pts = max(final_totals.values())
        bar_width = 20
        lines.append("")
        for username in sorted_users:
            pts = final_totals[username]
            filled = int((pts / max_pts) * bar_width) if max_pts > 0 else 0
            bar = "\u2588" * filled + "\u2591" * (bar_width - filled)
            lines.append(f"`{username:<10} {bar} {pts:.0f}`")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ── Group inline quick buttons ──

async def group_quick_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show quick action buttons in group chat."""
    if update.effective_chat.type == "private":
        return

    bot_username = (await context.bot.get_me()).username
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("\U0001f4ca Standings", callback_data="grp_standings"),
            InlineKeyboardButton("\U0001f3c1 Next Race", callback_data="grp_nextrace"),
        ],
        [
            InlineKeyboardButton("\U0001f3ce My Team", url=f"https://t.me/{bot_username}?start=myteam"),
            InlineKeyboardButton("\U0001f52e Predict", url=f"https://t.me/{bot_username}?start=predict"),
        ],
        [
            InlineKeyboardButton("\U0001f4c8 Chart", callback_data="grp_chart"),
            InlineKeyboardButton("\U0001f3af Pred.Table", callback_data="grp_predstandings"),
        ],
        [
            InlineKeyboardButton("\U0001f3af Survivor", callback_data="grp_survivor"),
        ],
    ])
    await update.message.reply_text(
        "\U0001f3ce *F1 Fantasy \u2014 Quick Menu*",
        reply_markup=keyboard,
        parse_mode="Markdown",
    )


async def group_inline_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle group inline button presses."""
    query = update.callback_query
    await query.answer()

    if query.data == "grp_standings":
        from handlers.standings import standings_command
        # Create a fake update with message for the handler
        from utils.formatters import format_standings_table
        db = _get_db(context)
        standings = await db.get_standings()
        if standings:
            text = format_standings_table(standings)
        else:
            text = "No standings yet."
        await query.message.reply_text(text, parse_mode="Markdown")

    elif query.data == "grp_nextrace":
        from utils.formatters import format_race_info
        db = _get_db(context)
        race = await db.get_next_race()
        if race:
            text = format_race_info(race)
        else:
            text = "No upcoming races."
        await query.message.reply_text(text, parse_mode="Markdown")

    elif query.data == "grp_chart":
        # Reuse chart logic
        update_proxy = type('obj', (object,), {
            'message': query.message,
            'effective_chat': update.effective_chat,
            'effective_user': update.effective_user,
        })()
        await chart_command(update_proxy, context)

    elif query.data == "grp_predstandings":
        update_proxy = type('obj', (object,), {
            'message': query.message,
            'effective_chat': update.effective_chat,
            'effective_user': update.effective_user,
        })()
        await predstandings_command(update_proxy, context)

    elif query.data == "grp_survivor":
        from handlers.survivor import survivor_standings_command
        update_proxy = type('obj', (object,), {
            'message': query.message,
            'effective_chat': update.effective_chat,
            'effective_user': update.effective_user,
        })()
        await survivor_standings_command(update_proxy, context)


def setup_extras_handlers(app: Application) -> None:
    app.add_handler(CommandHandler("predstandings", predstandings_command))
    app.add_handler(CommandHandler("history", history_command))
    app.add_handler(CommandHandler("chart", chart_command))
    app.add_handler(CommandHandler("menu", group_quick_menu))
    from telegram.ext import CallbackQueryHandler
    app.add_handler(CallbackQueryHandler(group_inline_callback, pattern=r"^grp_"))
