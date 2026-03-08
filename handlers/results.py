from __future__ import annotations

import json

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from data.database import Database
from services.budget import get_driver_name
from utils.formatters import format_driver_scores


def _get_db(context: ContextTypes.DEFAULT_TYPE) -> Database:
    return context.bot_data["db"]


async def results_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db = _get_db(context)

    # Check if specific round requested
    round_num = None
    if context.args:
        try:
            round_num = int(context.args[0])
        except ValueError:
            pass

    if round_num is None:
        # Find latest round with scores
        standings = await db.get_standings()
        if not standings:
            await update.message.reply_text("Ещё нет результатов.")
            return
        # Get max round from scores
        async with db.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT MAX(race_round) as max_round FROM scores"
            )
        round_num = row["max_round"] if row and row["max_round"] else None

    if round_num is None:
        await update.message.reply_text("Ещё нет результатов.")
        return

    scores = await db.get_race_scores(round_num)
    scores_data = await db.get_race_scores_with_users(round_num)
    if not scores_data:
        await update.message.reply_text(f"Нет результатов за Round {round_num}.")
        return

    race = await db.get_race(round_num)
    race_name = race.name if race else f"Round {round_num}"

    # Get all teams for this round to show who picked whom
    teams = await db.get_all_teams_for_round(round_num)
    driver_owners: dict[str, list[str]] = {}
    for t in teams:
        for d in t.drivers:
            driver_owners.setdefault(d, []).append(f"@{t.username}" if t.username else str(t.user_id))

    # Race results from DB
    race_results = await db.get_race_results(round_num)

    lines = [f"\U0001f3c1 *{race_name} \u2014 Fantasy Results*\n"]

    # Scores table
    lines.append("```")
    lines.append(f"{'#':>2} {'Manager':<14} {'Pts':>6}")
    lines.append("-" * 24)
    for i, s in enumerate(scores_data, 1):
        username = s["username"] if s.get("username") else str(s["user_id"])
        lines.append(f"{i:>2} @{username:<13} {s['fantasy_points']:>6.0f}")
    lines.append("```")

    # Best and worst picks
    if scores_data:
        best = scores_data[0]
        worst = scores_data[-1]
        best_name = f"@{best['username']}" if best.get("username") else str(best["user_id"])
        worst_name = f"@{worst['username']}" if worst.get("username") else str(worst["user_id"])

        lines.append(f"\n\U0001f3c6 *Best Manager:* {best_name} ({best['fantasy_points']:.0f} pts)")
        lines.append(f"\U0001f4a9 *Worst Manager:* {worst_name} ({worst['fantasy_points']:.0f} pts)")

    # Personal breakdown (DM only)
    if update.effective_chat.type == "private":
        user_score = None
        for s in scores_data:
            if s["user_id"] == update.effective_user.id:
                user_score = s
                break
        if user_score and user_score.get("breakdown"):
            breakdown = user_score["breakdown"]
            if isinstance(breakdown, str):
                breakdown = json.loads(breakdown)
            if isinstance(breakdown, dict) and "drivers" in breakdown:
                lines.append(f"\n\U0001f4ca *\u0422\u0432\u043e\u0438 \u043e\u0447\u043a\u0438 \u2014 Round {round_num}*\n")
                lines.append("```")
                for did, dscores in breakdown["drivers"].items():
                    dname = get_driver_name(did)
                    if len(dname) > 14:
                        dname = dname[:13] + "."
                    dtotal = dscores.get("total", 0) if isinstance(dscores, dict) else 0
                    turbo = " \u26a1" if did == breakdown.get("turbo_driver") else ""
                    lines.append(f"{dname:<14} {dtotal:>5.0f}{turbo}")
                # Constructor
                cdata = breakdown.get("constructor", {})
                ctotal = cdata.get("total", 0) if isinstance(cdata, dict) else 0
                lines.append(f"{'Constructor':<14} {ctotal:>5.0f}")
                # Penalty
                penalty = breakdown.get("transfer_penalty", 0)
                if penalty:
                    lines.append(f"{'Transfer pen.':<14} {-penalty:>5.0f}")
                lines.append("```")

    # Top scoring driver
    if race_results:
        best_driver = None
        best_driver_pts = -999
        for r in race_results:
            # Simple heuristic: use finish position points
            pts = 0
            if r.get("finish_position") and r["finish_position"] <= 10:
                from services.scoring import RACE_POINTS
                pts = RACE_POINTS.get(r["finish_position"], 0)
            if r.get("fastest_lap"):
                pts += 10
            if pts > best_driver_pts:
                best_driver_pts = pts
                best_driver = r

        if best_driver:
            name = get_driver_name(best_driver["driver_id"])
            owners = driver_owners.get(best_driver["driver_id"], [])
            owners_str = ", ".join(owners) if owners else "никто"
            lines.append(f"\n\u2b50 *Top driver:* {name} ({best_driver_pts} pts, picked by: {owners_str})")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


def setup_results_handlers(app: Application) -> None:
    app.add_handler(CommandHandler("results", results_command))
