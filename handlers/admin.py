from __future__ import annotations

import json
import logging
import os
import random
from datetime import datetime, timezone

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from config import settings
from data.database import Database
from data.models import Race, RaceResult, UserTeam
from services.budget import get_all_drivers, get_driver_name
from services.scoring import calculate_team_score
from services.transfers import TransferService
from utils.decorators import admin_only

logger = logging.getLogger(__name__)


def _get_db(context: ContextTypes.DEFAULT_TYPE) -> Database:
    return context.bot_data["db"]


@admin_only
async def admin_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db = _get_db(context)

    users = await db.get_all_users()
    race = await db.get_next_race()
    jobs = context.job_queue.jobs() if context.job_queue else []

    db_size = "?"
    if os.path.exists(settings.DB_PATH):
        size_bytes = os.path.getsize(settings.DB_PATH)
        db_size = f"{size_bytes / 1024:.1f} KB"

    lines = [
        "\U0001f527 *Admin Status*\n",
        f"Users: {len(users)}",
        f"DB size: {db_size}",
        f"Active jobs: {len(list(jobs))}",
    ]

    if race:
        deadline = datetime.fromisoformat(race.qualifying_datetime)
        delta = deadline - datetime.now(timezone.utc).replace(tzinfo=None)
        lines.append(f"\nNext race: {race.name} (Round {race.round})")
        lines.append(f"Deadline: {deadline.strftime('%Y-%m-%d %H:%M UTC')}")
        if delta.total_seconds() > 0:
            lines.append(f"Time left: {delta.days}d {delta.seconds // 3600}h")
        else:
            lines.append("Deadline: PASSED")

        teams = await db.get_all_teams_for_round(race.round)
        lines.append(f"Teams submitted: {len(teams)}/{len(users)}")
    else:
        lines.append("\nNo upcoming races")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


@admin_only
async def admin_addrace(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Usage: /admin_addrace <round> <name> <country> <quali_dt> <race_dt> [sprint]"""
    if not context.args or len(context.args) < 5:
        await update.message.reply_text(
            "Usage: /admin\\_addrace round name country quali\\_datetime race\\_datetime [sprint]\n"
            "Example: /admin\\_addrace 1 Bahrain\\_GP Bahrain 2026-03-14T15:00:00 2026-03-15T15:00:00 false",
            parse_mode="Markdown",
        )
        return

    db = _get_db(context)
    try:
        round_num = int(context.args[0])
        name = context.args[1].replace("_", " ")
        country = context.args[2].replace("_", " ")
        quali_dt = context.args[3]
        race_dt = context.args[4]
        sprint = context.args[5].lower() == "true" if len(context.args) > 5 else False

        race = Race(
            round=round_num, name=name, country=country, circuit="",
            qualifying_datetime=quali_dt, race_datetime=race_dt, sprint=sprint,
        )
        await db.save_race(race)
        await update.message.reply_text(f"\u2705 Race added: Round {round_num} \u2014 {name}")
    except Exception as e:
        await update.message.reply_text(f"\u274c Error: {e}")


@admin_only
async def admin_forcescore(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Usage: /admin_forcescore <round>"""
    if not context.args:
        await update.message.reply_text("Usage: /admin\\_forcescore round", parse_mode="Markdown")
        return

    db = _get_db(context)
    round_num = int(context.args[0])

    results = await db.get_race_results(round_num)
    if not results:
        await update.message.reply_text(f"No race results found for round {round_num}. Use /admin\\_simulate first.", parse_mode="Markdown")
        return

    # Trigger scoring
    await update.message.reply_text(f"Forcing score calculation for round {round_num}...")

    from data.models import RaceResult as RR
    race_results = [
        RR(
            round=r["round"], driver_id=r["driver_id"],
            grid_position=r["grid_position"],
            finish_position=r.get("finish_position"),
            dnf=bool(r.get("dnf", False)),
            fastest_lap=bool(r.get("fastest_lap", False)),
        )
        for r in results
    ]

    quali_results = [{"driver_id": r["driver_id"], "position": r["grid_position"]} for r in results]
    teams = await db.get_all_teams_for_round(round_num)
    transfer_svc = TransferService(db)

    for team in teams:
        active_chip = await db.get_active_chip(team.user_id, round_num)
        penalty = await transfer_svc.get_transfer_penalty(team.user_id, round_num)
        breakdown = calculate_team_score(
            team, race_results, quali_results, None, [],
            active_chip=active_chip, transfer_penalty=penalty,
        )
        await db.save_score(team.user_id, round_num, breakdown["total"], breakdown)

    await update.message.reply_text(f"\u2705 Scored {len(teams)} teams for round {round_num}")


@admin_only
async def admin_reveal(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Usage: /admin_reveal [round]"""
    db = _get_db(context)

    round_num = None
    if context.args:
        round_num = int(context.args[0])
    else:
        race = await db.get_next_race()
        round_num = race.round if race else None

    if round_num is None:
        await update.message.reply_text("No round specified and no upcoming race.")
        return

    teams = await db.get_all_teams_for_round(round_num)
    if not teams:
        await update.message.reply_text(f"No teams for round {round_num}.")
        return

    lines = [f"\U0001f441 *All teams \u2014 Round {round_num}*\n"]
    for t in teams:
        username = f"@{t.username}" if t.username else str(t.user_id)
        drivers = ", ".join(get_driver_name(d) for d in t.drivers)
        turbo = get_driver_name(t.turbo_driver)
        chip = await db.get_active_chip(t.user_id, round_num)
        chip_str = f" | Chip: {chip}" if chip else ""
        lines.append(f"*{username}:*")
        lines.append(f"  \U0001f3ce {drivers}")
        lines.append(f"  \u26a1 {turbo}{chip_str}\n")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


@admin_only
async def admin_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Usage: /admin_broadcast <message>"""
    if not context.args:
        await update.message.reply_text("Usage: /admin\\_broadcast message", parse_mode="Markdown")
        return

    db = _get_db(context)
    message = " ".join(context.args)
    users = await db.get_all_users()
    sent = 0

    for user in users:
        try:
            await context.bot.send_message(
                chat_id=user["telegram_id"],
                text=f"\U0001f4e2 *Broadcast:*\n\n{message}",
                parse_mode="Markdown",
            )
            sent += 1
        except Exception:
            pass

    await update.message.reply_text(f"\u2705 Broadcast sent to {sent}/{len(users)} users")


@admin_only
async def admin_resetuser(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Usage: /admin_resetuser <user_id> <round>"""
    if not context.args or len(context.args) < 2:
        await update.message.reply_text("Usage: /admin\\_resetuser user\\_id round", parse_mode="Markdown")
        return

    db = _get_db(context)
    user_id = int(context.args[0])
    round_num = int(context.args[1])

    await db.db.execute(
        "DELETE FROM teams WHERE user_id = ? AND race_round = ?",
        (user_id, round_num),
    )
    await db.db.execute(
        "DELETE FROM scores WHERE user_id = ? AND race_round = ?",
        (user_id, round_num),
    )
    await db.db.commit()

    await update.message.reply_text(f"\u2705 Reset user {user_id} for round {round_num}")


@admin_only
async def admin_backup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send SQLite database file to admin DM."""
    if not os.path.exists(settings.DB_PATH):
        await update.message.reply_text("Database file not found.")
        return

    await context.bot.send_document(
        chat_id=update.effective_user.id,
        document=open(settings.DB_PATH, "rb"),
        filename=f"fantasy_backup_{datetime.now(timezone.utc).replace(tzinfo=None).strftime('%Y%m%d_%H%M')}.db",
        caption="\U0001f4be Database backup",
    )


@admin_only
async def admin_setprices(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Reload prices from initial_prices.json."""
    import services.budget as budget_mod
    budget_mod._prices_cache = None
    prices = budget_mod.load_prices()
    await update.message.reply_text(
        f"\u2705 Prices reloaded: {len(prices['drivers'])} drivers, {len(prices['constructors'])} constructors"
    )


@admin_only
async def admin_simulate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Usage: /admin_simulate <round> — generate fake results and score."""
    if not context.args:
        await update.message.reply_text("Usage: /admin\\_simulate round", parse_mode="Markdown")
        return

    db = _get_db(context)
    round_num = int(context.args[0])

    drivers = get_all_drivers()
    driver_ids = [d.id for d in drivers]
    random.shuffle(driver_ids)

    # Generate random results
    results_dicts = []
    fastest_lap_idx = random.randint(0, min(9, len(driver_ids) - 1))

    for i, driver_id in enumerate(driver_ids):
        pos = i + 1
        dnf = pos > 18 and random.random() < 0.5
        grid = random.randint(max(1, pos - 5), min(20, pos + 5))

        results_dicts.append({
            "round": round_num,
            "driver_id": driver_id,
            "grid_position": grid,
            "finish_position": None if dnf else pos,
            "dnf": dnf,
            "fastest_lap": i == fastest_lap_idx and not dnf,
            "points_scored": 0.0,
        })

    await db.save_race_results(results_dicts)

    # Score all teams
    race_results = [
        RaceResult(
            round=r["round"], driver_id=r["driver_id"],
            grid_position=r["grid_position"],
            finish_position=r["finish_position"],
            dnf=r["dnf"], fastest_lap=r["fastest_lap"],
        )
        for r in results_dicts
    ]

    quali_results = [{"driver_id": r["driver_id"], "position": r["grid_position"]} for r in results_dicts]
    teams = await db.get_all_teams_for_round(round_num)
    transfer_svc = TransferService(db)

    scored = 0
    for team in teams:
        active_chip = await db.get_active_chip(team.user_id, round_num)
        penalty = await transfer_svc.get_transfer_penalty(team.user_id, round_num)
        breakdown = calculate_team_score(
            team, race_results, quali_results, None, [],
            active_chip=active_chip, transfer_penalty=penalty,
        )
        await db.save_score(team.user_id, round_num, breakdown["total"], breakdown)
        scored += 1

    # Show results summary
    lines = [f"\U0001f3b2 *Simulation \u2014 Round {round_num}*\n"]
    lines.append("Race results (random):")
    for r in results_dicts[:10]:
        pos = r["finish_position"] if r["finish_position"] else "DNF"
        fl = " FL" if r["fastest_lap"] else ""
        lines.append(f"  P{pos} {get_driver_name(r['driver_id'])}{fl}")

    if scored:
        lines.append(f"\nScored {scored} teams. Use /results {round_num} to see scores.")
    else:
        lines.append("\nNo teams to score. Users need to /pickteam first.")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


@admin_only
async def admin_export(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Export all data as JSON."""
    db = _get_db(context)
    data = await db.export_all_data()

    # Send as JSON file
    json_bytes = json.dumps(data, indent=2, ensure_ascii=False, default=str).encode("utf-8")
    buf = __import__("io").BytesIO(json_bytes)
    buf.name = f"fantasy_export_{datetime.now(timezone.utc).replace(tzinfo=None).strftime('%Y%m%d_%H%M')}.json"

    await context.bot.send_document(
        chat_id=update.effective_user.id,
        document=buf,
        caption="\U0001f4be Full data export (JSON)",
    )


@admin_only
async def admin_export_csv(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Export standings and scores as CSV."""
    import csv
    import io

    db = _get_db(context)

    # Standings CSV
    standings = await db.get_standings()
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["Rank", "User ID", "Username", "Total Points"])
    for i, row in enumerate(standings, 1):
        row = dict(row) if not isinstance(row, dict) else row
        writer.writerow([i, row["user_id"], row.get("username", ""), row.get("total_points", 0)])

    standings_bytes = buf.getvalue().encode("utf-8")
    standings_buf = io.BytesIO(standings_bytes)
    standings_buf.name = f"standings_{datetime.now(timezone.utc).replace(tzinfo=None).strftime('%Y%m%d')}.csv"

    await context.bot.send_document(
        chat_id=update.effective_user.id,
        document=standings_buf,
        caption="\U0001f4ca Standings (CSV)",
    )

    # Scores per round CSV
    data = await db.export_all_data("scores")
    scores = data.get("scores", [])
    if scores:
        buf2 = io.StringIO()
        writer2 = csv.writer(buf2)
        writer2.writerow(["User ID", "Race Round", "Fantasy Points", "Created At"])
        for s in scores:
            writer2.writerow([s["user_id"], s["race_round"], s["fantasy_points"], s.get("created_at", "")])

        scores_bytes = buf2.getvalue().encode("utf-8")
        scores_buf = io.BytesIO(scores_bytes)
        scores_buf.name = f"scores_{datetime.now(timezone.utc).replace(tzinfo=None).strftime('%Y%m%d')}.csv"

        await context.bot.send_document(
            chat_id=update.effective_user.id,
            document=scores_buf,
            caption="\U0001f4ca All scores (CSV)",
        )


@admin_only
async def admin_cancelrace(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Usage: /admin_cancelrace <round>"""
    if not context.args:
        await update.message.reply_text("Usage: /admin\\_cancelrace round", parse_mode="Markdown")
        return

    db = _get_db(context)
    round_num = int(context.args[0])

    race = await db.get_race(round_num)
    if not race:
        await update.message.reply_text(f"Round {round_num} not found.")
        return

    await db.cancel_race(round_num)

    # Cancel scheduled jobs for this round
    removed = 0
    if context.job_queue:
        prefix = f"r{round_num}"
        for job in context.job_queue.jobs():
            if job.name and job.name.startswith(prefix):
                job.schedule_removal()
                removed += 1

    await update.message.reply_text(
        f"\u2705 Round {round_num} ({race.name}) cancelled.\n"
        f"Removed {removed} scheduled jobs.\n"
        f"All teams, scores, and predictions for this round deleted."
    )

    # Notify group
    for chat_id in settings.GROUP_CHAT_IDS:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"\u26a0\ufe0f *Round {round_num} ({race.name}) has been cancelled.*",
                parse_mode="Markdown",
            )
        except Exception:
            pass


@admin_only
async def admin_health(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show health status of API clients, database, and bot uptime."""
    db = _get_db(context)
    f1_data = context.bot_data["f1_data"]

    # Database status
    db_connected = db.db is not None
    db_size = "?"
    if os.path.exists(settings.DB_PATH):
        size_bytes = os.path.getsize(settings.DB_PATH)
        if size_bytes >= 1024 * 1024:
            db_size = f"{size_bytes / (1024 * 1024):.2f} MB"
        else:
            db_size = f"{size_bytes / 1024:.1f} KB"

    # Jolpica circuit breaker
    jolpica_cb = f1_data.jolpica._circuit_breaker
    jolpica_state = "OPEN" if jolpica_cb._open else "CLOSED"
    jolpica_failures = jolpica_cb._failures
    jolpica_threshold = jolpica_cb._threshold

    # OpenF1 circuit breaker
    openf1_cb = f1_data.openf1._circuit_breaker
    openf1_state = "OPEN" if openf1_cb._open else "CLOSED"
    openf1_failures = openf1_cb._failures
    openf1_threshold = openf1_cb._threshold

    # Cache stats
    jolpica_cache_count = len(f1_data.jolpica._cache._store)
    openf1_cache_count = len(f1_data.openf1._cache._store)

    # Bot uptime
    start_time = context.bot_data.get("start_time")
    if start_time:
        uptime = datetime.now(timezone.utc) - start_time
        days = uptime.days
        hours, remainder = divmod(uptime.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        uptime_str = f"{days}d {hours}h {minutes}m {seconds}s"
    else:
        uptime_str = "unknown"

    lines = [
        "*System Health*\n",
        "*Database:*",
        f"  Status: {'connected' if db_connected else 'disconnected'}",
        f"  Size: {db_size}",
        "",
        "*Jolpica API:*",
        f"  Circuit breaker: {jolpica_state}",
        f"  Failures: {jolpica_failures}/{jolpica_threshold}",
        "",
        "*OpenF1 API:*",
        f"  Circuit breaker: {openf1_state}",
        f"  Failures: {openf1_failures}/{openf1_threshold}",
        "",
        "*Cache:*",
        f"  Jolpica entries: {jolpica_cache_count}",
        f"  OpenF1 entries: {openf1_cache_count}",
        "",
        f"*Uptime:* {uptime_str}",
    ]

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


def setup_admin_handlers(app: Application) -> None:
    app.add_handler(CommandHandler("admin_status", admin_status))
    app.add_handler(CommandHandler("admin_addrace", admin_addrace))
    app.add_handler(CommandHandler("admin_forcescore", admin_forcescore))
    app.add_handler(CommandHandler("admin_reveal", admin_reveal))
    app.add_handler(CommandHandler("admin_broadcast", admin_broadcast))
    app.add_handler(CommandHandler("admin_resetuser", admin_resetuser))
    app.add_handler(CommandHandler("admin_backup", admin_backup))
    app.add_handler(CommandHandler("admin_setprices", admin_setprices))
    app.add_handler(CommandHandler("admin_simulate", admin_simulate))
    app.add_handler(CommandHandler("admin_export", admin_export))
    app.add_handler(CommandHandler("admin_export_csv", admin_export_csv))
    app.add_handler(CommandHandler("admin_cancelrace", admin_cancelrace))
    app.add_handler(CommandHandler("admin_health", admin_health))
