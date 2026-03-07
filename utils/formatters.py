from __future__ import annotations

from datetime import datetime, timezone

from services.budget import get_constructor_name, get_driver_name

COUNTRY_FLAGS = {
    "Bahrain": "\U0001f1e7\U0001f1ed",
    "Saudi Arabia": "\U0001f1f8\U0001f1e6",
    "Australia": "\U0001f1e6\U0001f1fa",
    "Japan": "\U0001f1ef\U0001f1f5",
    "China": "\U0001f1e8\U0001f1f3",
    "USA": "\U0001f1fa\U0001f1f8",
    "United States": "\U0001f1fa\U0001f1f8",
    "Italy": "\U0001f1ee\U0001f1f9",
    "Monaco": "\U0001f1f2\U0001f1e8",
    "Canada": "\U0001f1e8\U0001f1e6",
    "Spain": "\U0001f1ea\U0001f1f8",
    "Austria": "\U0001f1e6\U0001f1f9",
    "UK": "\U0001f1ec\U0001f1e7",
    "United Kingdom": "\U0001f1ec\U0001f1e7",
    "Hungary": "\U0001f1ed\U0001f1fa",
    "Belgium": "\U0001f1e7\U0001f1ea",
    "Netherlands": "\U0001f1f3\U0001f1f1",
    "Singapore": "\U0001f1f8\U0001f1ec",
    "Azerbaijan": "\U0001f1e6\U0001f1ff",
    "Mexico": "\U0001f1f2\U0001f1fd",
    "Brazil": "\U0001f1e7\U0001f1f7",
    "Las Vegas": "\U0001f1fa\U0001f1f8",
    "Qatar": "\U0001f1f6\U0001f1e6",
    "Abu Dhabi": "\U0001f1e6\U0001f1ea",
    "UAE": "\U0001f1e6\U0001f1ea",
    "Portugal": "\U0001f1f5\U0001f1f9",
    "France": "\U0001f1eb\U0001f1f7",
    "Germany": "\U0001f1e9\U0001f1ea",
    "Miami": "\U0001f1fa\U0001f1f8",
}

POSITION_EMOJI = {
    1: "\U0001f947", 2: "\U0001f948", 3: "\U0001f949",
}


def format_standings_table(
    standings: list,
    highlight_user_id: int | None = None,
) -> str:
    """Format league standings as monospace table."""
    if not standings:
        return "Нет данных о standings."

    lines = ["\U0001f3c6 *F1 Fantasy \u2014 Standings*\n"]
    lines.append("```")
    lines.append(f"{'#':>2} {'Manager':<14} {'Pts':>6}")
    lines.append("-" * 24)

    for i, row in enumerate(standings, 1):
        if hasattr(row, "keys"):
            username = row.get("username", "???")
            points = row.get("total_points", 0)
        else:
            # tuple from DB: (user_id, username, total_points)
            username = row[1] or "???"
            points = row[2]

        pos_mark = POSITION_EMOJI.get(i, f"{i:>2}")
        marker = " <" if highlight_user_id and (
            (hasattr(row, "keys") and row.get("user_id") == highlight_user_id) or
            (not hasattr(row, "keys") and row[0] == highlight_user_id)
        ) else ""

        # In monospace we use plain numbers
        lines.append(f"{i:>2} {username:<14} {points:>6.0f}{marker}")

    lines.append("```")
    return "\n".join(lines)


def format_race_scores(
    scores: list,
    race_round: int,
) -> str:
    """Format race round scores."""
    if not scores:
        return "Нет результатов за этот раунд."

    lines = [f"\U0001f4ca *Fantasy \u2014 Round {race_round}*\n"]
    lines.append("```")
    lines.append(f"{'#':>2} {'Manager':<14} {'Pts':>6}")
    lines.append("-" * 24)

    for i, score in enumerate(scores, 1):
        username = "???"
        pts = score.fantasy_points if hasattr(score, "fantasy_points") else score.get("fantasy_points", 0)
        if hasattr(score, "breakdown"):
            bd = score.breakdown if isinstance(score.breakdown, dict) else {}
        else:
            bd = {}

        # Try to get username from breakdown or user_id
        if hasattr(score, "user_id"):
            username = str(score.user_id)

        lines.append(f"{i:>2} {'@' + username:<14} {pts:>6.0f}")

    lines.append("```")
    return "\n".join(lines)


def format_driver_scores(breakdown: dict) -> str:
    """Format detailed driver score breakdown."""
    lines = []
    for driver_id, scores in breakdown.get("drivers", {}).items():
        name = get_driver_name(driver_id)
        if isinstance(scores, dict):
            total = scores.get("total", 0)
            parts = []
            if scores.get("race", 0): parts.append(f"Race: {scores['race']}")
            if scores.get("qualifying", 0): parts.append(f"Quali: {scores['qualifying']}")
            if scores.get("position_gain", 0): parts.append(f"Pos: +{scores['position_gain']}")
            if scores.get("beat_teammate", 0): parts.append(f"TM: +{scores['beat_teammate']}")
            if scores.get("fastest_lap", 0): parts.append(f"FL: +{scores['fastest_lap']}")
            if scores.get("dnf", 0): parts.append(f"DNF: {scores['dnf']}")
            if scores.get("turbo_bonus", 0): parts.append(f"Turbo: +{scores['turbo_bonus']}")
            if scores.get("sprint", 0): parts.append(f"Sprint: +{scores['sprint']}")
            detail = ", ".join(parts) if parts else "0"
            is_turbo = driver_id == breakdown.get("turbo_driver", "")
            turbo_mark = " \u26a1" if is_turbo else ""
            lines.append(f"  {name}{turbo_mark}: {total} ({detail})")
    return "\n".join(lines)


def format_team_summary(team, compact: bool = False) -> str:
    """Format a user's team."""
    turbo = team.turbo_driver
    if compact:
        drivers = ", ".join(get_driver_name(d) for d in team.drivers)
        return f"Drivers: {drivers} | Constructor: {get_constructor_name(team.constructor)} | Turbo: {get_driver_name(turbo)}"

    drivers_str = "\n".join(
        f"  {'⚡ ' if d == turbo else '   '}{get_driver_name(d)}"
        for d in team.drivers
    )
    return (
        f"\U0001f3ce Pilots:\n{drivers_str}\n"
        f"\U0001f3d7 Constructor: {get_constructor_name(team.constructor)}\n"
        f"\u26a1 DRS Boost: {get_driver_name(turbo)}"
    )


def format_race_info(race) -> str:
    """Format race info with flag and schedule."""
    flag = COUNTRY_FLAGS.get(race.country, "\U0001f3c1")
    sprint_tag = " \U0001f3ce Sprint Weekend" if race.sprint else ""

    lines = [
        f"{flag} *{race.name}*{sprint_tag}",
        f"\U0001f3df {race.circuit}, {race.country}",
        "",
        f"\U0001f4c5 Qualifying: {_format_dt(race.qualifying_datetime)}",
        f"\U0001f3c1 Race: {_format_dt(race.race_datetime)}",
    ]

    deadline = datetime.fromisoformat(race.qualifying_datetime)
    countdown = format_countdown(deadline)
    lines.append(f"\n\u23f0 Deadline: {countdown}")
    lines.append(f"\n\U0001f449 /pickteam | /predict | /survivor")

    return "\n".join(lines)


def format_countdown(target: datetime) -> str:
    """Format time remaining until target."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    if now > target:
        return "\u26a0\ufe0f Deadline passed!"

    delta = target - now
    days = delta.days
    hours = delta.seconds // 3600
    minutes = (delta.seconds % 3600) // 60

    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    parts.append(f"{minutes}min")
    return " ".join(parts)


def format_awards(awards: list) -> str:
    """Format round awards."""
    if not awards:
        return ""
    lines = ["\U0001f3c6 *Awards:*\n"]
    for a in awards:
        lines.append(f"{a['emoji']} *{a['title']}*: @{a['user']} \u2014 {a['description']}")
    return "\n".join(lines)


def format_predictions_summary(user_predictions: dict, actual_results: dict) -> str:
    """Format prediction game results."""
    lines = ["\U0001f3af *Predictions:*\n"]
    for qid, pred in user_predictions.items():
        answer = pred.get("answer", False)
        confidence = pred.get("confidence", 1)
        actual = actual_results.get(qid, False)
        correct = answer == actual
        mark = "\u2705" if correct else "\u274c"
        pts = confidence if correct else 0
        lines.append(f"{mark} Q{qid}: conf={confidence} \u2192 +{pts}")
    return "\n".join(lines)


def _format_dt(dt_str: str) -> str:
    """Format datetime string for display."""
    try:
        dt = datetime.fromisoformat(dt_str)
        return dt.strftime("%a %d %b, %H:%M UTC")
    except ValueError:
        return dt_str


def escape_markdown_v2(text: str) -> str:
    """Escape special characters for MarkdownV2."""
    special = r"_*[]()~`>#+-=|{}.!"
    for char in special:
        text = text.replace(char, f"\\{char}")
    return text
