from __future__ import annotations

from data.models import RaceResult, UserTeam

# Points tables
RACE_POINTS = {1: 25, 2: 18, 3: 15, 4: 12, 5: 10, 6: 8, 7: 6, 8: 4, 9: 2, 10: 1}
QUALI_POINTS = {1: 10, 2: 9, 3: 8, 4: 7, 5: 6, 6: 5, 7: 4, 8: 3, 9: 2, 10: 1}
SPRINT_POINTS = {1: 8, 2: 7, 3: 6, 4: 5, 5: 4, 6: 3, 7: 2, 8: 1}

POSITION_GAIN_BONUS = 2       # per position gained
POSITION_LOSS_PENALTY = -1    # per position lost (floor 0 for this component)
BEAT_TEAMMATE_BONUS = 5
FASTEST_LAP_BONUS = 10
DNF_PENALTY = -10
BOTH_Q3_BONUS = 10
PIT_UNDER_2S_BONUS = 20
PIT_2_TO_219S_BONUS = 10
PIT_FASTEST_BONUS = 5


def _get_teammate(driver_id: str, all_results: list[RaceResult]) -> RaceResult | None:
    """Find teammate based on initial_prices team mapping."""
    from services.budget import get_all_drivers
    drivers = get_all_drivers()
    driver_map = {d.id: d.team for d in drivers}
    my_team = driver_map.get(driver_id)
    if not my_team:
        return None
    for r in all_results:
        if r.driver_id != driver_id and driver_map.get(r.driver_id) == my_team:
            return r
    return None


def calculate_driver_race_score(
    result: RaceResult,
    quali_position: int | None = None,
    teammate_result: RaceResult | None = None,
) -> dict:
    """Calculate fantasy points for a driver in a race."""
    breakdown = {
        "race": 0,
        "qualifying": 0,
        "position_gain": 0,
        "beat_teammate": 0,
        "fastest_lap": 0,
        "dnf": 0,
        "total": 0,
    }

    # DNF
    if result.dnf:
        breakdown["dnf"] = DNF_PENALTY
        breakdown["total"] = DNF_PENALTY
        # Qualifying points still count even with DNF
        if quali_position and quali_position in QUALI_POINTS:
            breakdown["qualifying"] = QUALI_POINTS[quali_position]
            breakdown["total"] += breakdown["qualifying"]
        return breakdown

    # Race finish points
    if result.finish_position and result.finish_position in RACE_POINTS:
        breakdown["race"] = RACE_POINTS[result.finish_position]

    # Qualifying points
    if quali_position and quali_position in QUALI_POINTS:
        breakdown["qualifying"] = QUALI_POINTS[quali_position]

    # Position gain/loss (grid vs finish)
    if result.finish_position and result.grid_position > 0:
        diff = result.grid_position - result.finish_position
        if diff > 0:
            breakdown["position_gain"] = diff * POSITION_GAIN_BONUS
        elif diff < 0:
            penalty = diff * abs(POSITION_LOSS_PENALTY)
            breakdown["position_gain"] = max(0, penalty)  # floor at 0

    # Beat teammate
    if teammate_result and result.finish_position:
        if teammate_result.dnf:
            breakdown["beat_teammate"] = BEAT_TEAMMATE_BONUS
        elif teammate_result.finish_position and result.finish_position < teammate_result.finish_position:
            breakdown["beat_teammate"] = BEAT_TEAMMATE_BONUS

    # Fastest lap
    if result.fastest_lap:
        breakdown["fastest_lap"] = FASTEST_LAP_BONUS

    breakdown["total"] = sum(v for k, v in breakdown.items() if k != "total")
    return breakdown


def calculate_driver_sprint_score(
    position: int,
    grid: int,
    dnf: bool = False,
) -> dict:
    """Calculate sprint race points."""
    breakdown = {"sprint": 0, "sprint_position_gain": 0, "total": 0}

    if dnf:
        breakdown["total"] = DNF_PENALTY
        return breakdown

    if position in SPRINT_POINTS:
        breakdown["sprint"] = SPRINT_POINTS[position]

    if position and grid > 0:
        diff = grid - position
        if diff > 0:
            breakdown["sprint_position_gain"] = diff * POSITION_GAIN_BONUS

    breakdown["total"] = breakdown["sprint"] + breakdown["sprint_position_gain"]
    return breakdown


def _parse_pit_duration(duration_str: str) -> float | None:
    """Parse pit stop duration string to seconds."""
    try:
        parts = duration_str.split(":")
        if len(parts) == 2:
            return float(parts[0]) * 60 + float(parts[1])
        return float(duration_str)
    except (ValueError, IndexError):
        return None


def calculate_constructor_score(
    driver1_result: RaceResult,
    driver2_result: RaceResult,
    driver1_quali_pos: int | None,
    driver2_quali_pos: int | None,
    pit_stops: list[dict],
    all_pit_stops: list[dict],
    constructor_id: str,
) -> dict:
    """Calculate constructor fantasy points."""
    from services.budget import get_all_drivers
    drivers = get_all_drivers()
    team_driver_ids = [d.id for d in drivers if d.team == constructor_id]

    breakdown = {
        "driver_points": 0,
        "both_q3": 0,
        "pit_stop_bonus": 0,
        "fastest_pit": 0,
        "total": 0,
    }

    # Sum of both drivers' race + quali scores
    d1_score = calculate_driver_race_score(driver1_result, driver1_quali_pos)
    d2_score = calculate_driver_race_score(driver2_result, driver2_quali_pos)
    breakdown["driver_points"] = d1_score["total"] + d2_score["total"]

    # Both drivers in Q3 (positions 1-10)
    if (driver1_quali_pos and driver1_quali_pos <= 10 and
            driver2_quali_pos and driver2_quali_pos <= 10):
        breakdown["both_q3"] = BOTH_Q3_BONUS

    # Pit stop bonuses for this constructor's drivers
    team_pits = [p for p in pit_stops if p.get("driver_id") in team_driver_ids]
    for pit in team_pits:
        dur = _parse_pit_duration(pit.get("duration", ""))
        if dur is not None:
            if dur < 2.0:
                breakdown["pit_stop_bonus"] += PIT_UNDER_2S_BONUS
            elif dur <= 2.19:
                breakdown["pit_stop_bonus"] += PIT_2_TO_219S_BONUS

    # Fastest pit stop of the race
    if all_pit_stops:
        all_durations = []
        for p in all_pit_stops:
            dur = _parse_pit_duration(p.get("duration", ""))
            if dur is not None:
                all_durations.append((dur, p.get("driver_id", "")))
        if all_durations:
            fastest_driver = min(all_durations, key=lambda x: x[0])[1]
            if fastest_driver in team_driver_ids:
                breakdown["fastest_pit"] = PIT_FASTEST_BONUS

    breakdown["total"] = sum(v for k, v in breakdown.items() if k != "total")
    return breakdown


def calculate_team_score(
    user_team: UserTeam,
    race_results: list[RaceResult],
    quali_results: list[dict],
    sprint_results: list[dict] | None,
    pit_stops: list[dict],
    active_chip: str | None = None,
    transfer_penalty: int = 0,
) -> dict:
    """Calculate total fantasy score for a user's team."""
    results_map = {r.driver_id: r for r in race_results}
    quali_map = {q["driver_id"]: q["position"] for q in quali_results}

    team_breakdown = {
        "drivers": {},
        "constructor": {},
        "sprint": {},
        "turbo_driver": user_team.turbo_driver,
        "turbo_multiplier": "2x",
        "chips_active": active_chip or "",
        "transfer_penalty": transfer_penalty,
        "total": 0,
    }

    # Determine turbo multiplier
    turbo_mult = 2
    if active_chip == "TRIPLE_BOOST":
        turbo_mult = 3
        team_breakdown["turbo_multiplier"] = "3x"

    drivers_total = 0
    for driver_id in user_team.drivers:
        result = results_map.get(driver_id)
        if not result:
            team_breakdown["drivers"][driver_id] = {"total": 0, "note": "no result"}
            continue

        quali_pos = quali_map.get(driver_id)
        teammate = _get_teammate(driver_id, race_results)
        driver_score = calculate_driver_race_score(result, quali_pos, teammate)

        # Apply NO_NEGATIVE chip
        if active_chip == "NO_NEGATIVE":
            for key in driver_score:
                if key != "total" and driver_score[key] < 0:
                    driver_score[key] = 0
            driver_score["total"] = sum(v for k, v in driver_score.items() if k != "total")

        # Apply turbo multiplier
        if driver_id == user_team.turbo_driver:
            original = driver_score["total"]
            driver_score["total"] = original * turbo_mult
            driver_score["turbo_bonus"] = original * (turbo_mult - 1)

        # Sprint scoring
        if sprint_results:
            sprint_data = next(
                (s for s in sprint_results if s["driver_id"] == driver_id), None
            )
            if sprint_data:
                sprint_dnf = sprint_data.get("status", "Finished") not in ("Finished", "") and not sprint_data.get("status", "").startswith("+")
                sprint_score = calculate_driver_sprint_score(
                    sprint_data["position"], sprint_data.get("grid", 0), sprint_dnf
                )
                driver_score["sprint"] = sprint_score["total"]
                driver_score["total"] += sprint_score["total"]
                team_breakdown["sprint"][driver_id] = sprint_score

        team_breakdown["drivers"][driver_id] = driver_score
        drivers_total += driver_score["total"]

    # Constructor scoring
    from services.budget import get_all_drivers
    all_drivers = get_all_drivers()
    constructor_driver_ids = [
        d.id for d in all_drivers if d.team == user_team.constructor
    ]

    if len(constructor_driver_ids) >= 2:
        d1_result = results_map.get(constructor_driver_ids[0])
        d2_result = results_map.get(constructor_driver_ids[1])
        if d1_result and d2_result:
            constructor_score = calculate_constructor_score(
                d1_result, d2_result,
                quali_map.get(constructor_driver_ids[0]),
                quali_map.get(constructor_driver_ids[1]),
                pit_stops, pit_stops, user_team.constructor,
            )
            if active_chip == "NO_NEGATIVE":
                for key in constructor_score:
                    if key != "total" and constructor_score[key] < 0:
                        constructor_score[key] = 0
                constructor_score["total"] = sum(
                    v for k, v in constructor_score.items() if k != "total"
                )
            team_breakdown["constructor"] = constructor_score
        else:
            team_breakdown["constructor"] = {"total": 0, "note": "missing results"}
    else:
        team_breakdown["constructor"] = {"total": 0, "note": "not enough drivers"}

    constructor_total = team_breakdown["constructor"].get("total", 0)
    team_breakdown["total"] = drivers_total + constructor_total - transfer_penalty

    return team_breakdown
