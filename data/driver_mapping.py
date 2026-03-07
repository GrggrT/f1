from __future__ import annotations

import json
import threading
from pathlib import Path

_PRICES_PATH = Path(__file__).parent / "initial_prices.json"

ACRONYM_TO_ID: dict[str, str] = {
    "VER": "verstappen", "NOR": "norris", "LEC": "leclerc", "PIA": "piastri",
    "HAM": "hamilton", "RUS": "russell", "ANT": "antonelli", "SAI": "sainz",
    "ALO": "alonso", "ALB": "albon", "GAS": "gasly", "LAW": "lawson",
    "HAD": "hadjar", "STR": "stroll", "OCO": "ocon", "HUL": "hulkenberg",
    "PER": "perez", "BOR": "bortoleto", "BEA": "bearman", "BOT": "bottas",
    "COL": "colapinto", "LIN": "lindblad",
}

_KNOWN_NUMBERS: dict[int, str] = {
    1: "verstappen", 4: "norris", 16: "leclerc", 81: "piastri",
    44: "hamilton", 63: "russell", 12: "antonelli", 55: "sainz",
    14: "alonso", 23: "albon", 10: "gasly", 30: "lawson",
    6: "hadjar", 18: "stroll", 31: "ocon", 27: "hulkenberg",
    11: "perez", 5: "bortoleto", 87: "bearman", 77: "bottas",
    43: "colapinto", 2: "lindblad",
}

_number_to_id: dict[int, str] = dict(_KNOWN_NUMBERS)

_mapping_lock = threading.Lock()

_prices_data: dict | None = None


def _load_prices() -> dict:
    global _prices_data
    if _prices_data is None:
        with open(_PRICES_PATH) as f:
            _prices_data = json.load(f)
    return _prices_data


def number_to_id(driver_number: int) -> str | None:
    return _number_to_id.get(driver_number)


def id_to_number(driver_id: str) -> int | None:
    for num, did in _number_to_id.items():
        if did == driver_id:
            return num
    return None


def update_mapping_from_openf1(drivers_data: list[dict]) -> None:
    with _mapping_lock:
        for driver in drivers_data:
            num = driver.get("driver_number")
            acronym = driver.get("name_acronym", "")
            if num is None:
                continue
            driver_id = ACRONYM_TO_ID.get(acronym)
            if driver_id:
                _number_to_id[num] = driver_id


def get_team_for_driver(driver_id: str) -> str:
    data = _load_prices()
    for d in data["drivers"]:
        if d["id"] == driver_id:
            return d["team"]
    return ""


def get_teammates(driver_id: str) -> list[str]:
    team = get_team_for_driver(driver_id)
    if not team:
        return []
    data = _load_prices()
    return [d["id"] for d in data["drivers"] if d["team"] == team and d["id"] != driver_id]
