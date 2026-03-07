from __future__ import annotations

import json
import os
import threading

from config import settings
from data.models import Driver, Constructor

PRICES_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "initial_prices.json")
TOTAL_BUDGET = settings.TOTAL_BUDGET
MIN_PRICE = 4.0

_prices_lock = threading.Lock()
_prices_cache: dict | None = None


def load_prices() -> dict:
    global _prices_cache
    if _prices_cache is not None:
        return _prices_cache
    with _prices_lock:
        if _prices_cache is not None:
            return _prices_cache
        with open(PRICES_PATH) as f:
            data = json.load(f)
        _prices_cache = data
        return data


def get_all_drivers() -> list[Driver]:
    data = load_prices()
    return [Driver(**d) for d in data["drivers"]]


def get_all_constructors() -> list[Constructor]:
    data = load_prices()
    return [Constructor(**c) for c in data["constructors"]]


def get_driver_price(driver_id: str) -> float:
    for d in load_prices()["drivers"]:
        if d["id"] == driver_id:
            return d["price"]
    return 0.0


def get_constructor_price(constructor_id: str) -> float:
    for c in load_prices()["constructors"]:
        if c["id"] == constructor_id:
            return c["price"]
    return 0.0


def get_driver_name(driver_id: str) -> str:
    for d in load_prices()["drivers"]:
        if d["id"] == driver_id:
            return d["name"]
    return driver_id


def get_constructor_name(constructor_id: str) -> str:
    for c in load_prices()["constructors"]:
        if c["id"] == constructor_id:
            return c["name"]
    return constructor_id


def calculate_team_cost(driver_ids: list[str], constructor_id: str) -> float:
    cost = sum(get_driver_price(d) for d in driver_ids)
    cost += get_constructor_price(constructor_id)
    return cost


def calculate_remaining_budget(driver_ids: list[str], constructor_id: str) -> float:
    return TOTAL_BUDGET - calculate_team_cost(driver_ids, constructor_id)


def validate_team(
    driver_ids: list[str], constructor_id: str, budget: float = TOTAL_BUDGET
) -> tuple[bool, str]:
    if len(driver_ids) != 5:
        return False, "Нужно выбрать ровно 5 пилотов"
    if len(set(driver_ids)) != 5:
        return False, "Пилоты не должны повторяться"
    if not constructor_id:
        return False, "Нужно выбрать конструктора"

    all_driver_ids = {d["id"] for d in load_prices()["drivers"]}
    for d in driver_ids:
        if d not in all_driver_ids:
            return False, f"Неизвестный пилот: {d}"

    all_constructor_ids = {c["id"] for c in load_prices()["constructors"]}
    if constructor_id not in all_constructor_ids:
        return False, f"Неизвестный конструктор: {constructor_id}"

    cost = calculate_team_cost(driver_ids, constructor_id)
    if cost > budget:
        return False, f"Превышен бюджет: ${cost:.1f}M > ${budget:.1f}M"

    return True, "OK"


def get_affordable_drivers(
    current_drivers: list[str], remaining_budget: float
) -> list[Driver]:
    all_drivers = get_all_drivers()
    return [
        d for d in all_drivers
        if d.id not in current_drivers and d.price <= remaining_budget
    ]


def get_affordable_constructors(remaining_budget: float) -> list[Constructor]:
    all_constructors = get_all_constructors()
    return [c for c in all_constructors if c.price <= remaining_budget]
