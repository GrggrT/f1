from __future__ import annotations

import asyncio
import json
import logging
import os

from data.database import Database
from services.budget import PRICES_PATH, MIN_PRICE

logger = logging.getLogger(__name__)

MAX_CHANGE_PER_RACE = 1.0


async def update_prices_after_race(db: Database, race_round: int) -> dict[str, float]:
    """Update driver prices based on 3-race rolling average performance.

    Returns dict of {driver_id: new_price}.
    """
    # Load current prices (async)
    def _read_prices():
        with open(PRICES_PATH) as f:
            return json.load(f)

    data = await asyncio.to_thread(_read_prices)

    drivers = data["drivers"]
    driver_map = {d["id"]: d for d in drivers}

    # Get last 3 rounds of results
    rounds_to_check = list(range(max(1, race_round - 2), race_round + 1))
    performance: dict[str, list[float]] = {}

    for r in rounds_to_check:
        results = await db.get_race_results(r)
        for res in results:
            driver_id = res["driver_id"]
            pos = res.get("finish_position")
            if pos is not None:
                performance.setdefault(driver_id, []).append(pos)
            elif res.get("dnf"):
                performance.setdefault(driver_id, []).append(20)  # DNF = last

    changes = {}
    for driver_id, positions in performance.items():
        if driver_id not in driver_map:
            continue

        avg_pos = sum(positions) / len(positions)
        current_price = driver_map[driver_id]["price"]

        # Expected position based on price ranking
        sorted_drivers = sorted(drivers, key=lambda d: -d["price"])
        expected_rank = next(
            (i + 1 for i, d in enumerate(sorted_drivers) if d["id"] == driver_id),
            10,
        )

        # If performing better than expected (lower avg_pos), increase price
        diff = expected_rank - avg_pos
        if diff > 3:
            change = min(0.5, MAX_CHANGE_PER_RACE)
        elif diff > 1:
            change = 0.2
        elif diff < -3:
            change = max(-0.5, -MAX_CHANGE_PER_RACE)
        elif diff < -1:
            change = -0.2
        else:
            change = 0.0

        new_price = max(MIN_PRICE, current_price + change)
        if new_price != current_price:
            driver_map[driver_id]["price"] = round(new_price, 1)
            changes[driver_id] = new_price

    # Save updated prices (async)
    if changes:
        data["drivers"] = [driver_map[d["id"]] for d in drivers if d["id"] in driver_map]

        def _write_prices():
            with open(PRICES_PATH, "w") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

        await asyncio.to_thread(_write_prices)

        # Clear cached prices
        import services.budget as budget_mod
        budget_mod._prices_cache = None

        logger.info("Updated %d driver prices after round %d", len(changes), race_round)

    return changes
