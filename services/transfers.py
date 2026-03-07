from __future__ import annotations

from data.database import Database

ALL_CHIPS = ["WILDCARD", "TRIPLE_BOOST", "NO_NEGATIVE"]
FREE_TRANSFERS_PER_RACE = 2
EXTRA_TRANSFER_PENALTY = -10


class TransferService:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def can_transfer(
        self, user_id: int, race_round: int
    ) -> tuple[bool, int, str]:
        """Returns (allowed, free_transfers_left, reason)."""
        # Check if wildcard is active
        active_chip = await self.db.get_active_chip(user_id, race_round)
        if active_chip == "WILDCARD":
            return True, 999, "Wildcard active"

        count = await self.db.get_transfers_count(user_id, race_round)
        free_left = max(0, FREE_TRANSFERS_PER_RACE - count)

        if free_left > 0:
            return True, free_left, "OK"
        else:
            return True, 0, f"Extra transfer: {EXTRA_TRANSFER_PENALTY} pts penalty"

    async def execute_transfer(
        self, user_id: int, race_round: int, driver_out: str, driver_in: str
    ) -> tuple[bool, int]:
        """Execute transfer. Returns (success, penalty_points)."""
        # Defense-in-depth deadline check
        from datetime import datetime, timezone
        race = await self.db.get_race(race_round)
        if race:
            deadline = datetime.fromisoformat(race.qualifying_datetime)
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            if now > deadline:
                return False, 0

        active_chip = await self.db.get_active_chip(user_id, race_round)
        count = await self.db.get_transfers_count(user_id, race_round)

        is_free = count < FREE_TRANSFERS_PER_RACE or active_chip == "WILDCARD"
        penalty = 0 if is_free else abs(EXTRA_TRANSFER_PENALTY)

        await self.db.log_transfer(user_id, race_round, driver_out, driver_in, is_free)
        return True, penalty

    async def get_transfer_penalty(self, user_id: int, race_round: int) -> int:
        """Calculate total transfer penalty for a round."""
        active_chip = await self.db.get_active_chip(user_id, race_round)
        if active_chip == "WILDCARD":
            return 0

        count = await self.db.get_transfers_count(user_id, race_round)
        extra = max(0, count - FREE_TRANSFERS_PER_RACE)
        return extra * abs(EXTRA_TRANSFER_PENALTY)

    async def get_available_chips(self, user_id: int) -> list[str]:
        used = await self.db.get_used_chips(user_id)
        used_types = {c["chip_type"] for c in used}
        return [c for c in ALL_CHIPS if c not in used_types]

    async def activate_chip(
        self, user_id: int, race_round: int, chip_type: str
    ) -> tuple[bool, str]:
        if chip_type not in ALL_CHIPS:
            return False, "Unknown chip type"

        # Check deadline
        from datetime import datetime, timezone
        race = await self.db.get_race(race_round)
        if race:
            deadline = datetime.fromisoformat(race.qualifying_datetime)
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            if now > deadline:
                return False, "Deadline passed - chips cannot be activated"

        # Check not already used
        used = await self.db.get_used_chips(user_id)
        used_types = {c["chip_type"] for c in used}
        if chip_type in used_types:
            return False, "Этот чип уже использован в этом сезоне"

        # Check no other chip active this round
        active = await self.db.get_active_chip(user_id, race_round)
        if active:
            return False, f"На этот раунд уже активирован чип: {active}"

        ok = await self.db.activate_chip(user_id, chip_type, race_round)
        if not ok:
            return False, "Ошибка активации чипа"

        return True, "OK"

    async def has_active_chip(
        self, user_id: int, race_round: int, chip_type: str
    ) -> bool:
        active = await self.db.get_active_chip(user_id, race_round)
        return active == chip_type
