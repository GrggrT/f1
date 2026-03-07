from __future__ import annotations

from data.database import Database
from data.models import RaceResult, SurvivorPick
from services.budget import get_all_drivers


class SurvivorService:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def get_available_drivers(self, user_id: int) -> tuple[list, list[str]]:
        """Returns (all_drivers, used_driver_ids)."""
        picks = await self.db.get_survivor_picks(user_id)
        used_ids = [p.driver_id for p in picks]
        all_drivers = get_all_drivers()
        return all_drivers, used_ids

    async def make_pick(
        self, user_id: int, race_round: int, driver_id: str
    ) -> tuple[bool, str]:
        """Make a survivor pick. Returns (success, message)."""
        picks = await self.db.get_survivor_picks(user_id)
        used_ids = [p.driver_id for p in picks]

        # Check if already picked for this round
        round_pick = next((p for p in picks if p.race_round == race_round), None)
        if round_pick:
            return False, f"Ты уже выбрал {round_pick.driver_id} на этот раунд"

        # Check driver not used before
        if driver_id in used_ids:
            return False, "Этот пилот уже использован в предыдущих раундах"

        # Validate driver exists
        all_drivers = get_all_drivers()
        if not any(d.id == driver_id for d in all_drivers):
            return False, "Неизвестный пилот"

        # Check zombie status
        eliminated = any(p.survived is False for p in picks)

        pick = SurvivorPick(
            user_id=user_id,
            race_round=race_round,
            driver_id=driver_id,
        )
        await self.db.save_survivor_pick(pick)

        if eliminated:
            return True, "ZOMBIE_PICK"
        return True, "OK"

    async def evaluate_picks(
        self, race_round: int, race_results: list[RaceResult]
    ) -> list[dict]:
        """Evaluate all survivor picks for a round. Returns status updates."""
        results_map = {r.driver_id: r for r in race_results}
        users = await self.db.get_all_users()
        updates = []

        for user in users:
            user_id = user["telegram_id"]
            picks = await self.db.get_survivor_picks(user_id)
            round_pick = next(
                (p for p in picks if p.race_round == race_round), None
            )

            if not round_pick:
                continue

            result = results_map.get(round_pick.driver_id)
            survived = (
                result is not None
                and result.finish_position is not None
                and result.finish_position <= 10
                and not result.dnf
            )

            await self.db.update_survivor_result(user_id, race_round, survived)

            is_eliminated = await self.is_eliminated(user_id)
            was_already_eliminated = await self._was_eliminated_before(
                user_id, race_round
            )

            updates.append({
                "user_id": user_id,
                "username": user.get("username", ""),
                "driver_id": round_pick.driver_id,
                "survived": survived,
                "finish_position": result.finish_position if result else None,
                "is_zombie": was_already_eliminated,
                "newly_eliminated": not survived and not was_already_eliminated,
            })

        return updates

    async def is_eliminated(self, user_id: int) -> bool:
        """Check if user has any failed pick."""
        picks = await self.db.get_survivor_picks(user_id)
        return any(p.survived is False for p in picks)

    async def _was_eliminated_before(self, user_id: int, current_round: int) -> bool:
        """Check if user was eliminated before this round."""
        picks = await self.db.get_survivor_picks(user_id)
        return any(
            p.survived is False and p.race_round < current_round
            for p in picks
        )

    async def get_survivor_standings(self) -> list[dict]:
        """Get survivor standings for all users."""
        users = await self.db.get_all_users()
        standings = []

        for user in users:
            user_id = user["telegram_id"]
            picks = await self.db.get_survivor_picks(user_id)
            if not picks:
                continue

            survived_count = sum(1 for p in picks if p.survived is True)
            eliminated = any(p.survived is False for p in picks)
            elim_round = None
            if eliminated:
                failed = [p for p in picks if p.survived is False]
                elim_round = min(p.race_round for p in failed)

            standings.append({
                "user_id": user_id,
                "username": user.get("username", ""),
                "survived_count": survived_count,
                "total_picks": len(picks),
                "eliminated": eliminated,
                "eliminated_round": elim_round,
                "status": "zombie" if eliminated else "alive",
            })

        # Sort: alive first (by survived_count desc), then zombies
        standings.sort(
            key=lambda x: (x["eliminated"], -x["survived_count"])
        )
        return standings
