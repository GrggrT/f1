import asyncio

import pytest

from data.database import Database
from data.models import UserTeam
from services.transfers import TransferService


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def db(event_loop):
    async def _create():
        d = Database(":memory:")
        await d.connect()
        await d.register_user(1, "user1", "User One")
        team = UserTeam(
            user_id=1, username="user1", race_round=1,
            drivers=["verstappen", "norris", "colapinto", "hadjar", "bottas"],
            constructor="red_bull", turbo_driver="verstappen",
            budget_remaining=10.0,
        )
        await d.save_team(1, 1, team)
        return d
    return event_loop.run_until_complete(_create())


@pytest.fixture
def service(db):
    return TransferService(db)


class TestTransfers:
    def test_free_transfers(self, event_loop, service):
        ok, left, msg = event_loop.run_until_complete(service.can_transfer(1, 1))
        assert ok
        assert left == 2

        event_loop.run_until_complete(service.execute_transfer(1, 1, "bottas", "albon"))
        ok, left, msg = event_loop.run_until_complete(service.can_transfer(1, 1))
        assert ok
        assert left == 1

        event_loop.run_until_complete(service.execute_transfer(1, 1, "hadjar", "lindblad"))
        ok, left, msg = event_loop.run_until_complete(service.can_transfer(1, 1))
        assert ok
        assert left == 0

    def test_third_transfer_penalty(self, event_loop, service):
        event_loop.run_until_complete(service.execute_transfer(1, 1, "a", "b"))
        event_loop.run_until_complete(service.execute_transfer(1, 1, "c", "d"))
        event_loop.run_until_complete(service.execute_transfer(1, 1, "e", "f"))

        penalty = event_loop.run_until_complete(service.get_transfer_penalty(1, 1))
        assert penalty == 10

    def test_multiple_extra_transfers(self, event_loop, service):
        for i in range(5):
            event_loop.run_until_complete(service.execute_transfer(1, 1, f"out{i}", f"in{i}"))

        penalty = event_loop.run_until_complete(service.get_transfer_penalty(1, 1))
        assert penalty == 30  # 3 extra * 10

    def test_wildcard_unlimited(self, event_loop, service):
        ok, msg = event_loop.run_until_complete(service.activate_chip(1, 1, "WILDCARD"))
        assert ok

        for i in range(10):
            event_loop.run_until_complete(service.execute_transfer(1, 1, f"out{i}", f"in{i}"))

        penalty = event_loop.run_until_complete(service.get_transfer_penalty(1, 1))
        assert penalty == 0

        ok, left, msg = event_loop.run_until_complete(service.can_transfer(1, 1))
        assert left == 999

    def test_transfers_per_round_independent(self, event_loop, service):
        event_loop.run_until_complete(service.execute_transfer(1, 1, "a", "b"))
        event_loop.run_until_complete(service.execute_transfer(1, 1, "c", "d"))

        # Round 2 should have fresh transfers
        ok, left, msg = event_loop.run_until_complete(service.can_transfer(1, 2))
        assert left == 2

    def test_budget_validation(self, event_loop):
        from services.budget import validate_team
        # All top drivers = over budget
        ok, msg = validate_team(
            ["verstappen", "norris", "leclerc", "hamilton", "piastri"],
            "red_bull",
        )
        assert not ok
        assert "бюджет" in msg.lower() or "budget" in msg.lower() or "Превышен" in msg


class TestChips:
    def test_available_chips(self, event_loop, service):
        chips = event_loop.run_until_complete(service.get_available_chips(1))
        assert chips == ["WILDCARD", "TRIPLE_BOOST", "NO_NEGATIVE"]

    def test_activate_chip(self, event_loop, service):
        ok, msg = event_loop.run_until_complete(
            service.activate_chip(1, 1, "WILDCARD")
        )
        assert ok
        chips = event_loop.run_until_complete(service.get_available_chips(1))
        assert "WILDCARD" not in chips
        assert len(chips) == 2

    def test_no_double_chip_per_round(self, event_loop, service):
        event_loop.run_until_complete(service.activate_chip(1, 1, "WILDCARD"))
        ok, msg = event_loop.run_until_complete(
            service.activate_chip(1, 1, "NO_NEGATIVE")
        )
        assert not ok

    def test_no_reuse_chip(self, event_loop, service):
        event_loop.run_until_complete(service.activate_chip(1, 1, "WILDCARD"))
        ok, msg = event_loop.run_until_complete(
            service.activate_chip(1, 3, "WILDCARD")
        )
        assert not ok

    def test_has_active_chip(self, event_loop, service):
        event_loop.run_until_complete(service.activate_chip(1, 1, "TRIPLE_BOOST"))
        has = event_loop.run_until_complete(
            service.has_active_chip(1, 1, "TRIPLE_BOOST")
        )
        assert has
        has_other = event_loop.run_until_complete(
            service.has_active_chip(1, 1, "WILDCARD")
        )
        assert not has_other
