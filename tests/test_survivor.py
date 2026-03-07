import asyncio

import pytest

from data.database import Database
from data.models import RaceResult
from services.survivor_logic import SurvivorService


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
        await d.register_user(2, "user2", "User Two")
        return d
    return event_loop.run_until_complete(_create())


@pytest.fixture
def service(db):
    return SurvivorService(db)


@pytest.fixture
def sample_results():
    return [
        RaceResult(round=1, driver_id="verstappen", grid_position=1, finish_position=1),
        RaceResult(round=1, driver_id="norris", grid_position=2, finish_position=3),
        RaceResult(round=1, driver_id="albon", grid_position=12, finish_position=14),
        RaceResult(round=1, driver_id="bottas", grid_position=18, finish_position=None, dnf=True),
    ]


class TestSurvivorService:
    def test_make_pick(self, event_loop, service):
        ok, msg = event_loop.run_until_complete(
            service.make_pick(1, 1, "verstappen")
        )
        assert ok

    def test_pick_unique_drivers(self, event_loop, service):
        event_loop.run_until_complete(service.make_pick(1, 1, "verstappen"))
        ok, msg = event_loop.run_until_complete(
            service.make_pick(1, 2, "verstappen")
        )
        assert not ok
        assert "уже использован" in msg

    def test_pick_once_per_round(self, event_loop, service):
        event_loop.run_until_complete(service.make_pick(1, 1, "verstappen"))
        ok, msg = event_loop.run_until_complete(
            service.make_pick(1, 1, "norris")
        )
        assert not ok
        assert "уже выбрал" in msg

    def test_survived(self, event_loop, service, sample_results):
        event_loop.run_until_complete(service.make_pick(1, 1, "verstappen"))
        updates = event_loop.run_until_complete(
            service.evaluate_picks(1, sample_results)
        )
        assert len(updates) == 1
        assert updates[0]["survived"] is True

    def test_eliminated(self, event_loop, service, sample_results):
        event_loop.run_until_complete(service.make_pick(1, 1, "albon"))
        updates = event_loop.run_until_complete(
            service.evaluate_picks(1, sample_results)
        )
        assert updates[0]["survived"] is False
        is_elim = event_loop.run_until_complete(service.is_eliminated(1))
        assert is_elim

    def test_dnf_eliminated(self, event_loop, service, sample_results):
        event_loop.run_until_complete(service.make_pick(1, 1, "bottas"))
        updates = event_loop.run_until_complete(
            service.evaluate_picks(1, sample_results)
        )
        assert updates[0]["survived"] is False

    def test_zombie_continues(self, event_loop, service, db):
        # Round 1: user1 picks albon (P14 = fail)
        event_loop.run_until_complete(service.make_pick(1, 1, "albon"))
        results_r1 = [
            RaceResult(round=1, driver_id="albon", grid_position=12, finish_position=14),
        ]
        event_loop.run_until_complete(service.evaluate_picks(1, results_r1))

        is_elim = event_loop.run_until_complete(service.is_eliminated(1))
        assert is_elim

        # Round 2: zombie can still pick
        ok, msg = event_loop.run_until_complete(
            service.make_pick(1, 2, "verstappen")
        )
        assert ok

    def test_standings(self, event_loop, service, sample_results):
        event_loop.run_until_complete(service.make_pick(1, 1, "verstappen"))
        event_loop.run_until_complete(service.make_pick(2, 1, "albon"))
        event_loop.run_until_complete(service.evaluate_picks(1, sample_results))

        standings = event_loop.run_until_complete(
            service.get_survivor_standings()
        )
        assert len(standings) == 2
        # User1 (alive) should be first
        assert standings[0]["status"] == "alive"
        assert standings[1]["status"] == "zombie"
