"""Tests for recently added functionality.

Covers: CircuitBreaker, Database.transaction(), Database.cancel_race(),
Database.get_race_scores_with_users(), zombie picks in SurvivorService,
pit_stop predictions, and callback validation helpers.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from data.database import Database
from data.models import Race, RaceResult
from services.predictions import PredictionService
from services.survivor_logic import SurvivorService


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


# ---------------------------------------------------------------------------
# 1. CircuitBreaker
# ---------------------------------------------------------------------------


class TestCircuitBreaker:
    def test_starts_closed(self):
        from data.api_client import CircuitBreaker

        cb = CircuitBreaker(failure_threshold=3, reset_timeout=60)
        assert not cb.is_open

    def test_opens_after_threshold(self):
        from data.api_client import CircuitBreaker

        cb = CircuitBreaker(failure_threshold=3, reset_timeout=60)
        cb.record_failure()
        cb.record_failure()
        assert not cb.is_open
        cb.record_failure()
        assert cb.is_open

    def test_success_resets(self):
        from data.api_client import CircuitBreaker

        cb = CircuitBreaker(failure_threshold=3, reset_timeout=60)
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        cb.record_failure()
        assert not cb.is_open

    def test_resets_after_timeout(self):
        from data.api_client import CircuitBreaker

        cb = CircuitBreaker(failure_threshold=2, reset_timeout=1)
        cb.record_failure()
        cb.record_failure()
        assert cb.is_open
        time.sleep(1.1)
        assert not cb.is_open


# ---------------------------------------------------------------------------
# 2. Database transaction rollback
# ---------------------------------------------------------------------------


class TestTransaction:
    @pytest.fixture
    def db(self, event_loop):
        async def _create():
            db = Database(":memory:")
            await db.connect()
            return db

        _db = event_loop.run_until_complete(_create())
        yield _db
        event_loop.run_until_complete(_db.close())

    def test_transaction_commits(self, event_loop, db):
        async def _test():
            async with db.transaction():
                await db.db.execute(
                    "INSERT INTO users (telegram_id, username, display_name) VALUES (?, ?, ?)",
                    (999, "txtest", "Tx Test"),
                )
            user = await db.get_user(999)
            assert user is not None
            assert user["username"] == "txtest"

        event_loop.run_until_complete(_test())

    def test_transaction_rollback(self, event_loop, db):
        async def _test():
            try:
                async with db.transaction():
                    await db.db.execute(
                        "INSERT INTO users (telegram_id, username, display_name) VALUES (?, ?, ?)",
                        (888, "rollback", "Rollback Test"),
                    )
                    raise ValueError("Simulated error")
            except ValueError:
                pass
            user = await db.get_user(888)
            assert user is None

        event_loop.run_until_complete(_test())


# ---------------------------------------------------------------------------
# 3. Database cancel_race
# ---------------------------------------------------------------------------


class TestCancelRace:
    @pytest.fixture
    def db(self, event_loop):
        async def _create():
            db = Database(":memory:")
            await db.connect()
            return db

        _db = event_loop.run_until_complete(_create())
        yield _db
        event_loop.run_until_complete(_db.close())

    def test_cancel_race_removes_all(self, event_loop, db):
        async def _test():
            race = Race(
                round=5,
                name="Test GP",
                country="Test",
                circuit="Test Circuit",
                qualifying_datetime="2026-06-01T14:00:00",
                race_datetime="2026-06-02T14:00:00",
            )
            await db.save_race(race)

            await db.db.execute(
                "INSERT INTO users (telegram_id, username, display_name) VALUES (1, 'u1', 'User 1')"
            )
            await db.db.execute(
                "INSERT INTO scores (user_id, race_round, fantasy_points, breakdown) VALUES (1, 5, 100.0, '{}')"
            )
            await db.db.commit()

            await db.cancel_race(5)

            race = await db.get_race(5)
            assert race is None

            scores = await db.get_race_scores(5)
            assert len(scores) == 0

        event_loop.run_until_complete(_test())


# ---------------------------------------------------------------------------
# 4. Zombie pick in survivor
# ---------------------------------------------------------------------------


class TestZombiePick:
    @pytest.fixture
    def db(self, event_loop):
        async def _create():
            db = Database(":memory:")
            await db.connect()
            await db.db.execute(
                "INSERT INTO users (telegram_id, username, display_name) VALUES (1, 'zombie', 'Zombie')"
            )
            await db.db.commit()
            return db

        _db = event_loop.run_until_complete(_create())
        yield _db
        event_loop.run_until_complete(_db.close())

    def test_zombie_pick_returns_zombie_message(self, event_loop, db):
        async def _test():
            svc = SurvivorService(db)

            ok, msg = await svc.make_pick(1, 1, "verstappen")
            assert ok and msg == "OK"

            await db.update_survivor_result(1, 1, False)

            ok, msg = await svc.make_pick(1, 2, "norris")
            assert ok
            assert msg == "ZOMBIE_PICK"

        event_loop.run_until_complete(_test())


# ---------------------------------------------------------------------------
# 5. Pit stop prediction resolution
# ---------------------------------------------------------------------------


class TestPitStopPrediction:
    def test_pit_under_threshold_with_data(self):
        svc = PredictionService()
        results = [
            RaceResult(
                round=1,
                driver_id="verstappen",
                grid_position=1,
                finish_position=1,
            )
        ]
        pit_stops = [{"driver_id": "verstappen", "duration": "1.8"}]
        questions = [
            {
                "id": "1",
                "resolve_key": "pit_under_threshold",
                "resolve_param": 2.0,
                "text": "test",
            }
        ]
        actuals = svc.resolve_questions(questions, results, pit_stops=pit_stops)
        assert actuals["1"] is True

    def test_pit_under_threshold_no_data(self):
        svc = PredictionService()
        results = [
            RaceResult(
                round=1,
                driver_id="verstappen",
                grid_position=1,
                finish_position=1,
            )
        ]
        questions = [
            {
                "id": "1",
                "resolve_key": "pit_under_threshold",
                "resolve_param": 2.0,
                "text": "test",
            }
        ]
        actuals = svc.resolve_questions(questions, results)
        assert actuals["1"] is False

    def test_pit_above_threshold(self):
        svc = PredictionService()
        results = [
            RaceResult(
                round=1,
                driver_id="verstappen",
                grid_position=1,
                finish_position=1,
            )
        ]
        pit_stops = [{"driver_id": "verstappen", "duration": "2.5"}]
        questions = [
            {
                "id": "1",
                "resolve_key": "pit_under_threshold",
                "resolve_param": 2.0,
                "text": "test",
            }
        ]
        actuals = svc.resolve_questions(questions, results, pit_stops=pit_stops)
        assert actuals["1"] is False


# ---------------------------------------------------------------------------
# 6. Callback validation (driver / constructor IDs in prices data)
# ---------------------------------------------------------------------------


class TestCallbackValidation:
    def test_unknown_driver_id(self):
        """Verify that load_prices has valid driver IDs for validation."""
        from services.budget import load_prices

        data = load_prices()
        ids = {d["id"] for d in data["drivers"]}
        assert "verstappen" in ids
        assert "nonexistent_driver" not in ids

    def test_unknown_constructor_id(self):
        from services.budget import load_prices

        data = load_prices()
        ids = {c["id"] for c in data["constructors"]}
        assert "red_bull" in ids
        assert "nonexistent_team" not in ids


# ---------------------------------------------------------------------------
# 7. get_race_scores_with_users
# ---------------------------------------------------------------------------


class TestRaceScoresWithUsers:
    @pytest.fixture
    def db(self, event_loop):
        async def _create():
            db = Database(":memory:")
            await db.connect()
            return db

        _db = event_loop.run_until_complete(_create())
        yield _db
        event_loop.run_until_complete(_db.close())

    def test_scores_include_username(self, event_loop, db):
        async def _test():
            await db.register_user(1, "testuser", "Test User")
            await db.save_score(1, 1, 150.0, {"total": 150})
            results = await db.get_race_scores_with_users(1)
            assert len(results) == 1
            assert results[0]["username"] == "testuser"
            assert results[0]["fantasy_points"] == 150.0

        event_loop.run_until_complete(_test())
