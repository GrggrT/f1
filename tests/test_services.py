"""Tests for budget, awards, and transfer-penalty services."""

import asyncio
import json

import pytest

import services.budget as budget_mod
from services.budget import (
    calculate_team_cost,
    get_all_constructors,
    get_all_drivers,
    get_constructor_name,
    get_constructor_price,
    get_driver_name,
    get_driver_price,
    load_prices,
    validate_team,
)
from services.awards import AwardsEngine
from services.transfers import TransferService
from data.database import Database


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _clear_budget_cache():
    """Reset the module-level prices cache so each test starts fresh."""
    budget_mod._prices_cache = None
    yield
    budget_mod._prices_cache = None


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def db(event_loop):
    async def _create():
        db = Database(":memory:")
        await db.connect()
        return db

    _db = event_loop.run_until_complete(_create())
    yield _db
    event_loop.run_until_complete(_db.close())


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Budget service tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestLoadPrices:
    def test_load_prices_returns_drivers_and_constructors(self):
        data = load_prices()
        assert "drivers" in data
        assert "constructors" in data
        assert isinstance(data["drivers"], list)
        assert isinstance(data["constructors"], list)

    def test_get_all_drivers_count(self):
        drivers = get_all_drivers()
        assert len(drivers) == 22

    def test_get_all_constructors_count(self):
        constructors = get_all_constructors()
        assert len(constructors) == 11


class TestDriverPricesPositive:
    def test_driver_prices_positive(self):
        drivers = get_all_drivers()
        for d in drivers:
            assert d.price > 0, f"Driver {d.id} has non-positive price: {d.price}"


class TestCalculateTeamCost:
    def test_calculate_team_cost(self):
        # Pick 5 cheap drivers + 1 cheap constructor and verify sum
        drivers = get_all_drivers()
        constructors = get_all_constructors()
        # Sort by price ascending, pick 5 cheapest drivers
        cheapest_drivers = sorted(drivers, key=lambda d: d.price)[:5]
        cheapest_constructor = sorted(constructors, key=lambda c: c.price)[0]

        driver_ids = [d.id for d in cheapest_drivers]
        expected_cost = sum(d.price for d in cheapest_drivers) + cheapest_constructor.price

        cost = calculate_team_cost(driver_ids, cheapest_constructor.id)
        assert cost == pytest.approx(expected_cost)


class TestValidateTeam:
    def test_validate_team_valid(self):
        """A valid team of 5 cheap drivers + cheap constructor within budget."""
        drivers = sorted(get_all_drivers(), key=lambda d: d.price)[:5]
        constructor = sorted(get_all_constructors(), key=lambda c: c.price)[0]
        driver_ids = [d.id for d in drivers]

        ok, msg = validate_team(driver_ids, constructor.id)
        assert ok, f"Expected valid team, got: {msg}"
        assert msg == "OK"

    def test_validate_team_over_budget(self):
        """All premium picks should exceed $100M budget."""
        ok, msg = validate_team(
            ["verstappen", "norris", "leclerc", "piastri", "hamilton"],
            "mclaren",
        )
        assert not ok
        assert "бюджет" in msg.lower() or "Превышен" in msg

    def test_validate_team_duplicate_drivers(self):
        """Duplicated driver IDs should fail validation."""
        ok, msg = validate_team(
            ["verstappen", "verstappen", "norris", "leclerc", "piastri"],
            "haas",
        )
        assert not ok
        assert "повторяться" in msg.lower() or "duplicate" in msg.lower()

    def test_validate_team_wrong_driver_count(self):
        ok, msg = validate_team(["verstappen", "norris"], "haas")
        assert not ok

    def test_validate_team_empty_constructor(self):
        ok, msg = validate_team(
            ["verstappen", "norris", "leclerc", "piastri", "hamilton"],
            "",
        )
        assert not ok


class TestGetDriverName:
    def test_get_driver_name_known(self):
        name = get_driver_name("verstappen")
        assert name  # non-empty
        assert isinstance(name, str)
        assert "Verstappen" in name

    def test_get_driver_name_unknown(self):
        # Should not crash; returns the id itself as fallback
        result = get_driver_name("unknown_driver_xyz")
        assert result is not None
        assert isinstance(result, str)


class TestGetConstructorName:
    def test_get_constructor_name_known(self):
        name = get_constructor_name("ferrari")
        assert name
        assert "Ferrari" in name

    def test_get_constructor_name_unknown(self):
        result = get_constructor_name("unknown_constructor")
        assert result is not None


class TestGetPrices:
    def test_get_driver_price_known(self):
        price = get_driver_price("verstappen")
        assert price > 0

    def test_get_driver_price_unknown(self):
        price = get_driver_price("nonexistent")
        assert price == 0.0

    def test_get_constructor_price_known(self):
        price = get_constructor_price("mclaren")
        assert price > 0

    def test_get_constructor_price_unknown(self):
        price = get_constructor_price("nonexistent")
        assert price == 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Awards engine tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestAwardsEngine:
    def test_no_scores_returns_empty(self, event_loop, db):
        engine = AwardsEngine(db)
        awards = event_loop.run_until_complete(
            engine.calculate_round_awards(race_round=1)
        )
        assert awards == []

    def test_awards_with_scores(self, event_loop, db):
        engine = AwardsEngine(db)

        async def _setup_and_run():
            # Register two users
            await db.register_user(1, "alice", "Alice")
            await db.register_user(2, "bob", "Bob")

            # Save scores with breakdowns for round 1
            breakdown1 = {
                "turbo_driver": "verstappen",
                "drivers": {
                    "verstappen": {"total": 30, "turbo_bonus": 15},
                    "norris": {"total": 20, "turbo_bonus": 0},
                },
            }
            breakdown2 = {
                "turbo_driver": "norris",
                "drivers": {
                    "norris": {"total": 25, "turbo_bonus": 10},
                    "leclerc": {"total": 15, "turbo_bonus": 0},
                },
            }
            await db.save_score(1, 1, 80.0, breakdown1)
            await db.save_score(2, 1, 45.0, breakdown2)

            return await engine.calculate_round_awards(race_round=1)

        awards = event_loop.run_until_complete(_setup_and_run())
        assert len(awards) > 0
        # Should at least have "Manager of the Round" and "Antimanager"
        titles = [a["title"] for a in awards]
        assert "Manager of the Round" in titles
        assert "Antimanager" in titles

    def test_awards_single_user(self, event_loop, db):
        """With only 1 user, only 'Manager of the Round' should appear (no Antimanager)."""
        engine = AwardsEngine(db)

        async def _setup_and_run():
            await db.register_user(1, "solo", "Solo")
            await db.save_score(1, 1, 60.0, {"turbo_driver": "", "drivers": {}})
            return await engine.calculate_round_awards(race_round=1)

        awards = event_loop.run_until_complete(_setup_and_run())
        titles = [a["title"] for a in awards]
        assert "Manager of the Round" in titles
        assert "Antimanager" not in titles

    def test_generate_roast(self, db):
        engine = AwardsEngine(db)
        roast = engine.generate_roast("alice", 42.0)
        assert "@alice" in roast
        assert "42" in roast


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Transfer penalty calculation tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestTransferPenalty:
    @pytest.fixture
    def service(self, db):
        return TransferService(db)

    def _setup_user(self, event_loop, db):
        event_loop.run_until_complete(db.register_user(100, "tester", "Tester"))

    def test_wildcard_removes_penalty(self, event_loop, db, service):
        """Activating wildcard should yield 0 penalty regardless of transfer count."""
        self._setup_user(event_loop, db)

        async def _run():
            # Activate wildcard
            await db.activate_chip(100, "WILDCARD", 1)
            # Perform 5 transfers (3 would normally be extra)
            for i in range(5):
                await db.log_transfer(100, 1, f"out{i}", f"in{i}", True)
            return await service.get_transfer_penalty(100, 1)

        penalty = event_loop.run_until_complete(_run())
        assert penalty == 0

    def test_transfer_penalty_calculation(self, event_loop, db, service):
        """3 total transfers = 1 extra (beyond 2 free) = 10 pts penalty."""
        self._setup_user(event_loop, db)

        async def _run():
            for i in range(3):
                await db.log_transfer(100, 1, f"out{i}", f"in{i}", True)
            return await service.get_transfer_penalty(100, 1)

        penalty = event_loop.run_until_complete(_run())
        assert penalty == 10

    def test_no_penalty_within_free_limit(self, event_loop, db, service):
        """2 transfers (exactly the free limit) = 0 penalty."""
        self._setup_user(event_loop, db)

        async def _run():
            await db.log_transfer(100, 1, "out0", "in0", True)
            await db.log_transfer(100, 1, "out1", "in1", True)
            return await service.get_transfer_penalty(100, 1)

        penalty = event_loop.run_until_complete(_run())
        assert penalty == 0

    def test_multiple_extra_penalties(self, event_loop, db, service):
        """6 transfers = 4 extra = 40 pts penalty."""
        self._setup_user(event_loop, db)

        async def _run():
            for i in range(6):
                await db.log_transfer(100, 1, f"out{i}", f"in{i}", True)
            return await service.get_transfer_penalty(100, 1)

        penalty = event_loop.run_until_complete(_run())
        assert penalty == 40
