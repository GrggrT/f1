import asyncio

import pytest

from data.database import Database
from data.models import Prediction, Race, SurvivorPick, UserTeam


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
        return d
    return event_loop.run_until_complete(_create())


class TestUsers:
    def test_register_user(self, event_loop, db):
        ok = event_loop.run_until_complete(db.register_user(1, "alice", "Alice"))
        assert ok
        user = event_loop.run_until_complete(db.get_user(1))
        assert user["username"] == "alice"

    def test_duplicate_user(self, event_loop, db):
        event_loop.run_until_complete(db.register_user(1, "alice", "Alice"))
        ok = event_loop.run_until_complete(db.register_user(1, "alice", "Alice"))
        assert not ok

    def test_get_all_users(self, event_loop, db):
        event_loop.run_until_complete(db.register_user(1, "a", "A"))
        event_loop.run_until_complete(db.register_user(2, "b", "B"))
        users = event_loop.run_until_complete(db.get_all_users())
        assert len(users) == 2


class TestTeams:
    def test_save_and_get_team(self, event_loop, db):
        event_loop.run_until_complete(db.register_user(1, "alice", "Alice"))
        team = UserTeam(
            user_id=1, username="alice", race_round=1,
            drivers=["verstappen", "norris", "colapinto", "hadjar", "bottas"],
            constructor="red_bull", turbo_driver="verstappen",
            budget_remaining=10.0,
        )
        event_loop.run_until_complete(db.save_team(1, 1, team))
        got = event_loop.run_until_complete(db.get_team(1, 1))
        assert got is not None
        assert got.drivers == ["verstappen", "norris", "colapinto", "hadjar", "bottas"]
        assert got.turbo_driver == "verstappen"

    def test_upsert_team(self, event_loop, db):
        event_loop.run_until_complete(db.register_user(1, "alice", "Alice"))
        team1 = UserTeam(
            user_id=1, username="alice", race_round=1,
            drivers=["verstappen", "norris", "colapinto", "hadjar", "bottas"],
            constructor="red_bull", turbo_driver="verstappen",
            budget_remaining=10.0,
        )
        event_loop.run_until_complete(db.save_team(1, 1, team1))

        team2 = UserTeam(
            user_id=1, username="alice", race_round=1,
            drivers=["leclerc", "hamilton", "colapinto", "hadjar", "bottas"],
            constructor="ferrari", turbo_driver="leclerc",
            budget_remaining=5.0,
        )
        event_loop.run_until_complete(db.save_team(1, 1, team2))

        got = event_loop.run_until_complete(db.get_team(1, 1))
        assert got.constructor == "ferrari"
        assert "leclerc" in got.drivers

    def test_latest_team(self, event_loop, db):
        event_loop.run_until_complete(db.register_user(1, "alice", "Alice"))
        for r in [1, 2, 3]:
            team = UserTeam(
                user_id=1, username="alice", race_round=r,
                drivers=["verstappen", "norris", "colapinto", "hadjar", "bottas"],
                constructor="red_bull", turbo_driver="verstappen",
                budget_remaining=10.0,
            )
            event_loop.run_until_complete(db.save_team(1, r, team))

        latest = event_loop.run_until_complete(db.get_latest_team(1))
        assert latest.race_round == 3

    def test_all_teams_for_round(self, event_loop, db):
        event_loop.run_until_complete(db.register_user(1, "a", "A"))
        event_loop.run_until_complete(db.register_user(2, "b", "B"))
        for uid in [1, 2]:
            team = UserTeam(
                user_id=uid, username="x", race_round=1,
                drivers=["verstappen", "norris", "colapinto", "hadjar", "bottas"],
                constructor="red_bull", turbo_driver="verstappen",
                budget_remaining=10.0,
            )
            event_loop.run_until_complete(db.save_team(uid, 1, team))

        teams = event_loop.run_until_complete(db.get_all_teams_for_round(1))
        assert len(teams) == 2


class TestScores:
    def test_standings_ordering(self, event_loop, db):
        event_loop.run_until_complete(db.register_user(1, "low", "Low"))
        event_loop.run_until_complete(db.register_user(2, "high", "High"))

        event_loop.run_until_complete(db.save_score(1, 1, 50.0, {}))
        event_loop.run_until_complete(db.save_score(2, 1, 100.0, {}))
        event_loop.run_until_complete(db.save_score(1, 2, 60.0, {}))
        event_loop.run_until_complete(db.save_score(2, 2, 80.0, {}))

        standings = event_loop.run_until_complete(db.get_standings())
        # High: 180, Low: 110
        assert standings[0][1] == "high"
        assert standings[0][2] == 180.0
        assert standings[1][1] == "low"
        assert standings[1][2] == 110.0


class TestRaces:
    def test_save_and_get_race(self, event_loop, db):
        race = Race(
            round=1, name="Bahrain GP", country="Bahrain",
            circuit="Sakhir", qualifying_datetime="2026-03-14T15:00:00",
            race_datetime="2026-03-15T15:00:00",
        )
        event_loop.run_until_complete(db.save_race(race))
        got = event_loop.run_until_complete(db.get_race(1))
        assert got.name == "Bahrain GP"

    def test_next_race(self, event_loop, db):
        # Past race
        past = Race(
            round=1, name="Past GP", country="X", circuit="X",
            qualifying_datetime="2020-01-01T10:00:00",
            race_datetime="2020-01-02T10:00:00",
        )
        # Future race
        future = Race(
            round=2, name="Future GP", country="Y", circuit="Y",
            qualifying_datetime="2030-06-14T15:00:00",
            race_datetime="2030-06-15T15:00:00",
        )
        event_loop.run_until_complete(db.save_race(past))
        event_loop.run_until_complete(db.save_race(future))

        nxt = event_loop.run_until_complete(db.get_next_race())
        assert nxt.name == "Future GP"


class TestTransfersLog:
    def test_transfers_log(self, event_loop, db):
        event_loop.run_until_complete(db.register_user(1, "a", "A"))
        event_loop.run_until_complete(db.log_transfer(1, 1, "old", "new", True))
        event_loop.run_until_complete(db.log_transfer(1, 1, "old2", "new2", False))
        count = event_loop.run_until_complete(db.get_transfers_count(1, 1))
        assert count == 2


class TestChips:
    def test_chips_activation(self, event_loop, db):
        event_loop.run_until_complete(db.register_user(1, "a", "A"))

        ok = event_loop.run_until_complete(db.activate_chip(1, "WILDCARD", 1))
        assert ok

        # Duplicate chip type fails
        ok2 = event_loop.run_until_complete(db.activate_chip(1, "WILDCARD", 2))
        assert not ok2

        # Different chip type succeeds
        ok3 = event_loop.run_until_complete(db.activate_chip(1, "NO_NEGATIVE", 2))
        assert ok3

        active = event_loop.run_until_complete(db.get_active_chip(1, 1))
        assert active == "WILDCARD"

        used = event_loop.run_until_complete(db.get_used_chips(1))
        assert len(used) == 2
