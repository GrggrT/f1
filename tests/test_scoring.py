import pytest

from data.models import RaceResult, UserTeam
from services.scoring import (
    calculate_driver_race_score,
    calculate_driver_sprint_score,
    calculate_team_score,
)


def make_result(driver_id="verstappen", grid=1, finish=1, dnf=False, fastest_lap=False, round_num=1):
    return RaceResult(
        round=round_num,
        driver_id=driver_id,
        grid_position=grid,
        finish_position=finish,
        dnf=dnf,
        fastest_lap=fastest_lap,
    )


def make_team(drivers=None, constructor="red_bull", turbo="verstappen"):
    if drivers is None:
        drivers = ["verstappen", "norris", "colapinto", "hadjar", "bottas"]
    return UserTeam(
        user_id=1,
        username="test",
        race_round=1,
        drivers=drivers,
        constructor=constructor,
        turbo_driver=turbo,
        budget_remaining=10.0,
    )


class TestDriverRaceScore:
    def test_p1_all_bonuses(self):
        """P1 race + P1 quali + fastest lap + beat teammate, started P3."""
        result = make_result("verstappen", grid=3, finish=1, fastest_lap=True)
        teammate = make_result("lawson", grid=10, finish=8)

        score = calculate_driver_race_score(result, quali_position=1, teammate_result=teammate)

        assert score["race"] == 25        # P1
        assert score["qualifying"] == 10  # P1 quali
        assert score["position_gain"] == 4  # gained 2 positions * 2pts
        assert score["beat_teammate"] == 5
        assert score["fastest_lap"] == 10
        assert score["dnf"] == 0
        assert score["total"] == 54

    def test_midfield_hero(self):
        """Started P18, finished P8 = big position gain bonus."""
        result = make_result("colapinto", grid=18, finish=8)
        teammate = make_result("gasly", grid=12, finish=10)

        score = calculate_driver_race_score(result, quali_position=18, teammate_result=teammate)

        assert score["race"] == 4           # P8
        assert score["qualifying"] == 0     # P18 = no quali points
        assert score["position_gain"] == 20  # 10 positions * 2pts
        assert score["beat_teammate"] == 5
        assert score["total"] == 29

    def test_dnf_penalty(self):
        """DNF = -10 points, but quali points still count."""
        result = make_result("hamilton", grid=5, finish=None, dnf=True)

        score = calculate_driver_race_score(result, quali_position=5)

        assert score["dnf"] == -10
        assert score["qualifying"] == 6  # P5 quali
        assert score["race"] == 0
        assert score["position_gain"] == 0
        assert score["total"] == -4

    def test_dnf_no_quali(self):
        """DNF with no quali points."""
        result = make_result("bottas", grid=20, finish=None, dnf=True)

        score = calculate_driver_race_score(result, quali_position=20)

        assert score["dnf"] == -10
        assert score["qualifying"] == 0
        assert score["total"] == -10

    def test_position_loss_floors_at_zero(self):
        """Losing positions: penalty floors at 0."""
        result = make_result("norris", grid=2, finish=8)

        score = calculate_driver_race_score(result, quali_position=2)

        assert score["race"] == 4           # P8
        assert score["qualifying"] == 9     # P2 quali
        assert score["position_gain"] == 0  # lost 6 positions, floor at 0
        assert score["total"] == 13

    def test_fastest_lap_only(self):
        """Fastest lap bonus."""
        result = make_result("leclerc", grid=5, finish=5, fastest_lap=True)

        score = calculate_driver_race_score(result, quali_position=5)

        assert score["fastest_lap"] == 10
        assert score["race"] == 10  # P5
        assert score["qualifying"] == 6  # P5


class TestDriverSprintScore:
    def test_sprint_p1(self):
        score = calculate_driver_sprint_score(position=1, grid=3)
        assert score["sprint"] == 8
        assert score["sprint_position_gain"] == 4  # 2 positions * 2pts
        assert score["total"] == 12

    def test_sprint_dnf(self):
        score = calculate_driver_sprint_score(position=0, grid=5, dnf=True)
        assert score["total"] == -10

    def test_sprint_no_points(self):
        score = calculate_driver_sprint_score(position=12, grid=15)
        assert score["sprint"] == 0
        assert score["sprint_position_gain"] == 6  # 3 positions * 2


class TestTeamScore:
    def _make_race_results(self):
        return [
            make_result("verstappen", grid=3, finish=1, fastest_lap=True),
            make_result("lawson", grid=10, finish=8),
            make_result("norris", grid=1, finish=2),
            make_result("piastri", grid=4, finish=3),
            make_result("colapinto", grid=18, finish=8),
            make_result("gasly", grid=12, finish=10),
            make_result("hadjar", grid=15, finish=12),
            make_result("lindblad", grid=9, finish=7),
            make_result("bottas", grid=20, finish=None, dnf=True),
            make_result("ocon", grid=16, finish=14),
            make_result("bearman", grid=19, finish=16),
        ]

    def _make_quali_results(self):
        return [
            {"driver_id": "verstappen", "position": 3},
            {"driver_id": "lawson", "position": 10},
            {"driver_id": "norris", "position": 1},
            {"driver_id": "piastri", "position": 4},
            {"driver_id": "colapinto", "position": 18},
            {"driver_id": "gasly", "position": 12},
            {"driver_id": "hadjar", "position": 15},
            {"driver_id": "lindblad", "position": 9},
            {"driver_id": "bottas", "position": 20},
            {"driver_id": "ocon", "position": 16},
            {"driver_id": "bearman", "position": 19},
        ]

    def test_drs_boost_doubles(self):
        """Turbo driver should get 2x points."""
        team = make_team(turbo="verstappen")
        results = self._make_race_results()
        quali = self._make_quali_results()

        breakdown = calculate_team_score(team, results, quali, None, [])

        ver_score = breakdown["drivers"]["verstappen"]
        assert "turbo_bonus" in ver_score
        # VER base: race=25, quali=8(P3), pos_gain=4, beat_tm=5, fl=10 = 52
        # With 2x: total = 104, turbo_bonus = 52
        assert ver_score["turbo_bonus"] == ver_score["total"] // 2

    def test_triple_boost_chip(self):
        """TRIPLE_BOOST chip = 3x turbo driver."""
        team = make_team(turbo="verstappen")
        results = self._make_race_results()
        quali = self._make_quali_results()

        breakdown = calculate_team_score(
            team, results, quali, None, [],
            active_chip="TRIPLE_BOOST",
        )

        ver_score = breakdown["drivers"]["verstappen"]
        # 3x means turbo_bonus = 2 * base
        base = ver_score["total"] // 3
        assert ver_score["turbo_bonus"] == base * 2
        assert breakdown["turbo_multiplier"] == "3x"

    def test_no_negative_chip(self):
        """NO_NEGATIVE chip zeros out negative components."""
        team = make_team(
            drivers=["bottas", "norris", "colapinto", "hadjar", "verstappen"],
            turbo="norris",
        )
        results = self._make_race_results()
        quali = self._make_quali_results()

        # Without chip
        bd_normal = calculate_team_score(team, results, quali, None, [])
        mag_normal = bd_normal["drivers"]["bottas"]

        # With NO_NEGATIVE
        bd_chip = calculate_team_score(
            team, results, quali, None, [],
            active_chip="NO_NEGATIVE",
        )
        mag_chip = bd_chip["drivers"]["bottas"]

        # bottas DNF: normally -10, with NO_NEGATIVE chip should be 0
        assert mag_normal["dnf"] == -10
        assert mag_chip["dnf"] == 0
        assert mag_chip["total"] >= 0

    def test_transfer_penalty(self):
        """Transfer penalty deducted from total."""
        team = make_team()
        results = self._make_race_results()
        quali = self._make_quali_results()

        bd_no_penalty = calculate_team_score(team, results, quali, None, [])
        bd_with_penalty = calculate_team_score(
            team, results, quali, None, [],
            transfer_penalty=10,
        )

        assert bd_with_penalty["total"] == bd_no_penalty["total"] - 10
        assert bd_with_penalty["transfer_penalty"] == 10

    def test_total_includes_constructor(self):
        """Total should include constructor points."""
        team = make_team(constructor="red_bull")
        results = self._make_race_results()
        quali = self._make_quali_results()

        breakdown = calculate_team_score(team, results, quali, None, [])

        drivers_total = sum(
            d["total"] for d in breakdown["drivers"].values()
            if isinstance(d, dict) and "total" in d
        )
        constructor_total = breakdown["constructor"].get("total", 0)
        assert breakdown["total"] == drivers_total + constructor_total
