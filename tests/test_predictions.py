import pytest

from data.models import Race, RaceResult
from services.predictions import PredictionService


@pytest.fixture
def service():
    return PredictionService()


@pytest.fixture
def sample_race():
    return Race(
        round=1, name="Bahrain GP", country="Bahrain",
        circuit="Sakhir", qualifying_datetime="2026-03-14T15:00:00",
        race_datetime="2026-03-15T15:00:00",
    )


@pytest.fixture
def sample_results():
    return [
        RaceResult(round=1, driver_id="verstappen", grid_position=1, finish_position=1, fastest_lap=True),
        RaceResult(round=1, driver_id="norris", grid_position=2, finish_position=2),
        RaceResult(round=1, driver_id="leclerc", grid_position=3, finish_position=3),
        RaceResult(round=1, driver_id="hamilton", grid_position=4, finish_position=4),
        RaceResult(round=1, driver_id="piastri", grid_position=5, finish_position=5),
        RaceResult(round=1, driver_id="russell", grid_position=6, finish_position=6),
        RaceResult(round=1, driver_id="sainz", grid_position=7, finish_position=7),
        RaceResult(round=1, driver_id="alonso", grid_position=8, finish_position=8),
        RaceResult(round=1, driver_id="gasly", grid_position=9, finish_position=9),
        RaceResult(round=1, driver_id="lindblad", grid_position=10, finish_position=10),
        RaceResult(round=1, driver_id="bottas", grid_position=18, finish_position=None, dnf=True),
        RaceResult(round=1, driver_id="stroll", grid_position=15, finish_position=12),
    ]


class TestPredictionService:
    def test_generate_questions(self, service, sample_race):
        questions = service.generate_questions(sample_race, 1)
        assert len(questions) == 7
        assert all("id" in q and "text" in q for q in questions)

    def test_questions_vary_by_round(self, service, sample_race):
        q1 = service.generate_questions(sample_race, 1)
        q2 = service.generate_questions(sample_race, 2)
        texts1 = {q["text"] for q in q1}
        texts2 = {q["text"] for q in q2}
        assert texts1 != texts2

    def test_questions_deterministic(self, service, sample_race):
        """Same round always produces same questions."""
        q1 = service.generate_questions(sample_race, 5)
        q2 = service.generate_questions(sample_race, 5)
        texts1 = [q["text"] for q in q1]
        texts2 = [q["text"] for q in q2]
        assert texts1 == texts2

    def test_questions_have_resolve_keys(self, service, sample_race):
        questions = service.generate_questions(sample_race, 1)
        for q in questions:
            assert "resolve_key" in q, f"Question missing resolve_key: {q}"

    def test_resolve_podium(self, service, sample_race, sample_results):
        """Test that podium resolution works correctly."""
        questions = [
            {"id": "1", "resolve_key": "podium_driver", "resolve_param": "verstappen"},
            {"id": "2", "resolve_key": "podium_driver", "resolve_param": "hamilton"},
        ]
        actuals = service.resolve_questions(questions, sample_results)
        assert actuals["1"] is True   # P1 = podium
        assert actuals["2"] is False  # P4 != podium

    def test_resolve_win(self, service, sample_results):
        questions = [
            {"id": "1", "resolve_key": "win_driver", "resolve_param": "verstappen"},
            {"id": "2", "resolve_key": "win_driver", "resolve_param": "norris"},
        ]
        actuals = service.resolve_questions(questions, sample_results)
        assert actuals["1"] is True
        assert actuals["2"] is False

    def test_resolve_dnf_count(self, service, sample_results):
        questions = [
            {"id": "1", "resolve_key": "dnf_count", "resolve_param": 1},
            {"id": "2", "resolve_key": "dnf_count", "resolve_param": 2},
        ]
        actuals = service.resolve_questions(questions, sample_results)
        assert actuals["1"] is True   # 1 DNF (bottas DNF)
        assert actuals["2"] is False  # not >= 2

    def test_resolve_dnf_exactly(self, service, sample_results):
        questions = [
            {"id": "1", "resolve_key": "dnf_exactly", "resolve_param": 1},
            {"id": "2", "resolve_key": "dnf_exactly", "resolve_param": 0},
        ]
        actuals = service.resolve_questions(questions, sample_results)
        assert actuals["1"] is True
        assert actuals["2"] is False

    def test_resolve_in_points(self, service, sample_results):
        questions = [
            {"id": "1", "resolve_key": "in_points", "resolve_param": "gasly"},
            {"id": "2", "resolve_key": "in_points", "resolve_param": "stroll"},
        ]
        actuals = service.resolve_questions(questions, sample_results)
        assert actuals["1"] is True   # P9
        assert actuals["2"] is False  # P12

    def test_resolve_head2head(self, service, sample_results):
        questions = [
            {"id": "1", "resolve_key": "head2head", "resolve_param": ("verstappen", "norris")},
            {"id": "2", "resolve_key": "head2head", "resolve_param": ("norris", "verstappen")},
        ]
        actuals = service.resolve_questions(questions, sample_results)
        assert actuals["1"] is True   # P1 > P2
        assert actuals["2"] is False

    def test_resolve_head2head_dnf(self, service, sample_results):
        questions = [
            {"id": "1", "resolve_key": "head2head", "resolve_param": ("verstappen", "bottas")},
            {"id": "2", "resolve_key": "head2head", "resolve_param": ("bottas", "verstappen")},
        ]
        actuals = service.resolve_questions(questions, sample_results)
        assert actuals["1"] is True   # bottas DNF'd
        assert actuals["2"] is False

    def test_resolve_pole_wins(self, service, sample_results):
        questions = [{"id": "1", "resolve_key": "pole_wins", "resolve_param": None}]
        actuals = service.resolve_questions(questions, sample_results)
        assert actuals["1"] is True  # verstappen P1 grid, P1 finish

    def test_resolve_constructor_both_points(self, service, sample_results):
        # mclaren: norris P2, piastri P5 — both in points
        questions = [
            {"id": "1", "resolve_key": "constructor_both_points", "resolve_param": "mclaren"},
            {"id": "2", "resolve_key": "constructor_both_points", "resolve_param": "cadillac"},
        ]
        actuals = service.resolve_questions(questions, sample_results)
        assert actuals["1"] is True
        assert actuals["2"] is False  # bottas DNF, perez not in results

    def test_resolve_constructor_1_2(self, service, sample_results):
        questions = [
            {"id": "1", "resolve_key": "constructor_1_2", "resolve_param": "red_bull"},
            {"id": "2", "resolve_key": "constructor_1_2", "resolve_param": "mclaren"},
        ]
        actuals = service.resolve_questions(questions, sample_results)
        # red_bull: verstappen P1 but lawson not in results
        assert actuals["1"] is False
        # mclaren: norris P2, piastri P5 -> not 1-2
        assert actuals["2"] is False

    def test_resolve_gain_positions(self, service, sample_results):
        questions = [
            {"id": "1", "resolve_key": "gain_positions", "resolve_param": ("stroll", 3)},
        ]
        actuals = service.resolve_questions(questions, sample_results)
        # stroll: grid 15, finish 12 -> gain 3
        assert actuals["1"] is True

    def test_resolve_p11_to_top5(self, service, sample_results):
        questions = [{"id": "1", "resolve_key": "p11_to_top5", "resolve_param": None}]
        actuals = service.resolve_questions(questions, sample_results)
        assert actuals["1"] is False  # nobody from P11+ finished top-5

    def test_resolve_fastest_lap_top3(self, service, sample_results):
        questions = [{"id": "1", "resolve_key": "fastest_lap_top3", "resolve_param": None}]
        actuals = service.resolve_questions(questions, sample_results)
        assert actuals["1"] is True  # verstappen FL and P1

    def test_resolve_fastest_lap_driver(self, service, sample_results):
        questions = [
            {"id": "1", "resolve_key": "fastest_lap_driver", "resolve_param": "verstappen"},
            {"id": "2", "resolve_key": "fastest_lap_driver", "resolve_param": "norris"},
        ]
        actuals = service.resolve_questions(questions, sample_results)
        assert actuals["1"] is True
        assert actuals["2"] is False

    def test_all_correct_max_confidence(self, service):
        user_answers = {
            "1": {"answer": True, "confidence": 5},
            "2": {"answer": False, "confidence": 5},
            "3": {"answer": True, "confidence": 5},
            "4": {"answer": False, "confidence": 5},
            "5": {"answer": True, "confidence": 5},
        }
        actual_results = {
            "1": True, "2": False, "3": True, "4": False, "5": True,
        }
        correct, total, bd = service.score_predictions(user_answers, actual_results)
        assert correct == 5
        assert total == 25

    def test_all_wrong(self, service):
        user_answers = {
            "1": {"answer": True, "confidence": 5},
            "2": {"answer": True, "confidence": 3},
        }
        actual_results = {"1": False, "2": False}
        correct, total, bd = service.score_predictions(user_answers, actual_results)
        assert correct == 0
        assert total == 0

    def test_mixed_results(self, service):
        user_answers = {
            "1": {"answer": True, "confidence": 5},   # correct
            "2": {"answer": True, "confidence": 3},   # wrong
            "3": {"answer": False, "confidence": 2},   # correct
        }
        actual_results = {"1": True, "2": False, "3": False}
        correct, total, bd = service.score_predictions(user_answers, actual_results)
        assert correct == 2
        assert total == 7  # 5 + 0 + 2
        assert bd["1"]["correct"] is True
        assert bd["2"]["correct"] is False
        assert bd["3"]["correct"] is True

    def test_track_specific_questions(self, service):
        """Street circuits should get street-specific questions."""
        monaco = Race(
            round=5, name="Monaco GP", country="Monaco",
            circuit="Monte Carlo", qualifying_datetime="2026-05-23T15:00:00",
            race_datetime="2026-05-24T15:00:00",
        )
        bahrain = Race(
            round=1, name="Bahrain GP", country="Bahrain",
            circuit="Sakhir", qualifying_datetime="2026-03-14T15:00:00",
            race_datetime="2026-03-15T15:00:00",
        )
        q_monaco = service.generate_questions(monaco, 5)
        q_bahrain = service.generate_questions(bahrain, 1)
        cats_monaco = {q.get("category", "") for q in q_monaco}
        cats_bahrain = {q.get("category", "") for q in q_bahrain}
        # Should have some different question categories
        assert cats_monaco != cats_bahrain or {q["text"] for q in q_monaco} != {q["text"] for q in q_bahrain}

    def test_all_rounds_produce_7_questions(self, service):
        """Every round should produce exactly 7 questions."""
        race = Race(
            round=1, name="Test GP", country="Unknown Land",
            circuit="Test", qualifying_datetime="2026-01-01T15:00:00",
            race_datetime="2026-01-02T15:00:00",
        )
        for r in range(1, 25):
            qs = service.generate_questions(race, r)
            assert len(qs) == 7, f"Round {r} produced {len(qs)} questions"
