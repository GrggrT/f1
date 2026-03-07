from __future__ import annotations

import hashlib
from typing import Any

from data.models import Race, RaceResult


# ── Driver tiers ──

TOP_DRIVERS = ["verstappen", "norris", "leclerc", "piastri", "hamilton", "russell"]
MIDFIELD_DRIVERS = ["antonelli", "sainz", "alonso", "albon", "gasly", "lawson"]
BACKFIELD_DRIVERS = ["hadjar", "stroll", "ocon", "hulkenberg", "perez", "bortoleto", "bearman", "bottas", "colapinto", "lindblad"]

CONSTRUCTOR_NAMES = {
    "red_bull": "Red Bull", "mclaren": "McLaren", "ferrari": "Ferrari",
    "mercedes": "Mercedes", "aston_martin": "Aston Martin", "alpine": "Alpine",
    "williams": "Williams", "racing_bulls": "Racing Bulls", "haas": "Haas",
    "audi": "Audi", "cadillac": "Cadillac",
}

TEAMMATE_PAIRS = [
    ("verstappen", "hadjar"),
    ("norris", "piastri"),
    ("leclerc", "hamilton"),
    ("russell", "antonelli"),
    ("albon", "sainz"),
    ("alonso", "stroll"),
    ("gasly", "colapinto"),
    ("lawson", "lindblad"),
    ("ocon", "bearman"),
    ("hulkenberg", "bortoleto"),
    ("perez", "bottas"),
]

CROSS_TEAM_RIVALRIES = [
    ("verstappen", "norris"),
    ("leclerc", "norris"),
    ("hamilton", "russell"),
    ("piastri", "leclerc"),
    ("sainz", "gasly"),
    ("alonso", "hulkenberg"),
    ("antonelli", "bearman"),
    ("hadjar", "lawson"),
    ("perez", "albon"),
    ("colapinto", "lindblad"),
]

# ── Track characteristics ──

TRACK_PROFILES: dict[str, dict[str, Any]] = {
    # Street circuits — high DNF, few overtakes
    "Monaco": {"type": "street", "overtake_difficulty": "extreme", "dnf_prone": True, "pit_stops_avg": 1},
    "Singapore": {"type": "street", "overtake_difficulty": "hard", "dnf_prone": True, "pit_stops_avg": 1},
    "Azerbaijan": {"type": "street", "overtake_difficulty": "medium", "dnf_prone": True, "pit_stops_avg": 1},
    "Las Vegas": {"type": "street", "overtake_difficulty": "medium", "dnf_prone": True, "pit_stops_avg": 1},
    # High-speed — lots of overtakes
    "Italy": {"type": "power", "overtake_difficulty": "easy", "dnf_prone": False, "pit_stops_avg": 1},
    "Belgium": {"type": "power", "overtake_difficulty": "easy", "dnf_prone": False, "pit_stops_avg": 1},
    "Mexico": {"type": "power", "overtake_difficulty": "easy", "dnf_prone": False, "pit_stops_avg": 2},
    # Classic tracks
    "Bahrain": {"type": "balanced", "overtake_difficulty": "medium", "dnf_prone": False, "pit_stops_avg": 2},
    "Spain": {"type": "balanced", "overtake_difficulty": "medium", "dnf_prone": False, "pit_stops_avg": 2},
    "Hungary": {"type": "downforce", "overtake_difficulty": "hard", "dnf_prone": False, "pit_stops_avg": 2},
    "Austria": {"type": "power", "overtake_difficulty": "easy", "dnf_prone": False, "pit_stops_avg": 1},
    "UK": {"type": "balanced", "overtake_difficulty": "medium", "dnf_prone": False, "pit_stops_avg": 2},
    "Japan": {"type": "downforce", "overtake_difficulty": "medium", "dnf_prone": False, "pit_stops_avg": 2},
    "Australia": {"type": "street_fast", "overtake_difficulty": "hard", "dnf_prone": True, "pit_stops_avg": 2},
    "Canada": {"type": "stop_go", "overtake_difficulty": "medium", "dnf_prone": True, "pit_stops_avg": 2},
    "Netherlands": {"type": "downforce", "overtake_difficulty": "hard", "dnf_prone": False, "pit_stops_avg": 1},
    "China": {"type": "balanced", "overtake_difficulty": "medium", "dnf_prone": False, "pit_stops_avg": 2},
    "USA": {"type": "balanced", "overtake_difficulty": "medium", "dnf_prone": False, "pit_stops_avg": 2},
    "United States": {"type": "balanced", "overtake_difficulty": "medium", "dnf_prone": False, "pit_stops_avg": 2},
    "Brazil": {"type": "balanced", "overtake_difficulty": "easy", "dnf_prone": True, "pit_stops_avg": 2},
    "Qatar": {"type": "balanced", "overtake_difficulty": "medium", "dnf_prone": False, "pit_stops_avg": 2},
    "Abu Dhabi": {"type": "balanced", "overtake_difficulty": "medium", "dnf_prone": False, "pit_stops_avg": 2},
    "Saudi Arabia": {"type": "street_fast", "overtake_difficulty": "medium", "dnf_prone": True, "pit_stops_avg": 2},
    "Miami": {"type": "street_fast", "overtake_difficulty": "medium", "dnf_prone": True, "pit_stops_avg": 2},
    "Portugal": {"type": "balanced", "overtake_difficulty": "medium", "dnf_prone": False, "pit_stops_avg": 2},
}

DEFAULT_PROFILE = {"type": "balanced", "overtake_difficulty": "medium", "dnf_prone": False, "pit_stops_avg": 2}


def _get_track_profile(country: str) -> dict:
    return TRACK_PROFILES.get(country, DEFAULT_PROFILE)


# ── Question pool ──
# Each template has:
#   - id: unique identifier
#   - build: function(ctx) -> question text
#   - resolve_key: key for resolve_questions
#   - resolve_param_fn: function(ctx) -> param for resolution
#   - tags: when this question is more relevant (track types, etc.)

def _seeded_pick(items: list, seed: int, offset: int = 0) -> Any:
    """Pick an item deterministically from a list."""
    return items[(seed + offset) % len(items)]


def _seeded_pick_pair(items: list, seed: int) -> tuple:
    """Pick two different items from a list."""
    a = items[seed % len(items)]
    b = items[(seed + 1) % len(items)]
    if a == b:
        b = items[(seed + 2) % len(items)]
    return a, b


class _Ctx:
    """Context passed to question builders."""
    def __init__(self, race: Race, race_round: int, seed: int, profile: dict):
        self.race = race
        self.race_round = race_round
        self.seed = seed
        self.profile = profile

    def driver_name(self, driver_id: str) -> str:
        from services.budget import get_driver_name
        return get_driver_name(driver_id)

    def constructor_name(self, cons_id: str) -> str:
        return CONSTRUCTOR_NAMES.get(cons_id, cons_id)


QUESTION_POOL: list[dict] = []


def _q(qid: str, tags: list[str] | None = None):
    """Decorator to register a question builder."""
    def decorator(fn):
        QUESTION_POOL.append({"id": qid, "builder": fn, "tags": tags or []})
        return fn
    return decorator


# ── Podium / Win questions ──

@_q("podium_top", tags=["always"])
def _q_podium_top(ctx: _Ctx) -> dict | None:
    driver = _seeded_pick(TOP_DRIVERS, ctx.seed, 0)
    return {
        "text": f"{ctx.driver_name(driver)} финиширует на подиуме (P1-P3)?",
        "resolve_key": "podium_driver",
        "resolve_param": driver,
    }


@_q("win_top", tags=["always"])
def _q_win_top(ctx: _Ctx) -> dict | None:
    driver = _seeded_pick(TOP_DRIVERS, ctx.seed, 2)
    return {
        "text": f"{ctx.driver_name(driver)} выиграет гонку?",
        "resolve_key": "win_driver",
        "resolve_param": driver,
    }


@_q("podium_mid", tags=["balanced", "power"])
def _q_podium_mid(ctx: _Ctx) -> dict | None:
    driver = _seeded_pick(MIDFIELD_DRIVERS, ctx.seed, 1)
    return {
        "text": f"{ctx.driver_name(driver)} финиширует на подиуме?",
        "resolve_key": "podium_driver",
        "resolve_param": driver,
    }


# ── Points finish questions ──

@_q("points_mid", tags=["always"])
def _q_points_mid(ctx: _Ctx) -> dict | None:
    driver = _seeded_pick(MIDFIELD_DRIVERS, ctx.seed, 3)
    return {
        "text": f"{ctx.driver_name(driver)} финиширует в очках (P1-P10)?",
        "resolve_key": "in_points",
        "resolve_param": driver,
    }


@_q("points_back", tags=["balanced", "power", "stop_go"])
def _q_points_back(ctx: _Ctx) -> dict | None:
    driver = _seeded_pick(BACKFIELD_DRIVERS, ctx.seed, 0)
    return {
        "text": f"{ctx.driver_name(driver)} финиширует в очках?",
        "resolve_key": "in_points",
        "resolve_param": driver,
    }


@_q("top5_mid", tags=["street", "downforce"])
def _q_top5_mid(ctx: _Ctx) -> dict | None:
    driver = _seeded_pick(MIDFIELD_DRIVERS, ctx.seed, 4)
    return {
        "text": f"{ctx.driver_name(driver)} финиширует в топ-5?",
        "resolve_key": "top_n",
        "resolve_param": (driver, 5),
    }


# ── Head-to-head questions ──

@_q("h2h_teammates", tags=["always"])
def _q_h2h_teammates(ctx: _Ctx) -> dict | None:
    d1, d2 = _seeded_pick(TEAMMATE_PAIRS, ctx.seed, 1)
    return {
        "text": f"{ctx.driver_name(d1)} финиширует выше напарника {ctx.driver_name(d2)}?",
        "resolve_key": "head2head",
        "resolve_param": (d1, d2),
    }


@_q("h2h_cross", tags=["always"])
def _q_h2h_cross(ctx: _Ctx) -> dict | None:
    d1, d2 = _seeded_pick(CROSS_TEAM_RIVALRIES, ctx.seed, 2)
    return {
        "text": f"{ctx.driver_name(d1)} финиширует выше {ctx.driver_name(d2)}?",
        "resolve_key": "head2head",
        "resolve_param": (d1, d2),
    }


# ── DNF questions ──

@_q("dnf_any", tags=["balanced", "power", "downforce"])
def _q_dnf_any(ctx: _Ctx) -> dict | None:
    return {
        "text": "Хотя бы один пилот сойдёт с дистанции (DNF)?",
        "resolve_key": "dnf_count",
        "resolve_param": 1,
    }


@_q("dnf_multi", tags=["street", "street_fast", "stop_go"])
def _q_dnf_multi(ctx: _Ctx) -> dict | None:
    return {
        "text": "DNF >= 2 пилотов?",
        "resolve_key": "dnf_count",
        "resolve_param": 2,
    }


@_q("dnf_three", tags=["street"])
def _q_dnf_three(ctx: _Ctx) -> dict | None:
    return {
        "text": "DNF >= 3 пилотов? (уличная трасса!)",
        "resolve_key": "dnf_count",
        "resolve_param": 3,
    }


@_q("dnf_zero", tags=["downforce", "balanced"])
def _q_dnf_zero(ctx: _Ctx) -> dict | None:
    return {
        "text": "Все пилоты финишируют (0 DNF)?",
        "resolve_key": "dnf_exactly",
        "resolve_param": 0,
    }


# ── Constructor questions ──

@_q("constructor_both_points", tags=["always"])
def _q_cons_both_points(ctx: _Ctx) -> dict | None:
    cons_ids = list(CONSTRUCTOR_NAMES.keys())
    cons = _seeded_pick(cons_ids, ctx.seed, 3)
    return {
        "text": f"Оба пилота {ctx.constructor_name(cons)} финишируют в очках?",
        "resolve_key": "constructor_both_points",
        "resolve_param": cons,
    }


@_q("constructor_pole", tags=["balanced", "power"])
def _q_cons_pole(ctx: _Ctx) -> dict | None:
    top_cons = ["red_bull", "mclaren", "ferrari", "mercedes"]
    cons = _seeded_pick(top_cons, ctx.seed, 4)
    return {
        "text": f"Пилот {ctx.constructor_name(cons)} возьмёт поул?",
        "resolve_key": "constructor_pole",
        "resolve_param": cons,
    }


@_q("constructor_1_2", tags=["power"])
def _q_cons_12(ctx: _Ctx) -> dict | None:
    top_cons = ["red_bull", "mclaren", "ferrari", "mercedes"]
    cons = _seeded_pick(top_cons, ctx.seed, 5)
    return {
        "text": f"{ctx.constructor_name(cons)} сделает дубль (1-2)?",
        "resolve_key": "constructor_1_2",
        "resolve_param": cons,
    }


# ── Grid / Position questions ──

@_q("pole_wins", tags=["street", "downforce", "street_fast"])
def _q_pole_wins(ctx: _Ctx) -> dict | None:
    return {
        "text": "Обладатель поула выиграет гонку?",
        "resolve_key": "pole_wins",
        "resolve_param": None,
    }


@_q("gain_5plus", tags=["balanced", "power", "stop_go"])
def _q_gain_5plus(ctx: _Ctx) -> dict | None:
    driver = _seeded_pick(MIDFIELD_DRIVERS + BACKFIELD_DRIVERS, ctx.seed, 5)
    return {
        "text": f"{ctx.driver_name(driver)} отыграет >= 5 позиций от старта?",
        "resolve_key": "gain_positions",
        "resolve_param": (driver, 5),
    }


@_q("top10_qualify_back", tags=["power", "balanced"])
def _q_top10_from_back(ctx: _Ctx) -> dict | None:
    return {
        "text": "Кто-то из стартовавших P11+ финиширует в топ-5?",
        "resolve_key": "p11_to_top5",
        "resolve_param": None,
    }


# ── Fastest lap questions ──

@_q("fl_top3", tags=["always"])
def _q_fl_top3(ctx: _Ctx) -> dict | None:
    return {
        "text": "Быстрейший круг у пилота из топ-3 финишировавших?",
        "resolve_key": "fastest_lap_top3",
        "resolve_param": None,
    }


@_q("fl_driver", tags=["balanced", "power"])
def _q_fl_driver(ctx: _Ctx) -> dict | None:
    driver = _seeded_pick(TOP_DRIVERS, ctx.seed, 4)
    return {
        "text": f"{ctx.driver_name(driver)} покажет быстрейший круг?",
        "resolve_key": "fastest_lap_driver",
        "resolve_param": driver,
    }


# ── Track-specific questions ──

@_q("street_podium_surprise", tags=["street"])
def _q_street_surprise(ctx: _Ctx) -> dict | None:
    return {
        "text": "На подиуме будет пилот не из топ-4 команд? (уличная трасса!)",
        "resolve_key": "podium_outside_top4",
        "resolve_param": None,
    }


@_q("all_top_finish", tags=["balanced", "downforce"])
def _q_all_top_finish(ctx: _Ctx) -> dict | None:
    return {
        "text": "Все 6 пилотов топ-3 команд финишируют в очках?",
        "resolve_key": "all_top6_in_points",
        "resolve_param": None,
    }


@_q("pit_under_2s", tags=["balanced", "power"])
def _q_pit_fast(ctx: _Ctx) -> dict | None:
    return {
        "text": "Будет пит-стоп быстрее 2.0 секунд?",
        "resolve_key": "pit_under_threshold",
        "resolve_param": 2.0,
    }


# ── Question selection logic ──


def _compute_seed(race_round: int, season: int | None = None) -> int:
    """Deterministic but well-distributed seed from round + season."""
    if season is None:
        from config import settings
        season = settings.SEASON_YEAR
    raw = f"{season}:{race_round}:f1fantasy"
    return int(hashlib.md5(raw.encode()).hexdigest()[:8], 16)


class PredictionService:
    def generate_questions(self, race: Race, race_round: int) -> list[dict]:
        """Generate 7 prediction questions for a race.

        Questions are selected from a pool based on track profile and
        deterministic seed for variety across rounds.
        """
        seed = _compute_seed(race_round)
        profile = _get_track_profile(race.country)
        track_type = profile["type"]
        ctx = _Ctx(race, race_round, seed, profile)

        # Partition pool: "always" questions vs track-relevant
        always_pool = [q for q in QUESTION_POOL if "always" in q["tags"]]
        track_pool = [q for q in QUESTION_POOL if track_type in q["tags"]]
        # Fallback pool: anything not already included
        used_ids = {q["id"] for q in always_pool} | {q["id"] for q in track_pool}
        fallback_pool = [q for q in QUESTION_POOL if q["id"] not in used_ids]

        # Build questions: always first, then track-specific, then fallback
        selected: list[dict] = []
        used_resolve_keys: set[str] = set()

        def _try_add(pool_item: dict) -> bool:
            result = pool_item["builder"](ctx)
            if result is None:
                return False
            rk = result["resolve_key"]
            # Allow at most 2 questions with same resolve_key
            count = sum(1 for s in selected if s.get("resolve_key") == rk)
            if count >= 2:
                return False
            result["id"] = str(len(selected) + 1)
            result["category"] = pool_item["id"]
            selected.append(result)
            return True

        # Shuffle pools deterministically using seed
        def _det_sort(pool: list[dict]) -> list[dict]:
            return sorted(pool, key=lambda q: hash((q["id"], seed)))

        for q in _det_sort(always_pool):
            if len(selected) >= 7:
                break
            _try_add(q)

        for q in _det_sort(track_pool):
            if len(selected) >= 7:
                break
            _try_add(q)

        for q in _det_sort(fallback_pool):
            if len(selected) >= 7:
                break
            _try_add(q)

        return selected[:7]

    def resolve_questions(
        self,
        questions: list[dict],
        race_results: list[RaceResult],
        pit_stops: list[dict] | None = None,
    ) -> dict[str, bool]:
        """Resolve question outcomes based on actual race results."""
        results_map = {r.driver_id: r for r in race_results}
        actuals: dict[str, bool] = {}

        for q in questions:
            qid = q["id"]
            key = q.get("resolve_key", "")
            param = q.get("resolve_param")

            actuals[qid] = self._resolve_one(key, param, race_results, results_map, pit_stops)

        return actuals

    def _resolve_one(
        self,
        key: str,
        param: Any,
        race_results: list[RaceResult],
        results_map: dict[str, RaceResult],
        pit_stops: list[dict] | None = None,
    ) -> bool:
        if key == "podium_driver":
            r = results_map.get(param)
            return r is not None and r.finish_position is not None and r.finish_position <= 3

        if key == "win_driver":
            r = results_map.get(param)
            return r is not None and r.finish_position == 1

        if key == "in_points":
            r = results_map.get(param)
            return r is not None and r.finish_position is not None and r.finish_position <= 10

        if key == "top_n":
            driver, n = param
            r = results_map.get(driver)
            return r is not None and r.finish_position is not None and r.finish_position <= n

        if key == "head2head":
            d1, d2 = param
            r1 = results_map.get(d1)
            r2 = results_map.get(d2)
            if r1 and r2:
                if r1.dnf and not r2.dnf:
                    return False
                if r2.dnf and not r1.dnf:
                    return True
                if r1.finish_position and r2.finish_position:
                    return r1.finish_position < r2.finish_position
            return False

        if key == "dnf_count":
            dnf_count = sum(1 for r in race_results if r.dnf)
            return dnf_count >= param

        if key == "dnf_exactly":
            dnf_count = sum(1 for r in race_results if r.dnf)
            return dnf_count == param

        if key == "constructor_both_points":
            from services.budget import get_all_drivers
            drivers = get_all_drivers()
            team_ids = [d.id for d in drivers if d.team == param]
            if len(team_ids) < 2:
                return False
            return all(
                results_map.get(did) is not None
                and results_map[did].finish_position is not None
                and results_map[did].finish_position <= 10
                for did in team_ids[:2]
            )

        if key == "constructor_pole":
            from services.budget import get_all_drivers
            drivers = get_all_drivers()
            team_ids = [d.id for d in drivers if d.team == param]
            return any(
                results_map.get(did) is not None
                and results_map[did].grid_position == 1
                for did in team_ids
            )

        if key == "constructor_1_2":
            from services.budget import get_all_drivers
            drivers = get_all_drivers()
            team_ids = [d.id for d in drivers if d.team == param]
            positions = []
            for did in team_ids:
                r = results_map.get(did)
                if r and r.finish_position is not None:
                    positions.append(r.finish_position)
            return sorted(positions)[:2] == [1, 2] if len(positions) >= 2 else False

        if key == "pole_wins":
            pole_driver = None
            winner = None
            for r in race_results:
                if r.grid_position == 1:
                    pole_driver = r.driver_id
                if r.finish_position == 1:
                    winner = r.driver_id
            return pole_driver is not None and pole_driver == winner

        if key == "gain_positions":
            driver, threshold = param
            r = results_map.get(driver)
            if r and r.finish_position is not None and r.grid_position > 0:
                gain = r.grid_position - r.finish_position
                return gain >= threshold
            return False

        if key == "p11_to_top5":
            return any(
                r.grid_position >= 11
                and r.finish_position is not None
                and r.finish_position <= 5
                for r in race_results
            )

        if key == "fastest_lap_top3":
            top3_ids = {
                r.driver_id for r in race_results
                if r.finish_position is not None and r.finish_position <= 3
            }
            fl_driver = next(
                (r.driver_id for r in race_results if r.fastest_lap), None
            )
            return fl_driver in top3_ids if fl_driver else False

        if key == "fastest_lap_driver":
            fl_driver = next(
                (r.driver_id for r in race_results if r.fastest_lap), None
            )
            return fl_driver == param

        if key == "podium_outside_top4":
            top4_teams = {"red_bull", "mclaren", "ferrari", "mercedes"}
            from services.budget import get_all_drivers
            drivers = get_all_drivers()
            team_map = {d.id: d.team for d in drivers}
            return any(
                r.finish_position is not None
                and r.finish_position <= 3
                and team_map.get(r.driver_id, "") not in top4_teams
                for r in race_results
            )

        if key == "all_top6_in_points":
            top3_teams = {"red_bull", "mclaren", "ferrari"}
            from services.budget import get_all_drivers
            drivers = get_all_drivers()
            top_ids = [d.id for d in drivers if d.team in top3_teams]
            return all(
                results_map.get(did) is not None
                and results_map[did].finish_position is not None
                and results_map[did].finish_position <= 10
                for did in top_ids
            )

        if key == "pit_under_threshold":
            if not pit_stops:
                return False
            threshold = param
            for ps in pit_stops:
                dur_str = ps.get("duration", "0")
                try:
                    dur = float(dur_str)
                except (ValueError, TypeError):
                    continue
                if dur < threshold:
                    return True
            return False

        return False

    def score_predictions(
        self,
        user_answers: dict[str, dict],
        actual_results: dict[str, bool],
    ) -> tuple[int, int, dict]:
        """Score a user's predictions.

        Returns (correct_count, total_score, per_question_breakdown).
        """
        correct = 0
        total = 0
        breakdown = {}

        for qid, pred in user_answers.items():
            answer = pred.get("answer", False)
            confidence = pred.get("confidence", 1)
            actual = actual_results.get(qid, False)
            is_correct = answer == actual
            pts = confidence if is_correct else 0

            if is_correct:
                correct += 1
            total += pts

            breakdown[qid] = {
                "answer": answer,
                "actual": actual,
                "confidence": confidence,
                "correct": is_correct,
                "points": pts,
            }

        return correct, total, breakdown
