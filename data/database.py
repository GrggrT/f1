from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import asyncpg
import asyncpg.exceptions

from data.models import (
    Prediction,
    Race,
    SurvivorPick,
    UserScore,
    UserTeam,
)

logger = logging.getLogger(__name__)

# Each statement must be executed individually — asyncpg does not support
# multi-statement execute().
CREATE_TABLES_STATEMENTS: list[str] = [
    """
    CREATE TABLE IF NOT EXISTS users (
        telegram_id BIGINT PRIMARY KEY,
        username TEXT,
        display_name TEXT,
        registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        is_active BOOLEAN NOT NULL DEFAULT TRUE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS teams (
        id SERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        race_round INTEGER NOT NULL,
        drivers TEXT NOT NULL,
        constructor TEXT NOT NULL,
        turbo_driver TEXT NOT NULL,
        budget_remaining DOUBLE PRECISION NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        FOREIGN KEY (user_id) REFERENCES users(telegram_id),
        UNIQUE(user_id, race_round)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS scores (
        id SERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        race_round INTEGER NOT NULL,
        fantasy_points DOUBLE PRECISION NOT NULL,
        breakdown TEXT NOT NULL DEFAULT '{}',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        FOREIGN KEY (user_id) REFERENCES users(telegram_id),
        UNIQUE(user_id, race_round)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS races (
        round INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        country TEXT NOT NULL,
        circuit TEXT NOT NULL,
        qualifying_datetime TEXT NOT NULL,
        race_datetime TEXT NOT NULL,
        sprint BOOLEAN NOT NULL DEFAULT FALSE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS race_results (
        id SERIAL PRIMARY KEY,
        round INTEGER NOT NULL,
        driver_id TEXT NOT NULL,
        grid_position INTEGER NOT NULL,
        finish_position INTEGER,
        dnf BOOLEAN NOT NULL DEFAULT FALSE,
        fastest_lap BOOLEAN NOT NULL DEFAULT FALSE,
        points_scored DOUBLE PRECISION NOT NULL DEFAULT 0.0,
        UNIQUE(round, driver_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS predictions (
        id SERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        race_round INTEGER NOT NULL,
        questions TEXT NOT NULL DEFAULT '{}',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        FOREIGN KEY (user_id) REFERENCES users(telegram_id),
        UNIQUE(user_id, race_round)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS prediction_scores (
        id SERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        race_round INTEGER NOT NULL,
        correct_count INTEGER NOT NULL DEFAULT 0,
        total_score INTEGER NOT NULL DEFAULT 0,
        FOREIGN KEY (user_id) REFERENCES users(telegram_id),
        UNIQUE(user_id, race_round)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS survivor_picks (
        id SERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        race_round INTEGER NOT NULL,
        driver_id TEXT NOT NULL,
        survived BOOLEAN,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        FOREIGN KEY (user_id) REFERENCES users(telegram_id),
        UNIQUE(user_id, race_round)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS transfers_log (
        id SERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        race_round INTEGER NOT NULL,
        driver_out TEXT NOT NULL,
        driver_in TEXT NOT NULL,
        is_free BOOLEAN NOT NULL DEFAULT TRUE,
        timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        FOREIGN KEY (user_id) REFERENCES users(telegram_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS h2h_rivals (
        id SERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        rival_id BIGINT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        FOREIGN KEY (user_id) REFERENCES users(telegram_id),
        FOREIGN KEY (rival_id) REFERENCES users(telegram_id),
        UNIQUE(user_id, rival_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS chips (
        id SERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        chip_type TEXT NOT NULL,
        race_round_used INTEGER NOT NULL,
        activated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        FOREIGN KEY (user_id) REFERENCES users(telegram_id),
        UNIQUE(user_id, chip_type)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_scores_user ON scores(user_id)",
    "CREATE INDEX IF NOT EXISTS idx_scores_round ON scores(race_round)",
    "CREATE INDEX IF NOT EXISTS idx_teams_user_round ON teams(user_id, race_round)",
    "CREATE INDEX IF NOT EXISTS idx_transfers_user_round ON transfers_log(user_id, race_round)",
    "CREATE INDEX IF NOT EXISTS idx_predictions_round ON predictions(race_round)",
    "CREATE INDEX IF NOT EXISTS idx_prediction_scores_user ON prediction_scores(user_id)",
    "CREATE INDEX IF NOT EXISTS idx_survivor_user ON survivor_picks(user_id)",
    "CREATE INDEX IF NOT EXISTS idx_race_results_round ON race_results(round)",
    "CREATE INDEX IF NOT EXISTS idx_h2h_rivals_user ON h2h_rivals(user_id)",
    """
    CREATE TABLE IF NOT EXISTS cron_events_log (
        id SERIAL PRIMARY KEY,
        event_type TEXT NOT NULL,
        race_round INTEGER NOT NULL,
        fired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE(event_type, race_round)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS poll_state (
        id SERIAL PRIMARY KEY,
        race_round INTEGER NOT NULL,
        session_name TEXT NOT NULL DEFAULT 'Race',
        attempt_count INTEGER NOT NULL DEFAULT 0,
        UNIQUE(race_round, session_name)
    )
    """,
]


class Database:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(
            dsn=self.dsn,
            min_size=1,
            max_size=5,
            command_timeout=30,
            statement_cache_size=0,  # Required for pgBouncer (Supabase)
        )
        async with self._pool.acquire() as conn:
            for statement in CREATE_TABLES_STATEMENTS:
                await conn.execute(statement)

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def __aenter__(self) -> "Database":
        await self.connect()
        return self

    async def __aexit__(self, *args) -> None:
        await self.close()

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self._pool

    @asynccontextmanager
    async def transaction(self):
        """Context manager for database transactions with rollback on error."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                yield conn

    async def ensure_connected(self) -> None:
        """Check DB connection health, reconnect if needed."""
        if self._pool is None:
            await self.connect()
            return
        try:
            async with self._pool.acquire() as conn:
                await conn.execute("SELECT 1")
        except Exception:
            logger.warning("DB connection lost, reconnecting...")
            try:
                await self._pool.close()
            except Exception:
                pass
            self._pool = None
            await self.connect()

    # ── Users ──

    async def register_user(
        self, telegram_id: int, username: str | None, display_name: str | None
    ) -> bool:
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO users (telegram_id, username, display_name) VALUES ($1, $2, $3)",
                    telegram_id, username, display_name,
                )
            return True
        except asyncpg.exceptions.UniqueViolationError:
            return False

    async def get_user(self, telegram_id: int) -> dict | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM users WHERE telegram_id = $1", telegram_id
            )
            return dict(row) if row else None

    async def get_all_users(self) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM users WHERE is_active = TRUE"
            )
            return [dict(r) for r in rows]

    # ── Teams ──

    async def save_team(
        self, user_id: int, race_round: int, team_data: UserTeam
    ) -> None:
        drivers_json = json.dumps(team_data.drivers)
        async with self.pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO teams (user_id, race_round, drivers, constructor, turbo_driver, budget_remaining)
                   VALUES ($1, $2, $3, $4, $5, $6)
                   ON CONFLICT(user_id, race_round) DO UPDATE SET
                     drivers=EXCLUDED.drivers,
                     constructor=EXCLUDED.constructor,
                     turbo_driver=EXCLUDED.turbo_driver,
                     budget_remaining=EXCLUDED.budget_remaining,
                     created_at=NOW()""",
                user_id, race_round, drivers_json,
                team_data.constructor, team_data.turbo_driver, team_data.budget_remaining,
            )

    async def get_team(self, user_id: int, race_round: int) -> UserTeam | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT t.*, u.username FROM teams t JOIN users u ON t.user_id = u.telegram_id "
                "WHERE t.user_id = $1 AND t.race_round = $2",
                user_id, race_round,
            )
            if not row:
                return None
            row = dict(row)
            return UserTeam(
                user_id=row["user_id"],
                username=row["username"] or "",
                race_round=row["race_round"],
                drivers=json.loads(row["drivers"]),
                constructor=row["constructor"],
                turbo_driver=row["turbo_driver"],
                budget_remaining=row["budget_remaining"],
            )

    async def get_latest_team(self, user_id: int) -> UserTeam | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT t.*, u.username FROM teams t JOIN users u ON t.user_id = u.telegram_id "
                "WHERE t.user_id = $1 ORDER BY t.race_round DESC LIMIT 1",
                user_id,
            )
            if not row:
                return None
            row = dict(row)
            return UserTeam(
                user_id=row["user_id"],
                username=row["username"] or "",
                race_round=row["race_round"],
                drivers=json.loads(row["drivers"]),
                constructor=row["constructor"],
                turbo_driver=row["turbo_driver"],
                budget_remaining=row["budget_remaining"],
            )

    async def get_all_teams_for_round(self, race_round: int) -> list[UserTeam]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT t.*, u.username FROM teams t JOIN users u ON t.user_id = u.telegram_id "
                "WHERE t.race_round = $1",
                race_round,
            )
            teams = []
            for row in rows:
                row = dict(row)
                teams.append(
                    UserTeam(
                        user_id=row["user_id"],
                        username=row["username"] or "",
                        race_round=row["race_round"],
                        drivers=json.loads(row["drivers"]),
                        constructor=row["constructor"],
                        turbo_driver=row["turbo_driver"],
                        budget_remaining=row["budget_remaining"],
                    )
                )
            return teams

    # ── Scores ──

    async def save_score(
        self, user_id: int, race_round: int, points: float, breakdown: dict
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO scores (user_id, race_round, fantasy_points, breakdown)
                   VALUES ($1, $2, $3, $4)
                   ON CONFLICT(user_id, race_round) DO UPDATE SET
                     fantasy_points=EXCLUDED.fantasy_points,
                     breakdown=EXCLUDED.breakdown""",
                user_id, race_round, points, json.dumps(breakdown),
            )

    async def get_standings(self) -> list[tuple]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT s.user_id, u.username, SUM(s.fantasy_points) as total_points
                   FROM scores s
                   JOIN users u ON s.user_id = u.telegram_id
                   GROUP BY s.user_id, u.username
                   ORDER BY total_points DESC"""
            )
            return [tuple(r.values()) for r in rows]

    async def get_race_scores(self, race_round: int) -> list[UserScore]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM scores WHERE race_round = $1 ORDER BY fantasy_points DESC",
                race_round,
            )
            return [
                UserScore(
                    user_id=row["user_id"],
                    race_round=row["race_round"],
                    fantasy_points=row["fantasy_points"],
                    breakdown=json.loads(row["breakdown"]),
                )
                for row in rows
            ]

    async def get_race_scores_with_users(self, race_round: int) -> list[dict]:
        """Get race scores with usernames in a single query (no N+1)."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT s.user_id, s.race_round, s.fantasy_points, s.breakdown,
                          u.username, u.display_name
                   FROM scores s
                   JOIN users u ON s.user_id = u.telegram_id
                   WHERE s.race_round = $1
                   ORDER BY s.fantasy_points DESC""",
                race_round,
            )
            return [dict(r) for r in rows]

    # ── Races ──

    async def save_race(self, race: Race) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO races (round, name, country, circuit, qualifying_datetime, race_datetime, sprint)
                   VALUES ($1, $2, $3, $4, $5, $6, $7)
                   ON CONFLICT(round) DO UPDATE SET
                     name=EXCLUDED.name, country=EXCLUDED.country, circuit=EXCLUDED.circuit,
                     qualifying_datetime=EXCLUDED.qualifying_datetime,
                     race_datetime=EXCLUDED.race_datetime, sprint=EXCLUDED.sprint""",
                race.round, race.name, race.country, race.circuit,
                race.qualifying_datetime, race.race_datetime, race.sprint,
            )

    async def get_next_race(self) -> Race | None:
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM races WHERE race_datetime > $1 ORDER BY round ASC LIMIT 1",
                now,
            )
            if not row:
                return None
            row = dict(row)
            return Race(
                round=row["round"],
                name=row["name"],
                country=row["country"],
                circuit=row["circuit"],
                qualifying_datetime=row["qualifying_datetime"],
                race_datetime=row["race_datetime"],
                sprint=bool(row["sprint"]),
            )

    async def get_race(self, round_num: int) -> Race | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM races WHERE round = $1", round_num
            )
            if not row:
                return None
            row = dict(row)
            return Race(
                round=row["round"],
                name=row["name"],
                country=row["country"],
                circuit=row["circuit"],
                qualifying_datetime=row["qualifying_datetime"],
                race_datetime=row["race_datetime"],
                sprint=bool(row["sprint"]),
            )

    # ── Race Results ──

    async def save_race_results(self, results: list[dict]) -> None:
        async with self.pool.acquire() as conn:
            for r in results:
                await conn.execute(
                    """INSERT INTO race_results (round, driver_id, grid_position, finish_position, dnf, fastest_lap, points_scored)
                       VALUES ($1, $2, $3, $4, $5, $6, $7)
                       ON CONFLICT(round, driver_id) DO UPDATE SET
                         grid_position=EXCLUDED.grid_position,
                         finish_position=EXCLUDED.finish_position,
                         dnf=EXCLUDED.dnf, fastest_lap=EXCLUDED.fastest_lap,
                         points_scored=EXCLUDED.points_scored""",
                    r["round"], r["driver_id"], r["grid_position"],
                    r.get("finish_position"),
                    bool(r.get("dnf", False)),
                    bool(r.get("fastest_lap", False)),
                    r.get("points_scored", 0.0),
                )

    async def get_race_results(self, round_num: int) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM race_results WHERE round = $1 ORDER BY finish_position ASC",
                round_num,
            )
            return [dict(r) for r in rows]

    # ── Predictions ──

    async def save_prediction(self, prediction: Prediction) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO predictions (user_id, race_round, questions)
                   VALUES ($1, $2, $3)
                   ON CONFLICT(user_id, race_round) DO UPDATE SET
                     questions=EXCLUDED.questions""",
                prediction.user_id, prediction.race_round,
                json.dumps(prediction.questions),
            )

    async def get_prediction(self, user_id: int, race_round: int) -> Prediction | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM predictions WHERE user_id = $1 AND race_round = $2",
                user_id, race_round,
            )
            if not row:
                return None
            return Prediction(
                user_id=row["user_id"],
                race_round=row["race_round"],
                questions=json.loads(row["questions"]),
            )

    async def get_predictions(self, race_round: int) -> list[Prediction]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM predictions WHERE race_round = $1", race_round
            )
            return [
                Prediction(
                    user_id=row["user_id"],
                    race_round=row["race_round"],
                    questions=json.loads(row["questions"]),
                )
                for row in rows
            ]

    async def save_prediction_score(
        self, user_id: int, race_round: int, correct_count: int, total_score: int
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO prediction_scores (user_id, race_round, correct_count, total_score)
                   VALUES ($1, $2, $3, $4)
                   ON CONFLICT(user_id, race_round) DO UPDATE SET
                     correct_count=EXCLUDED.correct_count, total_score=EXCLUDED.total_score""",
                user_id, race_round, correct_count, total_score,
            )

    # ── Survivor ──

    async def save_survivor_pick(self, pick: SurvivorPick) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO survivor_picks (user_id, race_round, driver_id, survived)
                   VALUES ($1, $2, $3, $4)
                   ON CONFLICT(user_id, race_round) DO UPDATE SET
                     driver_id=EXCLUDED.driver_id, survived=EXCLUDED.survived""",
                pick.user_id, pick.race_round, pick.driver_id, pick.survived,
            )

    async def get_survivor_picks(self, user_id: int) -> list[SurvivorPick]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM survivor_picks WHERE user_id = $1 ORDER BY race_round",
                user_id,
            )
            return [
                SurvivorPick(
                    user_id=row["user_id"],
                    race_round=row["race_round"],
                    driver_id=row["driver_id"],
                    survived=row["survived"],
                )
                for row in rows
            ]

    async def update_survivor_result(
        self, user_id: int, race_round: int, survived: bool
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE survivor_picks SET survived = $1 WHERE user_id = $2 AND race_round = $3",
                survived, user_id, race_round,
            )

    # ── Transfers ──

    async def log_transfer(
        self, user_id: int, race_round: int, driver_out: str, driver_in: str, is_free: bool = True
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO transfers_log (user_id, race_round, driver_out, driver_in, is_free) VALUES ($1, $2, $3, $4, $5)",
                user_id, race_round, driver_out, driver_in, is_free,
            )

    async def get_transfers_count(self, user_id: int, race_round: int) -> int:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT COUNT(*) as cnt FROM transfers_log WHERE user_id = $1 AND race_round = $2",
                user_id, race_round,
            )
            return row["cnt"] if row else 0

    # ── Chips ──

    async def activate_chip(
        self, user_id: int, chip_type: str, race_round: int
    ) -> bool:
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO chips (user_id, chip_type, race_round_used) VALUES ($1, $2, $3)",
                    user_id, chip_type, race_round,
                )
            return True
        except asyncpg.exceptions.UniqueViolationError:
            return False

    async def get_used_chips(self, user_id: int) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM chips WHERE user_id = $1", user_id
            )
            return [dict(r) for r in rows]

    async def get_active_chip(self, user_id: int, race_round: int) -> str | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT chip_type FROM chips WHERE user_id = $1 AND race_round_used = $2",
                user_id, race_round,
            )
            return row["chip_type"] if row else None

    # ── Prediction Standings ──

    async def get_prediction_standings(self) -> list[tuple]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT ps.user_id, u.username,
                          SUM(ps.correct_count) as total_correct,
                          SUM(ps.total_score) as total_score,
                          COUNT(ps.race_round) as rounds_played
                   FROM prediction_scores ps
                   JOIN users u ON ps.user_id = u.telegram_id
                   GROUP BY ps.user_id, u.username
                   ORDER BY total_score DESC"""
            )
            return [tuple(r.values()) for r in rows]

    async def get_user_prediction_history(self, user_id: int) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT ps.race_round, ps.correct_count, ps.total_score,
                          r.name as race_name
                   FROM prediction_scores ps
                   LEFT JOIN races r ON ps.race_round = r.round
                   WHERE ps.user_id = $1
                   ORDER BY ps.race_round""",
                user_id,
            )
            return [dict(r) for r in rows]

    # ── User History ──

    async def get_user_score_history(self, user_id: int) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT s.race_round, s.fantasy_points, r.name as race_name
                   FROM scores s
                   LEFT JOIN races r ON s.race_round = r.round
                   WHERE s.user_id = $1
                   ORDER BY s.race_round""",
                user_id,
            )
            return [dict(r) for r in rows]

    async def get_user_transfers(self, user_id: int) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT race_round, driver_out, driver_in, is_free, timestamp
                   FROM transfers_log
                   WHERE user_id = $1
                   ORDER BY timestamp DESC
                   LIMIT 20""",
                user_id,
            )
            return [dict(r) for r in rows]

    async def get_all_user_teams(self, user_id: int) -> list[UserTeam]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT t.*, u.username FROM teams t JOIN users u ON t.user_id = u.telegram_id "
                "WHERE t.user_id = $1 ORDER BY t.race_round",
                user_id,
            )
            teams = []
            for row in rows:
                row = dict(row)
                teams.append(UserTeam(
                    user_id=row["user_id"],
                    username=row["username"] or "",
                    race_round=row["race_round"],
                    drivers=json.loads(row["drivers"]),
                    constructor=row["constructor"],
                    turbo_driver=row["turbo_driver"],
                    budget_remaining=row["budget_remaining"],
                ))
            return teams

    # ── Deadline Check Helpers ──

    async def get_users_without_team(self, race_round: int) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT u.telegram_id, u.username
                   FROM users u
                   WHERE u.is_active = TRUE
                     AND u.telegram_id NOT IN (
                       SELECT user_id FROM teams WHERE race_round = $1
                     )""",
                race_round,
            )
            return [dict(r) for r in rows]

    async def get_users_without_prediction(self, race_round: int) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT u.telegram_id, u.username
                   FROM users u
                   WHERE u.is_active = TRUE
                     AND u.telegram_id NOT IN (
                       SELECT user_id FROM predictions WHERE race_round = $1
                     )""",
                race_round,
            )
            return [dict(r) for r in rows]

    # ── Standings Progression ──

    async def get_all_scores_by_round(self) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT s.user_id, u.username, s.race_round, s.fantasy_points
                   FROM scores s
                   JOIN users u ON s.user_id = u.telegram_id
                   ORDER BY s.race_round, s.fantasy_points DESC"""
            )
            return [dict(r) for r in rows]

    # ── Export ──

    async def export_all_data(self, table: str | None = None, limit: int = 0, offset: int = 0) -> dict:
        tables = [table] if table else [
            "users", "teams", "scores", "races", "race_results",
            "predictions", "prediction_scores", "survivor_picks",
            "transfers_log", "h2h_rivals", "chips",
        ]
        result = {}
        allowed = {"users", "teams", "scores", "races", "race_results",
                    "predictions", "prediction_scores", "survivor_picks",
                    "transfers_log", "h2h_rivals", "chips"}
        async with self.pool.acquire() as conn:
            for t in tables:
                if t not in allowed:
                    continue  # prevent SQL injection
                query = f"SELECT * FROM {t}"
                if limit > 0:
                    query += f" LIMIT {int(limit)} OFFSET {int(offset)}"
                rows = await conn.fetch(query)
                result[t] = [dict(r) for r in rows]
        return result

    # ── H2H Rivals ──

    async def set_rival(self, user_id: int, rival_id: int) -> bool:
        """Set a rival for H2H tracking. Returns False if already set."""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO h2h_rivals (user_id, rival_id) VALUES ($1, $2)",
                    user_id, rival_id,
                )
            return True
        except asyncpg.exceptions.UniqueViolationError:
            return False

    async def remove_rival(self, user_id: int, rival_id: int) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM h2h_rivals WHERE user_id = $1 AND rival_id = $2",
                user_id, rival_id,
            )

    async def get_rivals(self, user_id: int) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT h.rival_id, u.username, u.display_name
                   FROM h2h_rivals h
                   JOIN users u ON h.rival_id = u.telegram_id
                   WHERE h.user_id = $1""",
                user_id,
            )
            return [dict(r) for r in rows]

    async def get_h2h_record(self, user_id: int, rival_id: int) -> dict:
        """Get H2H record between two users across all rounds."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT s1.race_round, s1.fantasy_points as user_pts, s2.fantasy_points as rival_pts,
                          r.name as race_name
                   FROM scores s1
                   JOIN scores s2 ON s1.race_round = s2.race_round AND s2.user_id = $1
                   LEFT JOIN races r ON s1.race_round = r.round
                   WHERE s1.user_id = $2
                   ORDER BY s1.race_round""",
                rival_id, user_id,
            )
            results = [dict(r) for r in rows]
            wins = sum(1 for r in results if r["user_pts"] > r["rival_pts"])
            losses = sum(1 for r in results if r["user_pts"] < r["rival_pts"])
            draws = sum(1 for r in results if r["user_pts"] == r["rival_pts"])
            return {"rounds": results, "wins": wins, "losses": losses, "draws": draws}

    # ── Driver Stats ──

    async def get_driver_fantasy_stats(self, driver_id: str) -> list[dict]:
        """Get fantasy scoring history for a driver across all rounds."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT rr.round, rr.grid_position, rr.finish_position, rr.dnf,
                          rr.fastest_lap, r.name as race_name
                   FROM race_results rr
                   LEFT JOIN races r ON rr.round = r.round
                   WHERE rr.driver_id = $1
                   ORDER BY rr.round""",
                driver_id,
            )
            return [dict(r) for r in rows]

    async def get_driver_pick_stats(self, driver_id: str) -> list[dict]:
        """Get how many teams picked this driver per round."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT t.race_round, COUNT(*) as pick_count,
                          (SELECT COUNT(*) FROM teams t2 WHERE t2.race_round = t.race_round) as total_teams
                   FROM teams t
                   WHERE t.drivers LIKE $1
                   GROUP BY t.race_round
                   ORDER BY t.race_round""",
                f'%"{driver_id}"%',
            )
            return [dict(r) for r in rows]

    async def cancel_race(self, round_num: int) -> None:
        """Mark a race as cancelled and clean up related data."""
        async with self.transaction() as conn:
            await conn.execute(
                "DELETE FROM races WHERE round = $1", round_num
            )
            await conn.execute(
                "DELETE FROM teams WHERE race_round = $1", round_num
            )
            await conn.execute(
                "DELETE FROM scores WHERE race_round = $1", round_num
            )
            await conn.execute(
                "DELETE FROM predictions WHERE race_round = $1", round_num
            )
            await conn.execute(
                "DELETE FROM prediction_scores WHERE race_round = $1", round_num
            )
            await conn.execute(
                "DELETE FROM survivor_picks WHERE race_round = $1", round_num
            )
            await conn.execute(
                "DELETE FROM transfers_log WHERE race_round = $1", round_num
            )
