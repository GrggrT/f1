from __future__ import annotations

import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import aiosqlite

from data.models import (
    Prediction,
    Race,
    SurvivorPick,
    UserScore,
    UserTeam,
)

logger = logging.getLogger(__name__)

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS users (
    telegram_id INTEGER PRIMARY KEY,
    username TEXT,
    display_name TEXT,
    registered_at TEXT NOT NULL DEFAULT (datetime('now')),
    is_active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS teams (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    race_round INTEGER NOT NULL,
    drivers TEXT NOT NULL,
    constructor TEXT NOT NULL,
    turbo_driver TEXT NOT NULL,
    budget_remaining REAL NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (user_id) REFERENCES users(telegram_id),
    UNIQUE(user_id, race_round)
);

CREATE TABLE IF NOT EXISTS scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    race_round INTEGER NOT NULL,
    fantasy_points REAL NOT NULL,
    breakdown TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (user_id) REFERENCES users(telegram_id),
    UNIQUE(user_id, race_round)
);

CREATE TABLE IF NOT EXISTS races (
    round INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    country TEXT NOT NULL,
    circuit TEXT NOT NULL,
    qualifying_datetime TEXT NOT NULL,
    race_datetime TEXT NOT NULL,
    sprint INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS race_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    round INTEGER NOT NULL,
    driver_id TEXT NOT NULL,
    grid_position INTEGER NOT NULL,
    finish_position INTEGER,
    dnf INTEGER NOT NULL DEFAULT 0,
    fastest_lap INTEGER NOT NULL DEFAULT 0,
    points_scored REAL NOT NULL DEFAULT 0.0,
    UNIQUE(round, driver_id)
);

CREATE TABLE IF NOT EXISTS predictions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    race_round INTEGER NOT NULL,
    questions TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (user_id) REFERENCES users(telegram_id),
    UNIQUE(user_id, race_round)
);

CREATE TABLE IF NOT EXISTS prediction_scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    race_round INTEGER NOT NULL,
    correct_count INTEGER NOT NULL DEFAULT 0,
    total_score INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (user_id) REFERENCES users(telegram_id),
    UNIQUE(user_id, race_round)
);

CREATE TABLE IF NOT EXISTS survivor_picks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    race_round INTEGER NOT NULL,
    driver_id TEXT NOT NULL,
    survived INTEGER,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (user_id) REFERENCES users(telegram_id),
    UNIQUE(user_id, race_round)
);

CREATE TABLE IF NOT EXISTS transfers_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    race_round INTEGER NOT NULL,
    driver_out TEXT NOT NULL,
    driver_in TEXT NOT NULL,
    is_free INTEGER NOT NULL DEFAULT 1,
    timestamp TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (user_id) REFERENCES users(telegram_id)
);

CREATE TABLE IF NOT EXISTS h2h_rivals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    rival_id INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (user_id) REFERENCES users(telegram_id),
    FOREIGN KEY (rival_id) REFERENCES users(telegram_id),
    UNIQUE(user_id, rival_id)
);

CREATE TABLE IF NOT EXISTS chips (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    chip_type TEXT NOT NULL,
    race_round_used INTEGER NOT NULL,
    activated_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (user_id) REFERENCES users(telegram_id),
    UNIQUE(user_id, chip_type)
);

CREATE INDEX IF NOT EXISTS idx_scores_user ON scores(user_id);
CREATE INDEX IF NOT EXISTS idx_scores_round ON scores(race_round);
CREATE INDEX IF NOT EXISTS idx_teams_user_round ON teams(user_id, race_round);
CREATE INDEX IF NOT EXISTS idx_transfers_user_round ON transfers_log(user_id, race_round);
CREATE INDEX IF NOT EXISTS idx_predictions_round ON predictions(race_round);
CREATE INDEX IF NOT EXISTS idx_prediction_scores_user ON prediction_scores(user_id);
CREATE INDEX IF NOT EXISTS idx_survivor_user ON survivor_picks(user_id);
CREATE INDEX IF NOT EXISTS idx_race_results_round ON race_results(round);
CREATE INDEX IF NOT EXISTS idx_h2h_rivals_user ON h2h_rivals(user_id);
"""


class Database:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        self._db = await aiosqlite.connect(self.db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.executescript(CREATE_TABLES_SQL)
        await self._db.commit()

    async def close(self) -> None:
        if self._db:
            await self._db.close()
            self._db = None

    async def __aenter__(self) -> "Database":
        await self.connect()
        return self

    async def __aexit__(self, *args) -> None:
        await self.close()

    @property
    def db(self) -> aiosqlite.Connection:
        if self._db is None:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self._db

    @asynccontextmanager
    async def transaction(self):
        """Context manager for database transactions with rollback on error."""
        await self.db.execute("BEGIN")
        try:
            yield self.db
            await self.db.commit()
        except Exception:
            await self.db.rollback()
            raise

    async def ensure_connected(self) -> None:
        """Check DB connection health, reconnect if needed."""
        if self._db is None:
            await self.connect()
            return
        try:
            await self._db.execute("SELECT 1")
        except Exception:
            logger.warning("DB connection lost, reconnecting...")
            try:
                await self._db.close()
            except Exception:
                pass
            self._db = None
            await self.connect()

    # ── Users ──

    async def register_user(
        self, telegram_id: int, username: str | None, display_name: str | None
    ) -> bool:
        try:
            await self.db.execute(
                "INSERT INTO users (telegram_id, username, display_name) VALUES (?, ?, ?)",
                (telegram_id, username, display_name),
            )
            await self.db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False

    async def get_user(self, telegram_id: int) -> dict | None:
        cursor = await self.db.execute(
            "SELECT * FROM users WHERE telegram_id = ?", (telegram_id,)
        )
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def get_all_users(self) -> list[dict]:
        cursor = await self.db.execute(
            "SELECT * FROM users WHERE is_active = 1"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    # ── Teams ──

    async def save_team(
        self, user_id: int, race_round: int, team_data: UserTeam
    ) -> None:
        drivers_json = json.dumps(team_data.drivers)
        await self.db.execute(
            """INSERT INTO teams (user_id, race_round, drivers, constructor, turbo_driver, budget_remaining)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(user_id, race_round) DO UPDATE SET
                 drivers=excluded.drivers,
                 constructor=excluded.constructor,
                 turbo_driver=excluded.turbo_driver,
                 budget_remaining=excluded.budget_remaining,
                 created_at=datetime('now')""",
            (
                user_id,
                race_round,
                drivers_json,
                team_data.constructor,
                team_data.turbo_driver,
                team_data.budget_remaining,
            ),
        )
        await self.db.commit()

    async def get_team(self, user_id: int, race_round: int) -> UserTeam | None:
        cursor = await self.db.execute(
            "SELECT t.*, u.username FROM teams t JOIN users u ON t.user_id = u.telegram_id "
            "WHERE t.user_id = ? AND t.race_round = ?",
            (user_id, race_round),
        )
        row = await cursor.fetchone()
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
        cursor = await self.db.execute(
            "SELECT t.*, u.username FROM teams t JOIN users u ON t.user_id = u.telegram_id "
            "WHERE t.user_id = ? ORDER BY t.race_round DESC LIMIT 1",
            (user_id,),
        )
        row = await cursor.fetchone()
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
        cursor = await self.db.execute(
            "SELECT t.*, u.username FROM teams t JOIN users u ON t.user_id = u.telegram_id "
            "WHERE t.race_round = ?",
            (race_round,),
        )
        rows = await cursor.fetchall()
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
        await self.db.execute(
            """INSERT INTO scores (user_id, race_round, fantasy_points, breakdown)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(user_id, race_round) DO UPDATE SET
                 fantasy_points=excluded.fantasy_points,
                 breakdown=excluded.breakdown""",
            (user_id, race_round, points, json.dumps(breakdown)),
        )
        await self.db.commit()

    async def get_standings(self) -> list[tuple]:
        cursor = await self.db.execute(
            """SELECT s.user_id, u.username, SUM(s.fantasy_points) as total_points
               FROM scores s
               JOIN users u ON s.user_id = u.telegram_id
               GROUP BY s.user_id
               ORDER BY total_points DESC"""
        )
        return await cursor.fetchall()

    async def get_race_scores(self, race_round: int) -> list[UserScore]:
        cursor = await self.db.execute(
            "SELECT * FROM scores WHERE race_round = ? ORDER BY fantasy_points DESC",
            (race_round,),
        )
        rows = await cursor.fetchall()
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
        cursor = await self.db.execute(
            """SELECT s.user_id, s.race_round, s.fantasy_points, s.breakdown,
                      u.username, u.display_name
               FROM scores s
               JOIN users u ON s.user_id = u.telegram_id
               WHERE s.race_round = ?
               ORDER BY s.fantasy_points DESC""",
            (race_round,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    # ── Races ──

    async def save_race(self, race: Race) -> None:
        await self.db.execute(
            """INSERT INTO races (round, name, country, circuit, qualifying_datetime, race_datetime, sprint)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(round) DO UPDATE SET
                 name=excluded.name, country=excluded.country, circuit=excluded.circuit,
                 qualifying_datetime=excluded.qualifying_datetime,
                 race_datetime=excluded.race_datetime, sprint=excluded.sprint""",
            (
                race.round,
                race.name,
                race.country,
                race.circuit,
                race.qualifying_datetime,
                race.race_datetime,
                int(race.sprint),
            ),
        )
        await self.db.commit()

    async def get_next_race(self) -> Race | None:
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        cursor = await self.db.execute(
            "SELECT * FROM races WHERE race_datetime > ? ORDER BY round ASC LIMIT 1",
            (now,),
        )
        row = await cursor.fetchone()
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
        cursor = await self.db.execute(
            "SELECT * FROM races WHERE round = ?", (round_num,)
        )
        row = await cursor.fetchone()
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
        for r in results:
            await self.db.execute(
                """INSERT INTO race_results (round, driver_id, grid_position, finish_position, dnf, fastest_lap, points_scored)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(round, driver_id) DO UPDATE SET
                     grid_position=excluded.grid_position,
                     finish_position=excluded.finish_position,
                     dnf=excluded.dnf, fastest_lap=excluded.fastest_lap,
                     points_scored=excluded.points_scored""",
                (
                    r["round"],
                    r["driver_id"],
                    r["grid_position"],
                    r.get("finish_position"),
                    int(r.get("dnf", False)),
                    int(r.get("fastest_lap", False)),
                    r.get("points_scored", 0.0),
                ),
            )
        await self.db.commit()

    async def get_race_results(self, round_num: int) -> list[dict]:
        cursor = await self.db.execute(
            "SELECT * FROM race_results WHERE round = ? ORDER BY finish_position ASC",
            (round_num,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    # ── Predictions ──

    async def save_prediction(self, prediction: Prediction) -> None:
        await self.db.execute(
            """INSERT INTO predictions (user_id, race_round, questions)
               VALUES (?, ?, ?)
               ON CONFLICT(user_id, race_round) DO UPDATE SET
                 questions=excluded.questions""",
            (
                prediction.user_id,
                prediction.race_round,
                json.dumps(prediction.questions),
            ),
        )
        await self.db.commit()

    async def get_prediction(self, user_id: int, race_round: int) -> Prediction | None:
        cursor = await self.db.execute(
            "SELECT * FROM predictions WHERE user_id = ? AND race_round = ?",
            (user_id, race_round),
        )
        row = await cursor.fetchone()
        if not row:
            return None
        return Prediction(
            user_id=row["user_id"],
            race_round=row["race_round"],
            questions=json.loads(row["questions"]),
        )

    async def get_predictions(self, race_round: int) -> list[Prediction]:
        cursor = await self.db.execute(
            "SELECT * FROM predictions WHERE race_round = ?", (race_round,)
        )
        rows = await cursor.fetchall()
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
        await self.db.execute(
            """INSERT INTO prediction_scores (user_id, race_round, correct_count, total_score)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(user_id, race_round) DO UPDATE SET
                 correct_count=excluded.correct_count, total_score=excluded.total_score""",
            (user_id, race_round, correct_count, total_score),
        )
        await self.db.commit()

    # ── Survivor ──

    async def save_survivor_pick(self, pick: SurvivorPick) -> None:
        await self.db.execute(
            """INSERT INTO survivor_picks (user_id, race_round, driver_id, survived)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(user_id, race_round) DO UPDATE SET
                 driver_id=excluded.driver_id, survived=excluded.survived""",
            (pick.user_id, pick.race_round, pick.driver_id, pick.survived),
        )
        await self.db.commit()

    async def get_survivor_picks(self, user_id: int) -> list[SurvivorPick]:
        cursor = await self.db.execute(
            "SELECT * FROM survivor_picks WHERE user_id = ? ORDER BY race_round",
            (user_id,),
        )
        rows = await cursor.fetchall()
        return [
            SurvivorPick(
                user_id=row["user_id"],
                race_round=row["race_round"],
                driver_id=row["driver_id"],
                survived=bool(row["survived"]) if row["survived"] is not None else None,
            )
            for row in rows
        ]

    async def update_survivor_result(
        self, user_id: int, race_round: int, survived: bool
    ) -> None:
        await self.db.execute(
            "UPDATE survivor_picks SET survived = ? WHERE user_id = ? AND race_round = ?",
            (int(survived), user_id, race_round),
        )
        await self.db.commit()

    # ── Transfers ──

    async def log_transfer(
        self, user_id: int, race_round: int, driver_out: str, driver_in: str, is_free: bool = True
    ) -> None:
        await self.db.execute(
            "INSERT INTO transfers_log (user_id, race_round, driver_out, driver_in, is_free) VALUES (?, ?, ?, ?, ?)",
            (user_id, race_round, driver_out, driver_in, int(is_free)),
        )
        await self.db.commit()

    async def get_transfers_count(self, user_id: int, race_round: int) -> int:
        cursor = await self.db.execute(
            "SELECT COUNT(*) as cnt FROM transfers_log WHERE user_id = ? AND race_round = ?",
            (user_id, race_round),
        )
        row = await cursor.fetchone()
        return row["cnt"] if row else 0

    # ── Chips ──

    async def activate_chip(
        self, user_id: int, chip_type: str, race_round: int
    ) -> bool:
        try:
            await self.db.execute(
                "INSERT INTO chips (user_id, chip_type, race_round_used) VALUES (?, ?, ?)",
                (user_id, chip_type, race_round),
            )
            await self.db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False

    async def get_used_chips(self, user_id: int) -> list[dict]:
        cursor = await self.db.execute(
            "SELECT * FROM chips WHERE user_id = ?", (user_id,)
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_active_chip(self, user_id: int, race_round: int) -> str | None:
        cursor = await self.db.execute(
            "SELECT chip_type FROM chips WHERE user_id = ? AND race_round_used = ?",
            (user_id, race_round),
        )
        row = await cursor.fetchone()
        return row["chip_type"] if row else None

    # ── Prediction Standings ──

    async def get_prediction_standings(self) -> list[tuple]:
        cursor = await self.db.execute(
            """SELECT ps.user_id, u.username,
                      SUM(ps.correct_count) as total_correct,
                      SUM(ps.total_score) as total_score,
                      COUNT(ps.race_round) as rounds_played
               FROM prediction_scores ps
               JOIN users u ON ps.user_id = u.telegram_id
               GROUP BY ps.user_id
               ORDER BY total_score DESC"""
        )
        return await cursor.fetchall()

    async def get_user_prediction_history(self, user_id: int) -> list[dict]:
        cursor = await self.db.execute(
            """SELECT ps.race_round, ps.correct_count, ps.total_score,
                      r.name as race_name
               FROM prediction_scores ps
               LEFT JOIN races r ON ps.race_round = r.round
               WHERE ps.user_id = ?
               ORDER BY ps.race_round""",
            (user_id,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    # ── User History ──

    async def get_user_score_history(self, user_id: int) -> list[dict]:
        cursor = await self.db.execute(
            """SELECT s.race_round, s.fantasy_points, r.name as race_name
               FROM scores s
               LEFT JOIN races r ON s.race_round = r.round
               WHERE s.user_id = ?
               ORDER BY s.race_round""",
            (user_id,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_user_transfers(self, user_id: int) -> list[dict]:
        cursor = await self.db.execute(
            """SELECT race_round, driver_out, driver_in, is_free, timestamp
               FROM transfers_log
               WHERE user_id = ?
               ORDER BY timestamp DESC
               LIMIT 20""",
            (user_id,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_all_user_teams(self, user_id: int) -> list[UserTeam]:
        cursor = await self.db.execute(
            "SELECT t.*, u.username FROM teams t JOIN users u ON t.user_id = u.telegram_id "
            "WHERE t.user_id = ? ORDER BY t.race_round",
            (user_id,),
        )
        rows = await cursor.fetchall()
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
        cursor = await self.db.execute(
            """SELECT u.telegram_id, u.username
               FROM users u
               WHERE u.is_active = 1
                 AND u.telegram_id NOT IN (
                   SELECT user_id FROM teams WHERE race_round = ?
                 )""",
            (race_round,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_users_without_prediction(self, race_round: int) -> list[dict]:
        cursor = await self.db.execute(
            """SELECT u.telegram_id, u.username
               FROM users u
               WHERE u.is_active = 1
                 AND u.telegram_id NOT IN (
                   SELECT user_id FROM predictions WHERE race_round = ?
                 )""",
            (race_round,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    # ── Standings Progression ──

    async def get_all_scores_by_round(self) -> list[dict]:
        cursor = await self.db.execute(
            """SELECT s.user_id, u.username, s.race_round, s.fantasy_points
               FROM scores s
               JOIN users u ON s.user_id = u.telegram_id
               ORDER BY s.race_round, s.fantasy_points DESC"""
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    # ── Export ──

    async def export_all_data(self, table: str | None = None, limit: int = 0, offset: int = 0) -> dict:
        tables = [table] if table else [
            "users", "teams", "scores", "races", "race_results",
            "predictions", "prediction_scores", "survivor_picks",
            "transfers_log", "h2h_rivals", "chips",
        ]
        result = {}
        for t in tables:
            if t not in {"users", "teams", "scores", "races", "race_results",
                          "predictions", "prediction_scores", "survivor_picks",
                          "transfers_log", "h2h_rivals", "chips"}:
                continue  # prevent SQL injection
            query = f"SELECT * FROM {t}"
            params = []
            if limit > 0:
                query += " LIMIT ? OFFSET ?"
                params = [limit, offset]
            cursor = await self.db.execute(query, params)
            rows = await cursor.fetchall()
            result[t] = [dict(r) for r in rows]
        return result

    # ── H2H Rivals ──

    async def set_rival(self, user_id: int, rival_id: int) -> bool:
        """Set a rival for H2H tracking. Returns False if already set."""
        try:
            await self.db.execute(
                "INSERT INTO h2h_rivals (user_id, rival_id) VALUES (?, ?)",
                (user_id, rival_id),
            )
            await self.db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False

    async def remove_rival(self, user_id: int, rival_id: int) -> None:
        await self.db.execute(
            "DELETE FROM h2h_rivals WHERE user_id = ? AND rival_id = ?",
            (user_id, rival_id),
        )
        await self.db.commit()

    async def get_rivals(self, user_id: int) -> list[dict]:
        cursor = await self.db.execute(
            """SELECT h.rival_id, u.username, u.display_name
               FROM h2h_rivals h
               JOIN users u ON h.rival_id = u.telegram_id
               WHERE h.user_id = ?""",
            (user_id,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_h2h_record(self, user_id: int, rival_id: int) -> dict:
        """Get H2H record between two users across all rounds."""
        cursor = await self.db.execute(
            """SELECT s1.race_round, s1.fantasy_points as user_pts, s2.fantasy_points as rival_pts,
                      r.name as race_name
               FROM scores s1
               JOIN scores s2 ON s1.race_round = s2.race_round AND s2.user_id = ?
               LEFT JOIN races r ON s1.race_round = r.round
               WHERE s1.user_id = ?
               ORDER BY s1.race_round""",
            (rival_id, user_id),
        )
        rows = await cursor.fetchall()
        results = [dict(r) for r in rows]
        wins = sum(1 for r in results if r["user_pts"] > r["rival_pts"])
        losses = sum(1 for r in results if r["user_pts"] < r["rival_pts"])
        draws = sum(1 for r in results if r["user_pts"] == r["rival_pts"])
        return {"rounds": results, "wins": wins, "losses": losses, "draws": draws}

    # ── Driver Stats ──

    async def get_driver_fantasy_stats(self, driver_id: str) -> list[dict]:
        """Get fantasy scoring history for a driver across all rounds."""
        cursor = await self.db.execute(
            """SELECT rr.round, rr.grid_position, rr.finish_position, rr.dnf,
                      rr.fastest_lap, r.name as race_name
               FROM race_results rr
               LEFT JOIN races r ON rr.round = r.round
               WHERE rr.driver_id = ?
               ORDER BY rr.round""",
            (driver_id,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_driver_pick_stats(self, driver_id: str) -> list[dict]:
        """Get how many teams picked this driver per round."""
        cursor = await self.db.execute(
            """SELECT t.race_round, COUNT(*) as pick_count,
                      (SELECT COUNT(*) FROM teams t2 WHERE t2.race_round = t.race_round) as total_teams
               FROM teams t
               WHERE t.drivers LIKE ?
               GROUP BY t.race_round
               ORDER BY t.race_round""",
            (f'%"{driver_id}"%',),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def cancel_race(self, round_num: int) -> None:
        """Mark a race as cancelled and clean up related data."""
        async with self.transaction():
            await self.db.execute(
                "DELETE FROM races WHERE round = ?", (round_num,)
            )
            await self.db.execute(
                "DELETE FROM teams WHERE race_round = ?", (round_num,)
            )
            await self.db.execute(
                "DELETE FROM scores WHERE race_round = ?", (round_num,)
            )
            await self.db.execute(
                "DELETE FROM predictions WHERE race_round = ?", (round_num,)
            )
            await self.db.execute(
                "DELETE FROM prediction_scores WHERE race_round = ?", (round_num,)
            )
            await self.db.execute(
                "DELETE FROM survivor_picks WHERE race_round = ?", (round_num,)
            )
            await self.db.execute(
                "DELETE FROM transfers_log WHERE race_round = ?", (round_num,)
            )
