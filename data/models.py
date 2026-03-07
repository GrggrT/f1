from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class Driver(BaseModel):
    id: str
    name: str
    team: str
    price: float
    is_active: bool = True


class Constructor(BaseModel):
    id: str
    name: str
    price: float


class UserTeam(BaseModel):
    user_id: int
    username: str
    race_round: int
    drivers: list[str]
    constructor: str
    turbo_driver: str
    budget_remaining: float
    chips_used: list[str] = Field(default_factory=list)


class RaceResult(BaseModel):
    round: int
    driver_id: str
    grid_position: int
    finish_position: int | None = None
    dnf: bool = False
    fastest_lap: bool = False
    points_scored: float = 0.0


class UserScore(BaseModel):
    user_id: int
    race_round: int
    fantasy_points: float
    breakdown: dict


class Race(BaseModel):
    round: int
    name: str
    country: str
    circuit: str
    qualifying_datetime: str
    race_datetime: str
    sprint: bool = False


class Prediction(BaseModel):
    user_id: int
    race_round: int
    questions: dict[str, dict]  # {question_id: {answer: bool, confidence: int}}


class SurvivorPick(BaseModel):
    user_id: int
    race_round: int
    driver_id: str
    survived: bool | None = None


class DriverResult(BaseModel):
    """Single driver result from a race session."""
    position: int | None = None
    grid: int = 0
    driver_id: str = ""
    driver_number: int = 0
    team: str = ""
    status: str = "Finished"  # "Finished", "+1 Lap", "Retired", "Disqualified", etc.
    fastest_lap_rank: int = 0  # 1 = fastest lap of race


class PitStopResult(BaseModel):
    """Pit stop data for constructor scoring."""
    driver_number: int
    driver_id: str = ""
    stop_number: int = 1
    duration_seconds: float = 0.0  # stationary time (stop_duration from OpenF1)
    pit_duration: float = 0.0  # total pit lane time


class QualiResult(BaseModel):
    """Qualifying result."""
    driver_id: str
    driver_number: int = 0
    position: int = 0
    q1: str | None = None
    q2: str | None = None
    q3: str | None = None


class SprintResult(BaseModel):
    """Sprint race result."""
    driver_id: str
    driver_number: int = 0
    position: int = 0
    grid: int = 0
    status: str = "Finished"


class RaceResultsBundle(BaseModel):
    """Aggregated results from all data sources for scoring."""
    race_round: int
    session_key: int | None = None
    results: list[DriverResult] = Field(default_factory=list)
    pit_stops: list[PitStopResult] = Field(default_factory=list)
    qualifying: list[QualiResult] = Field(default_factory=list)
    sprint: list[SprintResult] = Field(default_factory=list)
    source: str = "jolpica"  # "openf1" or "jolpica"
    needs_rescore: bool = False
