from __future__ import annotations

import asyncio
import logging
import time

import httpx

from config import settings
from data.driver_mapping import number_to_id, update_mapping_from_openf1
from data.models import (
    DriverResult,
    PitStopResult,
    QualiResult,
    Race,
    RaceResult,
    RaceResultsBundle,
    SprintResult,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

class TTLCache:
    """Simple in-memory cache with per-key TTL."""

    def __init__(self) -> None:
        self._store: dict[str, tuple[float, object]] = {}

    def get(self, key: str) -> object | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        expires, value = entry
        if time.time() > expires:
            del self._store[key]
            return None
        return value

    def set(self, key: str, value: object, ttl: int) -> None:
        self._store[key] = (time.time() + ttl, value)


class CircuitBreaker:
    """Simple circuit breaker to avoid hammering a failing API."""

    def __init__(self, failure_threshold: int = 5, reset_timeout: int = 300) -> None:
        self._failures = 0
        self._threshold = failure_threshold
        self._reset_timeout = reset_timeout
        self._last_failure: float = 0
        self._open = False

    @property
    def is_open(self) -> bool:
        if self._open and time.time() - self._last_failure > self._reset_timeout:
            self._open = False
            self._failures = 0
        return self._open

    def record_success(self) -> None:
        self._failures = 0
        self._open = False

    def record_failure(self) -> None:
        self._failures += 1
        self._last_failure = time.time()
        if self._failures >= self._threshold:
            self._open = True
            logger.warning("Circuit breaker opened after %d failures", self._failures)


# ---------------------------------------------------------------------------
# JolpicaClient  (unchanged)
# ---------------------------------------------------------------------------

class JolpicaClient:
    """Client for the Jolpica-F1 API (Ergast replacement)."""

    def __init__(self) -> None:
        self.base_url = settings.JOLPICA_BASE_URL
        self._client = httpx.AsyncClient(timeout=30.0)
        self._cache = TTLCache()
        self._circuit_breaker = CircuitBreaker()

    async def close(self) -> None:
        await self._client.aclose()

    async def _get(self, path: str, cache_ttl: int = 3600) -> dict:
        if self._circuit_breaker.is_open:
            raise httpx.HTTPError(f"Circuit breaker open, skipping {path}")

        cache_key = f"jolpica:{path}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        url = f"{self.base_url}/{path}"
        for attempt in range(3):
            try:
                resp = await self._client.get(url)
                if resp.status_code == 429:
                    wait = 2 ** (attempt + 1)
                    logger.warning("Jolpica rate limited, waiting %ds", wait)
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                data = resp.json()
                self._cache.set(cache_key, data, cache_ttl)
                self._circuit_breaker.record_success()
                return data
            except httpx.HTTPStatusError as e:
                if e.response.status_code >= 500 and attempt < 2:
                    await asyncio.sleep(2 ** (attempt + 1))
                    continue
                self._circuit_breaker.record_failure()
                raise
            except (httpx.ConnectError, httpx.TimeoutException) as e:
                if attempt < 2:
                    logger.warning("Network error for %s (attempt %d): %s", path, attempt + 1, e)
                    await asyncio.sleep(2 ** (attempt + 1))
                    continue
                self._circuit_breaker.record_failure()
                raise
        raise httpx.HTTPError(f"Jolpica API failed after 3 retries for {path}")

    async def get_schedule(self, year: int) -> list[Race]:
        data = await self._get(f"{year}.json", cache_ttl=86400)
        races_data = (
            data.get("MRData", {})
            .get("RaceTable", {})
            .get("Races", [])
        )
        races = []
        for r in races_data:
            race_date = r.get("date", "")
            race_time = r.get("time", "14:00:00Z").rstrip("Z")
            quali = r.get("Qualifying", {})
            quali_date = quali.get("date", race_date)
            quali_time = quali.get("time", "14:00:00Z").rstrip("Z")
            sprint_data = r.get("Sprint")

            races.append(Race(
                round=int(r["round"]),
                name=r.get("raceName", ""),
                country=r.get("Circuit", {}).get("Location", {}).get("country", ""),
                circuit=r.get("Circuit", {}).get("circuitName", ""),
                qualifying_datetime=f"{quali_date}T{quali_time}",
                race_datetime=f"{race_date}T{race_time}",
                sprint=sprint_data is not None,
            ))
        return races

    async def get_race_results(self, year: int, round_num: int) -> list[RaceResult]:
        data = await self._get(f"{year}/{round_num}/results.json", cache_ttl=3600)
        races = (
            data.get("MRData", {})
            .get("RaceTable", {})
            .get("Races", [])
        )
        if not races:
            return []
        results_data = races[0].get("Results", [])
        results = []
        for r in results_data:
            driver_id = r.get("Driver", {}).get("driverId", "")
            grid = int(r.get("grid", 0))
            pos_text = r.get("position", "")
            finish_pos = int(pos_text) if pos_text.isdigit() else None
            status = r.get("status", "")
            dnf = status not in ("Finished", "") and not status.startswith("+")
            fastest = r.get("FastestLap", {}).get("rank", "") == "1"

            results.append(RaceResult(
                round=round_num,
                driver_id=driver_id,
                grid_position=grid,
                finish_position=finish_pos,
                dnf=dnf,
                fastest_lap=fastest,
            ))
        return results

    async def get_qualifying_results(self, year: int, round_num: int) -> list[dict]:
        data = await self._get(f"{year}/{round_num}/qualifying.json", cache_ttl=3600)
        races = (
            data.get("MRData", {})
            .get("RaceTable", {})
            .get("Races", [])
        )
        if not races:
            return []
        return [
            {
                "driver_id": r.get("Driver", {}).get("driverId", ""),
                "position": int(r.get("position", 0)),
                "q1": r.get("Q1"),
                "q2": r.get("Q2"),
                "q3": r.get("Q3"),
            }
            for r in races[0].get("QualifyingResults", [])
        ]

    async def get_sprint_results(self, year: int, round_num: int) -> list[dict] | None:
        try:
            data = await self._get(f"{year}/{round_num}/sprint.json", cache_ttl=3600)
            races = (
                data.get("MRData", {})
                .get("RaceTable", {})
                .get("Races", [])
            )
            if not races:
                return None
            return [
                {
                    "driver_id": r.get("Driver", {}).get("driverId", ""),
                    "position": int(r.get("position", 0)),
                    "grid": int(r.get("grid", 0)),
                    "status": r.get("status", ""),
                }
                for r in races[0].get("SprintResults", [])
            ]
        except httpx.HTTPStatusError:
            return None

    async def get_pit_stops(self, year: int, round_num: int) -> list[dict]:
        try:
            data = await self._get(f"{year}/{round_num}/pitstops.json", cache_ttl=3600)
            races = (
                data.get("MRData", {})
                .get("RaceTable", {})
                .get("Races", [])
            )
            if not races:
                return []
            return [
                {
                    "driver_id": p.get("driverId", ""),
                    "stop": int(p.get("stop", 0)),
                    "lap": int(p.get("lap", 0)),
                    "duration": p.get("duration", ""),
                }
                for p in races[0].get("PitStops", [])
            ]
        except httpx.HTTPStatusError:
            return []

    async def get_driver_standings(self, year: int) -> list[dict]:
        data = await self._get(f"{year}/driverStandings.json", cache_ttl=3600)
        standings_lists = (
            data.get("MRData", {})
            .get("StandingsTable", {})
            .get("StandingsLists", [])
        )
        if not standings_lists:
            return []
        return [
            {
                "driver_id": s.get("Driver", {}).get("driverId", ""),
                "position": int(s.get("position", 0)),
                "points": float(s.get("points", 0)),
                "wins": int(s.get("wins", 0)),
            }
            for s in standings_lists[0].get("DriverStandings", [])
        ]

    async def get_constructor_standings(self, year: int) -> list[dict]:
        data = await self._get(f"{year}/constructorStandings.json", cache_ttl=3600)
        standings_lists = (
            data.get("MRData", {})
            .get("StandingsTable", {})
            .get("StandingsLists", [])
        )
        if not standings_lists:
            return []
        return [
            {
                "constructor_id": s.get("Constructor", {}).get("constructorId", ""),
                "position": int(s.get("position", 0)),
                "points": float(s.get("points", 0)),
                "wins": int(s.get("wins", 0)),
            }
            for s in standings_lists[0].get("ConstructorStandings", [])
        ]


# ---------------------------------------------------------------------------
# OpenF1Client  (full rewrite with all endpoints)
# ---------------------------------------------------------------------------

class OpenF1Client:
    """Client for the OpenF1 API (fast post-session results)."""

    def __init__(self) -> None:
        self.base_url = settings.OPENF1_BASE_URL
        self._client = httpx.AsyncClient(timeout=30.0)
        self._cache = TTLCache()
        self._circuit_breaker = CircuitBreaker()

    async def close(self) -> None:
        await self._client.aclose()

    async def _get(
        self,
        endpoint: str,
        params: dict | None = None,
        cache_ttl: int = 600,
    ) -> list[dict]:
        if self._circuit_breaker.is_open:
            raise httpx.HTTPError(f"Circuit breaker open, skipping {endpoint}")

        cache_key = f"openf1:{endpoint}:{sorted((params or {}).items())}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        url = f"{self.base_url}/{endpoint}"
        for attempt in range(3):
            try:
                resp = await self._client.get(url, params=params or {})
                if resp.status_code == 429:
                    wait = 2 ** (attempt + 1)
                    logger.warning("OpenF1 rate limited, waiting %ds", wait)
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                data = resp.json()
                self._cache.set(cache_key, data, cache_ttl)
                self._circuit_breaker.record_success()
                return data
            except httpx.HTTPStatusError as e:
                if e.response.status_code >= 500 and attempt < 2:
                    await asyncio.sleep(2 ** (attempt + 1))
                    continue
                self._circuit_breaker.record_failure()
                raise
            except (httpx.ConnectError, httpx.TimeoutException) as e:
                if attempt < 2:
                    logger.warning("Network error for %s (attempt %d): %s", endpoint, attempt + 1, e)
                    await asyncio.sleep(2 ** (attempt + 1))
                    continue
                self._circuit_breaker.record_failure()
                raise
        raise httpx.HTTPError(f"OpenF1 API failed after 3 retries for {endpoint}")

    # -- Sessions ----------------------------------------------------------

    async def get_sessions(
        self,
        year: int,
        meeting_key: int | None = None,
    ) -> list[dict]:
        """Fetch sessions for a year (optionally filtered by meeting).

        Returns list of dicts with: session_key, session_name, date_start,
        date_end, meeting_key, etc.
        """
        params: dict[str, str] = {"year": str(year)}
        if meeting_key is not None:
            params["meeting_key"] = str(meeting_key)
        return await self._get("sessions", params=params, cache_ttl=600)

    # -- Session result (final classification) -----------------------------

    async def get_session_result(self, session_key: int) -> list[dict]:
        """Fetch final classification for a session.

        Returns list of dicts with: position, driver_number, points, etc.
        """
        return await self._get(
            "session_result",
            params={"session_key": str(session_key)},
            cache_ttl=600,
        )

    # -- Starting grid -----------------------------------------------------

    async def get_starting_grid(self, session_key: int) -> list[dict]:
        """Fetch the starting grid for a session.

        Returns list of dicts with: driver_number, position, qualifying_time.
        """
        return await self._get(
            "starting_grid",
            params={"session_key": str(session_key)},
            cache_ttl=600,
        )

    # -- Laps --------------------------------------------------------------

    async def get_laps(
        self,
        session_key: int,
        driver_number: int | None = None,
    ) -> list[dict]:
        """Fetch lap data for a session.

        Returns list of dicts with: driver_number, lap_number, lap_duration,
        is_pit_out_lap, etc.
        """
        params: dict[str, str] = {"session_key": str(session_key)}
        if driver_number is not None:
            params["driver_number"] = str(driver_number)
        return await self._get("laps", params=params, cache_ttl=600)

    # -- Pit stops ---------------------------------------------------------

    async def get_pit_stops(self, session_key: int) -> list[dict]:
        """Fetch pit stop data for a session.

        Returns list of dicts with: driver_number, lap_number, pit_duration,
        stop_duration (if available).
        """
        return await self._get(
            "pit",
            params={"session_key": str(session_key)},
            cache_ttl=600,
        )

    # -- Position ----------------------------------------------------------

    async def get_position(
        self,
        session_key: int,
        driver_number: int | None = None,
    ) -> list[dict]:
        """Fetch position updates during a session."""
        params: dict[str, str] = {"session_key": str(session_key)}
        if driver_number is not None:
            params["driver_number"] = str(driver_number)
        return await self._get("position", params=params, cache_ttl=300)

    # -- Drivers -----------------------------------------------------------

    async def get_drivers(self, session_key: int) -> list[dict]:
        """Fetch driver information for a session.

        Returns list of dicts with: driver_number, full_name, name_acronym,
        team_name, team_colour, country_code.
        """
        return await self._get(
            "drivers",
            params={"session_key": str(session_key)},
            cache_ttl=600,
        )

    # -- Meetings ----------------------------------------------------------

    async def get_meetings(self, year: int) -> list[dict]:
        """Fetch all meetings (race weekends) for a year.

        Returns list of dicts with: meeting_key, meeting_name, country_name,
        date_start.
        """
        return await self._get(
            "meetings",
            params={"year": str(year)},
            cache_ttl=3600,
        )


# ---------------------------------------------------------------------------
# F1DataService  (facade with two-phase logic)
# ---------------------------------------------------------------------------

class F1DataService:
    """Facade combining Jolpica and OpenF1 data sources.

    Two-phase result fetching:
      Phase 1 (``get_fast_race_results``): Pull data from OpenF1 as soon as
        the session ends (~35 min after race).
      Phase 2 (``get_validated_results``): Cross-validate against Jolpica on
        Monday for official confirmation.
    """

    def __init__(self) -> None:
        self.jolpica = JolpicaClient()
        self.openf1 = OpenF1Client()
        self._meetings_cache: dict[int, list[dict]] = {}

    async def close(self) -> None:
        await self.jolpica.close()
        await self.openf1.close()

    # -- Schedule ----------------------------------------------------------

    async def get_schedule(self, year: int | None = None) -> list[Race]:
        """Return the season schedule from Jolpica."""
        return await self.jolpica.get_schedule(year or settings.SEASON_YEAR)

    # -- Meeting-key mapping -----------------------------------------------

    async def get_meeting_key(
        self,
        year: int,
        race_round: int,
    ) -> int | None:
        """Map a Jolpica round number to an OpenF1 meeting_key.

        The mapping works by aligning the Jolpica schedule (ordered by round)
        with the OpenF1 meetings list (ordered by date).  We match by date
        first, falling back to ordinal position.
        """
        # Load Jolpica schedule
        schedule = await self.jolpica.get_schedule(year)
        target_race = None
        for race in schedule:
            if race.round == race_round:
                target_race = race
                break
        if target_race is None:
            logger.warning("Round %d not found in Jolpica schedule", race_round)
            return None

        # Load OpenF1 meetings (cached per year)
        if year not in self._meetings_cache:
            self._meetings_cache[year] = await self.openf1.get_meetings(year)
        meetings = self._meetings_cache[year]

        if not meetings:
            logger.warning("No OpenF1 meetings found for %d", year)
            return None

        # Try matching by date (compare date portion of race_datetime)
        race_date_str = target_race.race_datetime[:10]  # "YYYY-MM-DD"
        for meeting in meetings:
            meeting_date = meeting.get("date_start", "")[:10]
            if meeting_date == race_date_str:
                return meeting.get("meeting_key")

        # Fallback: try matching by name similarity
        race_name_lower = target_race.name.lower()
        for meeting in meetings:
            meeting_name = meeting.get("meeting_name", "").lower()
            country_name = meeting.get("country_name", "").lower()
            if (
                target_race.country.lower() in meeting_name
                or target_race.country.lower() == country_name
                or race_name_lower in meeting_name
            ):
                return meeting.get("meeting_key")

        # Last resort: match by ordinal position (round N = Nth meeting)
        sorted_meetings = sorted(meetings, key=lambda m: m.get("date_start", ""))
        if 1 <= race_round <= len(sorted_meetings):
            return sorted_meetings[race_round - 1].get("meeting_key")

        logger.warning(
            "Could not map round %d to an OpenF1 meeting_key", race_round
        )
        return None

    # -- Phase 1: Fast results from OpenF1 ---------------------------------

    async def get_fast_race_results(
        self,
        year: int,
        race_round: int,
    ) -> RaceResultsBundle | None:
        """Phase 1: Fetch fast results from OpenF1 (~35 min after race).

        Returns a ``RaceResultsBundle`` with source="openf1", or ``None``
        if the session has not completed or critical data is unavailable.
        """
        # 1. Resolve meeting_key
        meeting_key = await self.get_meeting_key(year, race_round)
        if meeting_key is None:
            logger.warning(
                "Cannot fetch fast results: no meeting_key for round %d",
                race_round,
            )
            return None

        # 2. Find the Race session
        sessions = await self.openf1.get_sessions(year, meeting_key=meeting_key)
        race_session = None
        quali_session = None
        sprint_session = None

        for s in sessions:
            name = s.get("session_name", "")
            if name == "Race":
                race_session = s
            elif name == "Qualifying":
                quali_session = s
            elif name == "Sprint":
                sprint_session = s

        if race_session is None:
            logger.info("No Race session found for meeting_key %d", meeting_key)
            return None

        session_key = race_session["session_key"]

        # 3. Check session completed
        if race_session.get("date_end") is None:
            logger.info(
                "Race session %d has not completed yet (date_end is None)",
                session_key,
            )
            return None

        # 4. Parallel fetch of race data
        (
            session_result_or_exc,
            starting_grid_or_exc,
            laps_or_exc,
            pit_stops_or_exc,
            drivers_or_exc,
        ) = await asyncio.gather(
            self.openf1.get_session_result(session_key),
            self.openf1.get_starting_grid(session_key),
            self.openf1.get_laps(session_key),
            self.openf1.get_pit_stops(session_key),
            self.openf1.get_drivers(session_key),
            return_exceptions=True,
        )

        # Unwrap results, treating exceptions as empty data
        if isinstance(session_result_or_exc, Exception):
            logger.error(
                "Failed to fetch session_result for session %d: %s",
                session_key,
                session_result_or_exc,
            )
            session_result: list[dict] = []
        else:
            session_result = session_result_or_exc

        if isinstance(starting_grid_or_exc, Exception):
            logger.warning(
                "Failed to fetch starting_grid for session %d: %s",
                session_key,
                starting_grid_or_exc,
            )
            starting_grid: list[dict] = []
        else:
            starting_grid = starting_grid_or_exc

        if isinstance(laps_or_exc, Exception):
            logger.warning(
                "Failed to fetch laps for session %d: %s",
                session_key,
                laps_or_exc,
            )
            laps: list[dict] = []
        else:
            laps = laps_or_exc

        if isinstance(pit_stops_or_exc, Exception):
            logger.warning(
                "Failed to fetch pit stops for session %d: %s",
                session_key,
                pit_stops_or_exc,
            )
            pit_stops_data: list[dict] = []
        else:
            pit_stops_data = pit_stops_or_exc

        if isinstance(drivers_or_exc, Exception):
            logger.warning(
                "Failed to fetch drivers for session %d: %s",
                session_key,
                drivers_or_exc,
            )
            drivers_data: list[dict] = []
        else:
            drivers_data = drivers_or_exc

        # Return None if critical data is missing
        if len(session_result) < 15:
            logger.warning(
                "session_result has only %d entries for session %d "
                "(need at least 15); results likely not ready",
                len(session_result),
                session_key,
            )
            return None

        if len(starting_grid) < 15:
            logger.warning(
                "starting_grid has only %d entries for session %d "
                "(need at least 15); data likely incomplete",
                len(starting_grid),
                session_key,
            )
            # Don't return None - grid data is supplementary, log warning only

        # 5. Update driver number -> id mapping from OpenF1 drivers data
        if drivers_data:
            update_mapping_from_openf1(drivers_data)

        # Build lookup: driver_number -> team_name from drivers data
        driver_teams: dict[int, str] = {}
        for d in drivers_data:
            num = d.get("driver_number")
            if num is not None:
                driver_teams[num] = d.get("team_name", "")

        # Build grid position lookup from starting_grid
        grid_positions: dict[int, int] = {}
        for g in starting_grid:
            num = g.get("driver_number")
            pos = g.get("position")
            if num is not None and pos is not None:
                grid_positions[num] = int(pos)

        # 6. Determine fastest lap
        # Filter out pit-out laps, then find minimum lap_duration
        valid_laps = [
            lap
            for lap in laps
            if lap.get("lap_duration") is not None
            and not lap.get("is_pit_out_lap", False)
        ]
        fastest_lap_driver: int | None = None
        if valid_laps:
            best_lap = min(valid_laps, key=lambda x: x["lap_duration"])
            fastest_lap_driver = best_lap.get("driver_number")

        # 7. Build DriverResult list
        # Also track which drivers are in session_result for DNF detection
        result_driver_numbers = set()
        driver_results: list[DriverResult] = []

        for entry in session_result:
            drv_num = entry.get("driver_number")
            if drv_num is None:
                continue
            result_driver_numbers.add(drv_num)

            position = entry.get("position")
            driver_id = number_to_id(drv_num) or f"driver_{drv_num}"
            grid = grid_positions.get(drv_num, 0)
            team = driver_teams.get(drv_num, "")

            # Determine status: if position is None or >20, likely DNF
            if position is None:
                status = "Retired"
            else:
                status = "Finished"

            fl_rank = 1 if drv_num == fastest_lap_driver else 0

            driver_results.append(DriverResult(
                position=position,
                grid=grid,
                driver_id=driver_id,
                driver_number=drv_num,
                team=team,
                status=status,
                fastest_lap_rank=fl_rank,
            ))

        # Check for DNFs: drivers in starting_grid but not in session_result
        for g in starting_grid:
            drv_num = g.get("driver_number")
            if drv_num is not None and drv_num not in result_driver_numbers:
                driver_id = number_to_id(drv_num) or f"driver_{drv_num}"
                team = driver_teams.get(drv_num, "")
                grid = grid_positions.get(drv_num, 0)
                driver_results.append(DriverResult(
                    position=None,
                    grid=grid,
                    driver_id=driver_id,
                    driver_number=drv_num,
                    team=team,
                    status="Retired",
                    fastest_lap_rank=0,
                ))

        # 8. Build PitStopResult list
        pit_stop_results: list[PitStopResult] = []
        # Track stop numbers per driver
        driver_stop_counts: dict[int, int] = {}
        for ps in pit_stops_data:
            drv_num = ps.get("driver_number")
            if drv_num is None:
                continue
            driver_stop_counts[drv_num] = driver_stop_counts.get(drv_num, 0) + 1
            driver_id = number_to_id(drv_num) or f"driver_{drv_num}"

            pit_duration = ps.get("pit_duration")
            stop_duration = ps.get("stop_duration")

            pit_stop_results.append(PitStopResult(
                driver_number=drv_num,
                driver_id=driver_id,
                stop_number=driver_stop_counts[drv_num],
                duration_seconds=float(stop_duration) if stop_duration is not None else 0.0,
                pit_duration=float(pit_duration) if pit_duration is not None else 0.0,
            ))

        # 9. Fetch Qualifying session results if available
        qualifying_results: list[QualiResult] = []
        if quali_session is not None:
            quali_key = quali_session["session_key"]
            try:
                quali_data = await self.openf1.get_session_result(quali_key)
                for qr in quali_data:
                    drv_num = qr.get("driver_number")
                    if drv_num is None:
                        continue
                    driver_id = number_to_id(drv_num) or f"driver_{drv_num}"
                    qualifying_results.append(QualiResult(
                        driver_id=driver_id,
                        driver_number=drv_num,
                        position=qr.get("position", 0) or 0,
                    ))
            except Exception as exc:
                logger.warning(
                    "Failed to fetch qualifying results for session %d: %s",
                    quali_key,
                    exc,
                )

        # 10. Fetch Sprint session results if available
        sprint_results: list[SprintResult] = []
        if sprint_session is not None:
            sprint_key = sprint_session["session_key"]
            try:
                sprint_sr, sprint_grid = await asyncio.gather(
                    self.openf1.get_session_result(sprint_key),
                    self.openf1.get_starting_grid(sprint_key),
                    return_exceptions=True,
                )
                if isinstance(sprint_sr, Exception):
                    logger.warning(
                        "Failed to fetch sprint session_result: %s", sprint_sr
                    )
                    sprint_sr = []
                if isinstance(sprint_grid, Exception):
                    logger.warning(
                        "Failed to fetch sprint starting_grid: %s", sprint_grid
                    )
                    sprint_grid = []

                sprint_grid_map: dict[int, int] = {}
                for sg in sprint_grid:
                    num = sg.get("driver_number")
                    pos = sg.get("position")
                    if num is not None and pos is not None:
                        sprint_grid_map[num] = int(pos)

                for sr in sprint_sr:
                    drv_num = sr.get("driver_number")
                    if drv_num is None:
                        continue
                    driver_id = number_to_id(drv_num) or f"driver_{drv_num}"
                    position = sr.get("position")
                    sprint_results.append(SprintResult(
                        driver_id=driver_id,
                        driver_number=drv_num,
                        position=position if position is not None else 0,
                        grid=sprint_grid_map.get(drv_num, 0),
                        status="Finished" if position is not None else "Retired",
                    ))
            except Exception as exc:
                logger.warning(
                    "Failed to fetch sprint results for session %d: %s",
                    sprint_key,
                    exc,
                )

        # 11. Return bundle
        return RaceResultsBundle(
            race_round=race_round,
            session_key=session_key,
            results=driver_results,
            pit_stops=pit_stop_results,
            qualifying=qualifying_results,
            sprint=sprint_results,
            source="openf1",
            needs_rescore=False,
        )

    # -- Phase 2: Cross-validation from Jolpica ----------------------------

    async def get_validated_results(
        self,
        year: int,
        race_round: int,
    ) -> RaceResultsBundle | None:
        """Phase 2: Cross-validate results against Jolpica (Monday).

        Returns a ``RaceResultsBundle`` with source="jolpica" and
        ``needs_rescore=True`` if discrepancies are found compared to what
        was previously scored.  Returns ``None`` if Jolpica data is not yet
        available or there are no discrepancies.
        """
        # 1. Fetch Jolpica race results
        jolpica_results = await self.jolpica.get_race_results(year, race_round)
        if not jolpica_results:
            logger.info(
                "Jolpica has no results for %d round %d yet", year, race_round
            )
            return None

        # 2. Build DriverResult list from Jolpica data
        driver_results: list[DriverResult] = []
        for rr in jolpica_results:
            status = "Finished"
            if rr.dnf:
                status = "Retired"
            fl_rank = 1 if rr.fastest_lap else 0
            driver_results.append(DriverResult(
                position=rr.finish_position,
                grid=rr.grid_position,
                driver_id=rr.driver_id,
                driver_number=0,  # Jolpica doesn't provide driver_number
                team="",
                status=status,
                fastest_lap_rank=fl_rank,
            ))

        # Fetch qualifying and pit stops from Jolpica
        quali_raw, sprint_raw, pitstops_raw = await asyncio.gather(
            self.jolpica.get_qualifying_results(year, race_round),
            self.jolpica.get_sprint_results(year, race_round),
            self.jolpica.get_pit_stops(year, race_round),
            return_exceptions=True,
        )

        # Build QualiResult list
        qualifying_results: list[QualiResult] = []
        if not isinstance(quali_raw, Exception) and quali_raw:
            for qr in quali_raw:
                qualifying_results.append(QualiResult(
                    driver_id=qr["driver_id"],
                    position=qr.get("position", 0),
                    q1=qr.get("q1"),
                    q2=qr.get("q2"),
                    q3=qr.get("q3"),
                ))

        # Build SprintResult list
        sprint_results: list[SprintResult] = []
        if not isinstance(sprint_raw, Exception) and sprint_raw:
            for sr in sprint_raw:
                sprint_results.append(SprintResult(
                    driver_id=sr["driver_id"],
                    position=sr.get("position", 0),
                    grid=sr.get("grid", 0),
                    status=sr.get("status", "Finished"),
                ))

        # Build PitStopResult list
        pit_stop_results: list[PitStopResult] = []
        if not isinstance(pitstops_raw, Exception) and pitstops_raw:
            for ps in pitstops_raw:
                # Parse duration string (e.g. "23.456") to float
                dur_str = ps.get("duration", "0")
                try:
                    dur_secs = float(dur_str) if dur_str else 0.0
                except (ValueError, TypeError):
                    dur_secs = 0.0
                pit_stop_results.append(PitStopResult(
                    driver_number=0,
                    driver_id=ps.get("driver_id", ""),
                    stop_number=ps.get("stop", 1),
                    duration_seconds=dur_secs,
                ))

        # 3 & 4. Always return the bundle with needs_rescore=True so the
        # caller can compare against what is stored in the DB and decide
        # whether re-scoring is needed.
        return RaceResultsBundle(
            race_round=race_round,
            session_key=None,
            results=driver_results,
            pit_stops=pit_stop_results,
            qualifying=qualifying_results,
            sprint=sprint_results,
            source="jolpica",
            needs_rescore=True,
        )

    # -- Backward-compatible Jolpica methods --------------------------------

    async def get_race_results(
        self,
        year: int,
        round_num: int,
    ) -> list[RaceResult]:
        """Fetch race results from Jolpica (backward compatibility)."""
        results = await self.jolpica.get_race_results(year, round_num)
        if results:
            return results
        logger.info("Jolpica has no results for round %d yet", round_num)
        return []

    async def get_qualifying_results(
        self,
        year: int,
        round_num: int,
    ) -> list[dict]:
        """Fetch qualifying results from Jolpica (backward compatibility)."""
        return await self.jolpica.get_qualifying_results(year, round_num)

    async def get_sprint_results(
        self,
        year: int,
        round_num: int,
    ) -> list[dict] | None:
        """Fetch sprint results from Jolpica (backward compatibility)."""
        return await self.jolpica.get_sprint_results(year, round_num)

    async def get_pit_stops(self, year: int, round_num: int) -> list[dict]:
        """Fetch pit stops from Jolpica (backward compatibility)."""
        return await self.jolpica.get_pit_stops(year, round_num)
