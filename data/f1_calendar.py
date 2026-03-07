from __future__ import annotations

import logging

from config import settings
from data.api_client import F1DataService
from data.database import Database
from data.models import Race

logger = logging.getLogger(__name__)


async def load_calendar(db: Database, f1_data: F1DataService) -> list[Race]:
    """Load the F1 calendar from Jolpica API and save to database."""
    races = await f1_data.get_schedule(settings.SEASON_YEAR)
    for race in races:
        await db.save_race(race)
    logger.info("Loaded %d races for %d season", len(races), settings.SEASON_YEAR)
    return races
