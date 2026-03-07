from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    BOT_TOKEN: str
    DB_PATH: str = "data/fantasy.db"
    ADMIN_IDS: list[int] = []
    GROUP_CHAT_ID: int = 0
    JOLPICA_BASE_URL: str = "https://api.jolpi.ca/ergast/f1"
    OPENF1_BASE_URL: str = "https://api.openf1.org/v1"
    SEASON_YEAR: int = 2026
    TOTAL_BUDGET: float = 100.0
    FREE_TRANSFERS_PER_RACE: int = 2
    EXTRA_TRANSFER_PENALTY: int = 10
    MAX_POLL_ATTEMPTS: int = 40
    POLL_INTERVAL: int = 300

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
