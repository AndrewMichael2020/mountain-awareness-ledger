from functools import lru_cache
from pathlib import Path
from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

# Load .env.local first (if exists), then .env
repo_root = Path(__file__).resolve().parents[1]
load_dotenv(repo_root / ".env.local", override=True)
load_dotenv(repo_root / ".env", override=False)


class Settings(BaseSettings):
    OPENAI_API_KEY: str | None = None
    TAVILY_API_KEY: str | None = None
    DATABASE_URL: str
    GCP_PROJECT_ID: str | None = None
    GCP_SQL_INSTANCE: str | None = None  # e.g., project:region:instance
    GCP_BUCKET: str | None = None
    ENV: str = "dev"

    model_config = SettingsConfigDict(env_file=None, extra="ignore")


@lru_cache
def get_settings() -> Settings:
    return Settings()
