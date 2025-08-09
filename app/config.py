from __future__ import annotations

import os
from typing import Optional
from pathlib import Path

try:
    from dotenv import load_dotenv  # type: ignore
except Exception:
    load_dotenv = None  # type: ignore


def _load_env() -> None:
    root = Path(__file__).resolve().parents[1]
    if load_dotenv:
        load_dotenv(dotenv_path=root / ".env", override=False)
        load_dotenv(dotenv_path=root / ".env.local", override=True)


# Load at import time
_load_env()


class Settings:
    def __init__(self) -> None:
        self.DATABASE_URL = os.getenv(
            "DATABASE_URL",
            "postgresql+psycopg2://postgres:postgres@localhost:5432/alpine_ledger",
        )
        self.ENV = os.getenv("ENV", "dev")
        self.DATA_DIR = os.getenv("DATA_DIR", str(Path.cwd() / "data"))
        self.TAVILY_API_KEY = os.getenv("TAVILY_API_KEY")
        self.OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")


def get_settings() -> Settings:
    return Settings()


def get_tavily_api_key() -> Optional[str]:
    return os.getenv("TAVILY_API_KEY")
