from __future__ import annotations

from sqlalchemy.engine import Engine
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)


EVENT_ALTERS = [
    # Simple text/varchar/date/datetime columns
    "ALTER TABLE events ADD COLUMN IF NOT EXISTS event_type VARCHAR",
    "ALTER TABLE events ADD COLUMN IF NOT EXISTS cause_primary TEXT",
    "ALTER TABLE events ADD COLUMN IF NOT EXISTS date_event_start DATE",
    "ALTER TABLE events ADD COLUMN IF NOT EXISTS date_event_end DATE",
    "ALTER TABLE events ADD COLUMN IF NOT EXISTS admin_area VARCHAR",
    "ALTER TABLE events ADD COLUMN IF NOT EXISTS iso_country VARCHAR",
    "ALTER TABLE events ADD COLUMN IF NOT EXISTS tz_local VARCHAR",
    "ALTER TABLE events ADD COLUMN IF NOT EXISTS phase VARCHAR",
    "ALTER TABLE events ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP",
    # Arrays of text
    "ALTER TABLE events ADD COLUMN IF NOT EXISTS contributing_factors TEXT[]",
    "ALTER TABLE events ADD COLUMN IF NOT EXISTS names_all TEXT[]",
    "ALTER TABLE events ADD COLUMN IF NOT EXISTS names_deceased TEXT[]",
    "ALTER TABLE events ADD COLUMN IF NOT EXISTS names_relatives TEXT[]",
    "ALTER TABLE events ADD COLUMN IF NOT EXISTS names_responders TEXT[]",
    "ALTER TABLE events ADD COLUMN IF NOT EXISTS names_spokespersons TEXT[]",
    "ALTER TABLE events ADD COLUMN IF NOT EXISTS names_medics TEXT[]",
]

SOURCE_ALTERS = [
    # JSONB and arrays of text
    "ALTER TABLE sources ADD COLUMN IF NOT EXISTS quoted_evidence JSONB",
    "ALTER TABLE sources ADD COLUMN IF NOT EXISTS summary_bullets TEXT[]",
]


def run_safe_migrations(engine: Engine) -> None:
    """Run idempotent ALTER TABLEs to align DB with models."""
    with engine.begin() as conn:
        for stmt in EVENT_ALTERS:
            logger.info("migrate: %s", stmt)
            conn.execute(text(stmt))
        for stmt in SOURCE_ALTERS:
            logger.info("migrate: %s", stmt)
            conn.execute(text(stmt))
    logger.info("migrations: done")
