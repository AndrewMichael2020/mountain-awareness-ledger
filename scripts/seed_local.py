#!/usr/bin/env python
from datetime import date
import uuid
from sqlalchemy import text

from app.db import engine


def main():
    event_id = uuid.uuid4()
    d = date.today()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE EXTENSION IF NOT EXISTS postgis;
                INSERT INTO events (
                  event_id, jurisdiction, iso_country, admin_area,
                  location_name, peak_name, event_type, activity,
                  n_fatalities, date_event_start, date_event_end, date_of_death,
                  tz_local, extraction_conf, created_at, updated_at
                ) VALUES (
                  :event_id, 'BC', 'CA', 'British Columbia',
                  'Test Peak, Demo Park', 'Test Peak', 'fatality', 'alpinism',
                  1, :d, :d, :d,
                  'America/Vancouver', 0.9, now(), now()
                ) ON CONFLICT (event_id) DO NOTHING;
                """
            ),
            {"event_id": str(event_id), "d": d},
        )
    print(f"Seeded event {event_id}")


if __name__ == "__main__":
    main()
