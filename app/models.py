from __future__ import annotations

from datetime import date, datetime
import uuid

from sqlalchemy import String, Text, SmallInteger, Date, DateTime
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.dialects.postgresql import ARRAY as PG_ARRAY, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Event(Base):
    __tablename__ = "events"

    event_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    jurisdiction: Mapped[str] = mapped_column(String)
    location_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    peak_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    activity: Mapped[str | None] = mapped_column(String, nullable=True)
    event_type: Mapped[str | None] = mapped_column(String, nullable=True)
    cause_primary: Mapped[str | None] = mapped_column(Text, nullable=True)
    contributing_factors: Mapped[list[str] | None] = mapped_column(PG_ARRAY(Text), nullable=True)
    n_fatalities: Mapped[int | None] = mapped_column(SmallInteger, nullable=True)
    date_event_start: Mapped[date | None] = mapped_column(Date, nullable=True)
    date_event_end: Mapped[date | None] = mapped_column(Date, nullable=True)
    date_of_death: Mapped[date | None] = mapped_column(Date, nullable=True)
    admin_area: Mapped[str | None] = mapped_column(String, nullable=True)
    iso_country: Mapped[str | None] = mapped_column(String, nullable=True)
    tz_local: Mapped[str | None] = mapped_column(String, nullable=True)
    phase: Mapped[str | None] = mapped_column(String, nullable=True)
    # Categorized names
    names_all: Mapped[list[str] | None] = mapped_column(PG_ARRAY(Text), nullable=True)
    names_deceased: Mapped[list[str] | None] = mapped_column(PG_ARRAY(Text), nullable=True)
    names_relatives: Mapped[list[str] | None] = mapped_column(PG_ARRAY(Text), nullable=True)
    names_responders: Mapped[list[str] | None] = mapped_column(PG_ARRAY(Text), nullable=True)
    names_spokespersons: Mapped[list[str] | None] = mapped_column(PG_ARRAY(Text), nullable=True)
    names_medics: Mapped[list[str] | None] = mapped_column(PG_ARRAY(Text), nullable=True)
    created_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class Source(Base):
    __tablename__ = "sources"

    source_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    event_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True))
    publisher: Mapped[str | None] = mapped_column(Text, nullable=True)
    article_title: Mapped[str | None] = mapped_column(Text, nullable=True)
    url: Mapped[str | None] = mapped_column(Text, nullable=True)
    date_published: Mapped[date | None] = mapped_column(Date, nullable=True)
    cleaned_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    quoted_evidence: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    summary_bullets: Mapped[list[str] | None] = mapped_column(PG_ARRAY(Text), nullable=True)
    date_scraped: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
