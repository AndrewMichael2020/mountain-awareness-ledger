from __future__ import annotations

from datetime import datetime, date as date_type
import uuid
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from .models import Event, Source


def create_event(db: Session, *, jurisdiction: str, location_name: Optional[str] = None,
                 peak_name: Optional[str] = None, activity: Optional[str] = None,
                 n_fatalities: Optional[int] = None, date_of_death: Optional[date_type] = None) -> Event:
    e = Event(
        event_id=uuid.uuid4(),
        jurisdiction=jurisdiction,
        location_name=location_name,
        peak_name=peak_name,
        activity=activity,
        n_fatalities=n_fatalities,
        date_of_death=date_of_death,
        created_at=datetime.utcnow(),
    )
    db.add(e)
    db.commit()
    db.refresh(e)
    return e


def get_source_by_url(db: Session, url: str) -> Optional[Source]:
    return db.execute(select(Source).where(Source.url == url)).scalar_one_or_none()


def create_source(db: Session, *, event_id: uuid.UUID, url: str,
                  publisher: Optional[str] = None, article_title: Optional[str] = None,
                  date_published: Optional[date_type] = None,
                  cleaned_text: Optional[str] = None, date_scraped: Optional[datetime] = None) -> Source:
    s = Source(
        source_id=uuid.uuid4(),
        event_id=event_id,
        url=url,
        publisher=publisher,
        article_title=article_title,
        date_published=date_published,
        cleaned_text=cleaned_text,
        date_scraped=date_scraped,
    )
    db.add(s)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        existing = get_source_by_url(db, url)
        if existing:
            return existing
        raise
    db.refresh(s)
    return s


def get_event_with_sources(db: Session, event_id: uuid.UUID) -> tuple[Event | None, list[Source]]:
    e = db.get(Event, event_id)
    if not e:
        return None, []
    sources = db.execute(
        select(Source).where(Source.event_id == event_id).order_by(Source.date_published.is_(None), Source.date_published.desc())
    ).scalars().all()
    return e, sources
