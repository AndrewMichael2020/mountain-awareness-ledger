from __future__ import annotations
from datetime import date, datetime
from pydantic import BaseModel, HttpUrl
from typing import Optional


class IngestRequest(BaseModel):
    url: HttpUrl
    publisher: Optional[str] = None
    article_title: Optional[str] = None
    date_published: Optional[date] = None


class SourceOut(BaseModel):
    source_id: str
    url: str
    publisher: Optional[str] = None
    article_title: Optional[str] = None
    date_published: Optional[date] = None


class EventOut(BaseModel):
    event_id: str
    jurisdiction: str
    location_name: Optional[str] = None
    peak_name: Optional[str] = None
    activity: Optional[str] = None
    n_fatalities: Optional[int] = None
    date_of_death: Optional[date] = None
    created_at: Optional[datetime] = None
    sources: list[SourceOut] = []
