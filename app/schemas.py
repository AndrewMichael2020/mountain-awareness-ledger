from __future__ import annotations
from datetime import date, datetime
from pydantic import BaseModel, HttpUrl
from typing import Optional, List


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


class RawIngestRequest(BaseModel):
    url: HttpUrl
    clean_text: str
    publisher: Optional[str] = None
    article_title: Optional[str] = None
    date_published: Optional[date] = None


class TavilyResult(BaseModel):
    url: HttpUrl
    raw_content: str


class TavilyIngestRequest(BaseModel):
    results: List[TavilyResult]
    failed_results: Optional[list] = None
    response_time: Optional[float] = None


class BatchIngestRequest(BaseModel):
    urls: List[HttpUrl]
    publisher: Optional[str] = None
    article_title: Optional[str] = None
    date_published: Optional[date] = None
