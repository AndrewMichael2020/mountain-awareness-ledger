from pydantic import BaseModel, Field, HttpUrl
from typing import Optional, List, Literal
from datetime import date, datetime

class Evidence(BaseModel):
    field: str
    quote: str
    source_offset: Optional[int] = None

class SARSegment(BaseModel):
    agency: Optional[str]
    op_type: Literal["search","recovery","rescue"]
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    outcome: Optional[str] = None

class ExtractionPayload(BaseModel):
    jurisdiction: Optional[Literal["BC","AB","WA"]] = None
    location_name: Optional[str] = None
    peak_name: Optional[str] = None
    route_name: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    activity: Optional[Literal["alpinism","climbing","hiking","scrambling","ski-mountaineering","unknown"]] = None
    cause_primary: Optional[str] = None
    n_fatalities: Optional[int] = None
    n_injured: Optional[int] = None
    party_size: Optional[int] = None
    date_event_start: Optional[date] = None
    date_event_end: Optional[date] = None
    date_of_death: Optional[date] = None
    sar: List[SARSegment] = []
    summary_bullets: List[str] = []
    evidence: List[Evidence] = []
    extraction_conf: float = Field(0.0, ge=0.0, le=1.0)

class IngestRequest(BaseModel):
    url: HttpUrl
