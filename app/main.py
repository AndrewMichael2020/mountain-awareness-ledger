from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy import text, select
from sqlalchemy.orm import Session
from datetime import datetime, date as date_type
from typing import Literal

from .db import get_db, engine
from .models import Event
from .repo import create_event, create_source, get_event_with_sources, get_source_by_url
from .schemas import IngestRequest, EventOut, SourceOut
from .pipeline.fetcher import fetch_url
from .pipeline.cleaner import clean_html

app = FastAPI(title="Alpine Disasters: Agentic Ledger API", version="0.1")

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/db/health")
def db_health():
    try:
        with engine.connect() as conn:
            pg_version = conn.execute(text("SELECT version()"))
            pg_version = pg_version.scalar()
            try:
                postgis = conn.execute(text("SELECT postgis_version()"))
                postgis = postgis.scalar()
            except Exception:
                postgis = None
    except Exception as e:
        return {"ok": False, "error": str(e)}
    return {"ok": True, "pg_version": pg_version, "postgis": postgis}

@app.post("/discover")
def discover(
    jurisdiction: Literal["BC", "AB", "WA"] = Query(..., description="One of: BC, AB, WA"),
    years: int = Query(10, ge=1, le=50, description="Lookback window in years"),
):
    # TODO: enqueue discovery job
    return {"status": "queued", "jurisdiction": jurisdiction, "years": years}

@app.post("/ingest")
def ingest(payload: IngestRequest, db: Session = Depends(get_db)):
    # If this URL already exists, short-circuit
    existing = get_source_by_url(db, str(payload.url))
    if existing:
        return {"status": "exists", "event_id": str(existing.event_id), "source_id": str(existing.source_id)}

    try:
        html, final_url = fetch_url(str(payload.url))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"fetch failed: {e}")

    text_body, meta = clean_html(html, final_url)

    # Prefer client-supplied date; else try metadata date (ISO YYYY-MM-DD)
    pub_date = payload.date_published
    if not pub_date and meta and isinstance(meta.get("date"), str):
        try:
            pub_date = date_type.fromisoformat(meta["date"][0:10])
        except Exception:
            pub_date = None

    e = create_event(db, jurisdiction="BC")
    s = create_source(
        db,
        event_id=e.event_id,
        url=final_url or str(payload.url),
        publisher=payload.publisher or None,
        article_title=payload.article_title or (meta.get("title") if meta else None),
        date_published=pub_date,
        cleaned_text=text_body,
        date_scraped=datetime.utcnow(),
    )
    return {"status": "created", "event_id": str(e.event_id), "source_id": str(s.source_id)}

@app.get("/events")
def list_events(db: Session = Depends(get_db)):
    items: list[Event] = db.execute(
        select(Event).order_by(Event.created_at.desc()).limit(50)
    ).scalars().all()
    return {
        "items": [
            {
                "event_id": str(e.event_id),
                "jurisdiction": e.jurisdiction,
                "location_name": e.location_name,
                "peak_name": e.peak_name,
                "activity": e.activity,
                "n_fatalities": e.n_fatalities,
                "date_of_death": e.date_of_death.isoformat() if e.date_of_death else None,
                "created_at": e.created_at.isoformat() if e.created_at else None,
            }
            for e in items
        ]
    }

@app.get("/events/{event_id}", response_model=EventOut)
def get_event(event_id: str, db: Session = Depends(get_db)):
    from uuid import UUID

    try:
        uid = UUID(event_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid event_id")

    e, sources = get_event_with_sources(db, uid)
    if not e:
        raise HTTPException(status_code=404, detail="Not found")

    return EventOut(
        event_id=str(e.event_id),
        jurisdiction=e.jurisdiction,
        location_name=e.location_name,
        peak_name=e.peak_name,
        activity=e.activity,
        n_fatalities=e.n_fatalities,
        date_of_death=e.date_of_death,
        created_at=e.created_at,
        sources=[
            SourceOut(
                source_id=str(s.source_id),
                url=s.url or "",
                publisher=s.publisher,
                article_title=s.article_title,
                date_published=s.date_published,
            )
            for s in sources
        ],
    )

@app.get("/export.csv")
def export_csv():
    # TODO: stream CSV export
    return {"todo": True}
