from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy import text, select
from sqlalchemy.orm import Session
from datetime import datetime, date as date_type
from typing import Literal, Optional

from .db import get_db, engine
from .models import Event
from .repo import create_event, create_source, get_event_with_sources, get_source_by_url, update_event_fields, update_source_annotations, insert_sar_segments, get_sar_ops, get_latest_source_for_event, delete_sar_ops_for_event
from .schemas import IngestRequest, EventOut, SourceOut, RawIngestRequest, TavilyIngestRequest, BatchIngestRequest
from .pipeline.fetcher import fetch_url
from .pipeline.cleaner import clean_html
from .pipeline.geocoder import geocode_from_extracted
from .pipeline.graph import run_ingest_graph_url, run_ingest_graph_raw
from .pipeline.discover import SearchParams, run_discovery
from alpine.extract_det import extract_core_fields

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
    activity: Optional[str] = Query(None, description="activity filter: alpinism, climbing, hiking, ski-mountaineering"),
    mode: Literal["broad", "allowlist", "both"] = Query("both", description="Search breadth"),
):
    params = SearchParams(jurisdiction=jurisdiction, years=years, activity=activity, mode=mode)
    out = run_discovery(params)
    return {"status": "ok", **out}

@app.post("/ingest")
def ingest(payload: IngestRequest, db: Session = Depends(get_db)):
    return run_ingest_graph_url(
        db,
        url=str(payload.url),
        publisher=payload.publisher or None,
        article_title=payload.article_title or None,
        pub_date=payload.date_published or None,
    )


@app.post("/ingest/raw")
def ingest_raw(payload: RawIngestRequest, db: Session = Depends(get_db)):
    return run_ingest_graph_raw(
        db,
        url=str(payload.url),
        clean_text=payload.clean_text,
        publisher=payload.publisher or None,
        article_title=payload.article_title or None,
        pub_date=payload.date_published or None,
    )


@app.post("/ingest/tavily")
def ingest_tavily(payload: TavilyIngestRequest, db: Session = Depends(get_db)):
    if not payload.results:
        raise HTTPException(status_code=400, detail="no results")
    first = payload.results[0]
    # Reuse raw ingest path
    body = RawIngestRequest(url=first.url, clean_text=first.raw_content)
    return ingest_raw(body, db)

@app.post("/ingest/batch")
def ingest_batch(payload: BatchIngestRequest, db: Session = Depends(get_db)):
    results = []
    for u in payload.urls:
        out = run_ingest_graph_url(
            db,
            url=str(u),
            publisher=payload.publisher or None,
            article_title=payload.article_title or None,
            pub_date=payload.date_published or None,
        )
        results.append({"url": str(u), **out})
    return {"items": results}


@app.post("/discover/ingest")
def discover_and_ingest(
    jurisdiction: Literal["BC", "AB", "WA"] = Query(...),
    years: int = Query(3, ge=1, le=50),
    activity: Optional[str] = Query(None),
    mode: Literal["broad", "allowlist", "both"] = Query("both"),
    max_urls: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
):
    params = SearchParams(jurisdiction=jurisdiction, years=years, activity=activity, mode=mode)
    disc = run_discovery(params)
    urls = [it["url"] for it in disc.get("items", []) if it.get("url")][:max_urls]
    results = []
    for u in urls:
        results.append({"url": u, **run_ingest_graph_url(db, url=u)})
    return {"queries": disc.get("queries"), "items": results}


@app.get("/sar_ops")
def list_sar_ops(event_id: Optional[str] = Query(None), db: Session = Depends(get_db)):
    from uuid import UUID

    uid = None
    if event_id:
        try:
            uid = UUID(event_id)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid event_id")
    return {"items": get_sar_ops(db, uid)}


@app.get("/events")
def list_events(
    jurisdiction: Optional[str] = Query(None, description="Filter by jurisdiction (BC, AB, WA)"),
    start_date: Optional[str] = Query(None, description="ISO date lower bound for date_of_death/event_start"),
    end_date: Optional[str] = Query(None, description="ISO date upper bound"),
    db: Session = Depends(get_db),
):
    q = select(Event)
    if jurisdiction:
        q = q.where(Event.jurisdiction == jurisdiction)
    if start_date:
        try:
            sd = date_type.fromisoformat(start_date)
            q = q.where((Event.date_of_death >= sd) | (Event.date_event_start >= sd))
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid start_date")
    if end_date:
        try:
            ed = date_type.fromisoformat(end_date)
            q = q.where((Event.date_of_death <= ed) | (Event.date_event_end <= ed))
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid end_date")

    items: list[Event] = db.execute(q.order_by(Event.created_at.desc()).limit(50)).scalars().all()
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

@app.post("/events/{event_id}/reprocess")
def reprocess_event(event_id: str, db: Session = Depends(get_db)):
    from uuid import UUID

    try:
        uid = UUID(event_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid event_id")

    e, _ = get_event_with_sources(db, uid)
    if not e:
        raise HTTPException(status_code=404, detail="Not found")

    latest = get_latest_source_for_event(db, uid)
    if not latest or not latest.cleaned_text:
        raise HTTPException(status_code=400, detail="No source text to process")

    try:
        pub_dt = None
        if latest.date_published:
            pub_dt = datetime.combine(latest.date_published, datetime.min.time())

        extracted = extract_core_fields(latest.cleaned_text, pub_dt)
        update_event_fields(db, uid, extracted)
        update_source_annotations(db, latest.source_id, quoted_evidence=extracted.get("quoted_evidence"), summary_bullets=extracted.get("summary_bullets"))

        delete_sar_ops_for_event(db, uid)
        insert_sar_segments(db, uid, extracted.get("sar") or [])

        hit = geocode_from_extracted(extracted)
        if hit:
            from .repo import set_event_geocode
            set_event_geocode(db, uid, hit)

        return {"status": "reprocessed", "event_id": event_id}
    except Exception as ex:
        # Bubble up the exact reason to simplify debugging
        raise HTTPException(status_code=500, detail=f"reprocess failed: {ex}")

@app.get("/export.csv")
def export_csv():
    # TODO: stream CSV export
    return {"todo": True}
