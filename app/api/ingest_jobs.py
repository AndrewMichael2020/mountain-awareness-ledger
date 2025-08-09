from typing import Optional, Literal
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from ..db import get_db
from ..schemas import IngestRequest, RawIngestRequest, TavilyIngestRequest, BatchIngestRequest
from ..pipeline.graph import run_ingest_graph_url, run_ingest_graph_raw
from ..pipeline.discover import SearchParams, run_discovery
from ..pipeline.graph_discover import run_discover_graph

router = APIRouter()

@router.post("/discover")
def discover(
    jurisdiction: Literal["BC", "AB", "WA"] = Query(..., description="One of: BC, AB, WA"),
    years: int = Query(10, ge=1, le=50, description="Lookback window in years"),
    activity: Optional[str] = Query(None, description="activity filter: alpinism, climbing, hiking, ski-mountaineering"),
    mode: Literal["broad", "allowlist", "both"] = Query("both", description="Search breadth"),
):
    params = SearchParams(jurisdiction=jurisdiction, years=years, activity=activity, mode=mode)
    out = run_discovery(params)
    return {"status": "ok", **out}

@router.post("/ingest")
def ingest(payload: IngestRequest, db: Session = Depends(get_db)):
    return run_ingest_graph_url(
        db,
        url=str(payload.url),
        publisher=payload.publisher or None,
        article_title=payload.article_title or None,
        pub_date=payload.date_published or None,
    )

@router.post("/ingest/raw")
def ingest_raw(payload: RawIngestRequest, db: Session = Depends(get_db)):
    return run_ingest_graph_raw(
        db,
        url=str(payload.url),
        clean_text=payload.clean_text,
        publisher=payload.publisher or None,
        article_title=payload.article_title or None,
        pub_date=payload.date_published or None,
    )

@router.post("/ingest/tavily")
def ingest_tavily(payload: TavilyIngestRequest, db: Session = Depends(get_db)):
    if not payload.results:
        raise HTTPException(status_code=400, detail="no results")
    first = payload.results[0]
    body = RawIngestRequest(url=first.url, clean_text=first.raw_content)
    return ingest_raw(body, db)

@router.post("/ingest/batch")
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

@router.post("/discover/ingest")
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

@router.post("/jobs/discover_graph")
def jobs_discover_graph(
    jurisdiction: Literal["BC", "AB", "WA"] = Query(...),
    years: int = Query(5, ge=1, le=50),
    activity: Optional[str] = Query(None),
    mode: Literal["broad", "allowlist", "both"] = Query("both"),
    max_urls: int = Query(10, ge=1, le=50),
    augment: bool = Query(False, description="Run LLM refinement after ingest"),
    db: Session = Depends(get_db),
):
    params = SearchParams(jurisdiction=jurisdiction, years=years, activity=activity, mode=mode, max_results_per_query=max_urls)
    out = run_discover_graph(db, params, max_urls=max_urls, augment=augment)
    return out

@router.post("/jobs/graph_discovery")
def jobs_graph_discovery(
    jurisdiction: Literal["BC", "AB", "WA"] = Query(...),
    years: int = Query(5, ge=1, le=50),
    activity: Optional[str] = Query(None),
    mode: Literal["broad", "allowlist", "both"] = Query("both"),
    max_urls: int = Query(10, ge=1, le=50),
    augment: bool = Query(False, description="Run LLM refinement after ingest"),
    db: Session = Depends(get_db),
):
    params = SearchParams(jurisdiction=jurisdiction, years=years, activity=activity, mode=mode, max_results_per_query=max_urls)
    return run_discover_graph(db, params, max_urls=max_urls, augment=augment)
