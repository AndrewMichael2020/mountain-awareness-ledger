from typing import Optional, Literal
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel
import os
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout, as_completed
from sqlalchemy.orm import sessionmaker
from ..db import engine, get_db
from ..schemas import IngestRequest, RawIngestRequest, TavilyIngestRequest, BatchIngestRequest
from ..pipeline.graph import run_ingest_graph_url, run_ingest_graph_raw
from ..pipeline.discover import SearchParams, run_discovery
from ..pipeline.graph_discover import run_discover_graph

router = APIRouter()

class IngestUrlJob(BaseModel):
    url: str

class IngestBatchJob(BaseModel):
    urls: list[str]

@router.post("/discover")
def discover(
    jurisdiction: Literal["BC", "AB", "WA"] = Query(..., description="One of: BC, AB, WA"),
    years: int = Query(10, ge=1, le=50, description="Lookback window in years"),
    activity: Optional[str] = Query(None, description="activity filter: alpinism, climbing, hiking, ski-mountaineering"),
    mode: Literal["broad", "allowlist", "both"] = Query("both", description="Search breadth"),
    strict: bool = Query(True, description="Filter results strictly to jurisdiction tokens"),
):
    params = SearchParams(jurisdiction=jurisdiction, years=years, activity=activity, mode=mode)
    params.strict = strict
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
def ingest_batch(job: IngestBatchJob, db: Session = Depends(get_db)):
    results = []
    errors = []

    LocalSession = sessionmaker(bind=engine)
    timeout = float(os.environ.get("INGEST_TIMEOUT", "10"))
    workers = int(os.environ.get("INGEST_WORKERS", "4"))

    def _ingest_one(u: str) -> dict:
        sess = LocalSession()
        try:
            out = run_ingest_graph_url(sess, u)
            status = out.get("status", "ok") if isinstance(out, dict) else "ok"
            reason = out.get("reason") if isinstance(out, dict) else None
            return {"url": u, "status": status, "reason": reason}
        finally:
            try:
                sess.close()
            except Exception:
                pass

    executor = ThreadPoolExecutor(max_workers=max(1, workers))
    futures = {executor.submit(_ingest_one, u): u for u in job.urls}
    try:
        for fut in as_completed(futures, timeout=timeout):
            u = futures[fut]
            try:
                results.append(fut.result())
            except Exception as exn:
                errors.append({"url": u, "error": str(exn)})
                results.append({"url": u, "status": "error", "reason": str(exn)})
    except FuturesTimeout:
        pass
    finally:
        for fut, u in futures.items():
            if not fut.done():
                results.append({"url": u, "status": "timeout", "reason": f">{int(timeout)}s"})
        executor.shutdown(wait=False, cancel_futures=True)

    ok_statuses = {"ok", "skipped", "created", "exists"}
    ok_count = sum(1 for r in results if r.get("status") in ok_statuses)
    return {"ok": ok_count, "errors": errors, "results": results}

@router.post("/jobs/ingest_batch")
def jobs_ingest_batch(job: IngestBatchJob, db: Session = Depends(get_db)):
    return ingest_batch(job, db)

@router.post("/discover/ingest")
def discover_and_ingest(
    jurisdiction: Literal["BC", "AB", "WA"] = Query(...),
    years: int = Query(3, ge=1, le=50),
    activity: Optional[str] = Query(None),
    mode: Literal["broad", "allowlist", "both"] = Query("both"),
    max_urls: int = Query(10, ge=1, le=50),
    strict: bool = Query(True, description="Filter results strictly to jurisdiction tokens"),
    db: Session = Depends(get_db),
):
    params = SearchParams(jurisdiction=jurisdiction, years=years, activity=activity, mode=mode)
    params.strict = strict
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
