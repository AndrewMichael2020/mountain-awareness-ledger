from __future__ import annotations

from datetime import datetime, date as date_type
from typing import Any, Dict, Optional, TypedDict
import logging

from langgraph.graph import StateGraph, END

from app.pipeline.fetcher import fetch_url
from app.pipeline.cleaner import clean_html
from alpine.extract_det import extract_core_fields
from app.pipeline.geocoder import geocode_from_extracted
from app.repo import (
    get_source_by_url,
    create_event,
    create_source,
    update_event_fields,
    update_source_annotations,
    insert_sar_segments,
    set_event_geocode,
)

logger = logging.getLogger(__name__)


class IngestState(TypedDict, total=False):
    url: str
    final_url: Optional[str]
    html: Optional[str]
    text_body: Optional[str]
    meta: Optional[dict]
    publisher: Optional[str]
    article_title: Optional[str]
    pub_date: Optional[date_type]
    extracted: Optional[dict]
    event_id: Optional[str]
    source_id: Optional[str]
    status: Optional[str]


def _router(state: IngestState) -> str:
    if state.get("text_body"):
        return "have_text"
    if state.get("html"):
        return "have_html"
    return "need_fetch"


def _node_fetch(state: IngestState) -> IngestState:
    try:
        html, final_url = fetch_url(state["url"])  # type: ignore[index]
        state["html"] = html or ""
        state["final_url"] = final_url or state.get("url")
        return state
    except PermissionError:
        # Respect robots.txt: do not raise, do not retry; short-circuit pipeline
        logger.warning("ingest.fetch: blocked by robots.txt url=%s", state.get("url"))
        state["html"] = ""
        state["final_url"] = state.get("url")
        state["error"] = "robots_blocked"
        state["skip"] = True
        return state
    except Exception as ex:
        # On other hard fetch errors, also avoid retries for now
        logger.error("ingest.fetch: failed url=%s err=%s", state.get("url"), ex)
        state["html"] = ""
        state["final_url"] = state.get("url")
        state["error"] = str(ex)
        state["skip"] = True
        return state


def _node_dup_check(db, state: IngestState) -> IngestState:
    # Check both original and final URLs
    for u in [state.get("final_url"), state.get("url")]:
        if u:
            existing = get_source_by_url(db, str(u))
            if existing:
                state["status"] = "exists"
                state["event_id"] = str(existing.event_id)
                state["source_id"] = str(existing.source_id)
                return state
    return state


def _node_clean(state: IngestState) -> IngestState:
    if state.get("skip"):
        return state
    text_body, meta = clean_html(state.get("html") or "", state.get("final_url"))
    state["text_body"] = text_body
    state["meta"] = meta or {}
    # Backfill pub_date from metadata if missing
    if not state.get("pub_date") and isinstance((meta or {}).get("date"), str):
        try:
            state["pub_date"] = date_type.fromisoformat(meta["date"][0:10])  # type: ignore[index]
        except Exception:
            pass
    return state


def _node_extract(state: IngestState) -> IngestState:
    if state.get("skip"):
        return state
    dt = state.get("pub_date")
    published = datetime.combine(dt, datetime.min.time()) if dt else None
    state["extracted"] = extract_core_fields(state.get("text_body") or "", published)
    return state


def _node_persist(db, state: IngestState) -> IngestState:
    if state.get("status") == "exists":
        return state
    # Create a new event; default jurisdiction until extraction overwrites
    e = create_event(db, jurisdiction="BC")
    state["event_id"] = str(e.event_id)

    # Create or reuse source (create_source guards unique URL)
    s = create_source(
        db,
        event_id=e.event_id,
        url=state.get("final_url") or state.get("url") or "",
        publisher=state.get("publisher"),
        article_title=state.get("article_title") or ((state.get("meta") or {}).get("title") if state.get("meta") else None),
        date_published=state.get("pub_date"),
        cleaned_text=state.get("text_body"),
        date_scraped=datetime.utcnow(),
    )
    state["source_id"] = str(s.source_id)

    extracted = state.get("extracted") or {}
    update_event_fields(db, e.event_id, extracted)
    update_source_annotations(db, s.source_id, quoted_evidence=extracted.get("quoted_evidence"), summary_bullets=extracted.get("summary_bullets"))
    insert_sar_segments(db, e.event_id, extracted.get("sar") or [])

    state["status"] = state.get("status") or "created"
    return state


def _node_geocode(db, state: IngestState) -> IngestState:
    if state.get("status") == "exists":
        return state
    hit = geocode_from_extracted(state.get("extracted") or {})
    if hit and state.get("event_id"):
        from uuid import UUID

        set_event_geocode(db, UUID(state["event_id"]), hit)
    return state


def build_ingest_graph(db):
    g = StateGraph(IngestState)
    g.add_node("fetch", _node_fetch)
    g.add_node("dup_check", lambda s: _node_dup_check(db, s))
    g.add_node("clean", _node_clean)
    g.add_node("extract", _node_extract)
    g.add_node("persist", lambda s: _node_persist(db, s))
    g.add_node("geocode", lambda s: _node_geocode(db, s))

    g.set_conditional_entry_point(_router, {
        "need_fetch": "fetch",
        "have_html": "clean",
        "have_text": "extract",
    })
    g.add_edge("fetch", "dup_check")
    g.add_edge("dup_check", "clean")
    g.add_edge("clean", "extract")
    g.add_edge("extract", "persist")
    g.add_edge("persist", "geocode")
    g.add_edge("geocode", END)
    return g.compile()


def run_ingest_graph_url(db, url: str, publisher: Optional[str] = None, article_title: Optional[str] = None, pub_date: Optional[date_type] = None) -> Dict[str, Any]:
    # Pre-check duplicate by input URL
    existing = get_source_by_url(db, str(url))
    if existing:
        return {"status": "exists", "event_id": str(existing.event_id), "source_id": str(existing.source_id)}
    state: IngestState = {
        "url": url,
        "publisher": publisher,
        "article_title": article_title,
        "pub_date": pub_date,
    }
    app = build_ingest_graph(db)
    out = app.invoke(state)
    # If fetch step marked this as skipped, return a concise result
    if isinstance(out, dict) and out.get("skip"):
        return {"status": "skipped", "url": out.get("final_url") or url, "reason": out.get("error")}
    return {"status": out.get("status"), "event_id": out.get("event_id"), "source_id": out.get("source_id")}


def run_ingest_graph_raw(db, url: str, clean_text: str, publisher: Optional[str] = None, article_title: Optional[str] = None, pub_date: Optional[date_type] = None) -> Dict[str, Any]:
    existing = get_source_by_url(db, str(url))
    if existing:
        return {"status": "exists", "event_id": str(existing.event_id), "source_id": str(existing.source_id)}
    state: IngestState = {
        "url": url,
        "text_body": clean_text,
        "publisher": publisher,
        "article_title": article_title,
        "pub_date": pub_date,
    }
    app = build_ingest_graph(db)
    out = app.invoke(state)
    return {"status": out.get("status"), "event_id": out.get("event_id"), "source_id": out.get("source_id")}
