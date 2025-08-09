from typing import Optional, Literal
from uuid import UUID
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import select

from ..db import get_db, engine
from ..models import Event
from ..repo import (
    get_event_with_sources,
    get_sar_ops,
    get_latest_source_for_event,
    update_event_fields,
    update_source_annotations,
    insert_sar_segments,
    delete_sar_ops_for_event,
    update_source_metadata,
)
from ..pipeline.geocoder import geocode_from_extracted
from ..pipeline.graph import run_ingest_graph_url
from ..pipeline.discover import SearchParams, run_discovery
from ..pipeline.graph_discover import run_discover_graph
from alpine.extract_det import extract_core_fields
from ..pipeline.llm_refine import refine_with_llm, merge_event_fields

router = APIRouter()

@router.get("/sar_ops")
def list_sar_ops(event_id: Optional[str] = Query(None), db: Session = Depends(get_db)):
    uid = None
    if event_id:
        try:
            uid = UUID(event_id)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid event_id")
    return {"items": get_sar_ops(db, uid)}

@router.get("/events")
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
            sd = datetime.date.fromisoformat(start_date)  # type: ignore
            q = q.where((Event.date_of_death >= sd) | (Event.date_event_start >= sd))
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid start_date")
    if end_date:
        try:
            ed = datetime.date.fromisoformat(end_date)  # type: ignore
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

@router.get("/events/{event_id}/simple")
def get_event_simple(event_id: str, db: Session = Depends(get_db)):
    try:
        uid = UUID(event_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid event_id")

    e, sources = get_event_with_sources(db, uid)
    if not e:
        raise HTTPException(status_code=404, detail="Not found")

    return {
        "event_id": str(e.event_id),
        "jurisdiction": e.jurisdiction,
        "location_name": e.location_name,
        "peak_name": e.peak_name,
        "activity": e.activity,
        "n_fatalities": e.n_fatalities,
        "date_of_death": e.date_of_death,
        "created_at": e.created_at,
        "sources": [
            {
                "source_id": str(s.source_id),
                "url": s.url or "",
                "publisher": s.publisher,
                "article_title": s.article_title,
                "date_published": s.date_published,
            }
            for s in sources
        ],
    }

@router.get("/events/{event_id}")
def get_event_detail(event_id: str, verbose: bool = Query(False), db: Session = Depends(get_db)):
    ev, srcs = get_event_with_sources(db, UUID(event_id))
    if not ev:
        raise HTTPException(status_code=404, detail="event not found")
    no_info = "unknown"
    out = {
        "event_id": str(ev.event_id),
        "jurisdiction": ev.jurisdiction or no_info,
        "location_name": (ev.location_name or no_info),
        "peak_name": (ev.peak_name or no_info),
        "activity": ev.activity or no_info,
        "event_type": getattr(ev, "event_type", None),
        "cause_primary": (getattr(ev, "cause_primary", None) or no_info),
        "contributing_factors": getattr(ev, "contributing_factors", None) or [],
        "n_fatalities": ev.n_fatalities,
        "date_event_start": getattr(ev, "date_event_start", None),
        "date_event_end": getattr(ev, "date_event_end", None),
        "date_of_death": ev.date_of_death,
        "admin_area": getattr(ev, "admin_area", None) or no_info,
        "iso_country": getattr(ev, "iso_country", None) or no_info,
        "tz_local": getattr(ev, "tz_local", None) or no_info,
        "phase": getattr(ev, "phase", None) or no_info,
        "names_all": getattr(ev, "names_all", None) or [],
        "names_deceased": getattr(ev, "names_deceased", None) or [],
        "names_relatives": getattr(ev, "names_relatives", None) or [],
        "names_responders": getattr(ev, "names_responders", None) or [],
        "names_spokespersons": getattr(ev, "names_spokespersons", None) or [],
        "names_medics": getattr(ev, "names_medics", None) or [],
        "created_at": ev.created_at.date().isoformat() if ev.created_at else None,
        "updated_at": getattr(ev, "updated_at", None),
    }
    sources = []
    for s in srcs:
        item = {
            "source_id": str(s.source_id),
            "url": s.url,
            "publisher": (s.publisher or no_info),
            "article_title": (s.article_title or no_info),
            "date_published": s.date_published,
            "summary_bullets": getattr(s, "summary_bullets", None),
            "quoted_evidence": getattr(s, "quoted_evidence", None),
        }
        if verbose and getattr(s, "cleaned_text", None):
            txt = s.cleaned_text or ""
            item["cleaned_text_excerpt"] = txt[:600]
        sources.append(item)
    out["sources"] = sources
    return out

@router.get("/events/{event_id}/sources")
def get_event_sources(event_id: str, text: bool = Query(False, description="Include full cleaned_text"), db: Session = Depends(get_db)):
    try:
        uid = UUID(event_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid event_id")

    ev, srcs = get_event_with_sources(db, uid)
    if not ev:
        raise HTTPException(status_code=404, detail="event not found")

    items = []
    for s in srcs:
        item = {
            "source_id": str(s.source_id),
            "url": s.url,
            "publisher": s.publisher,
            "article_title": s.article_title,
            "date_published": s.date_published,
        }
        if text:
            item["cleaned_text"] = s.cleaned_text
        else:
            if getattr(s, "cleaned_text", None):
                item["cleaned_text_excerpt"] = (s.cleaned_text or "")[:1000]
        items.append(item)
    return {"event_id": event_id, "sources": items}

@router.post("/events/{event_id}/reprocess")
def reprocess_event(event_id: str, db: Session = Depends(get_db)):
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
            from ..repo import set_event_geocode
            set_event_geocode(db, uid, hit)

        return {"status": "reprocessed", "event_id": event_id}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"reprocess failed: {ex}")

@router.post("/events/{event_id}/augment")
def augment_event(event_id: UUID, multi: bool = Query(True, description="Use all sources as context"), db: Session = Depends(get_db)):
    ev, srcs = get_event_with_sources(db, event_id)
    if not ev:
        raise HTTPException(status_code=404, detail="event not found")
    latest = get_latest_source_for_event(db, event_id)
    if not latest or not latest.cleaned_text:
        raise HTTPException(status_code=404, detail="no source text to augment")

    if multi and srcs:
        parts = []
        for s in srcs:
            if not s.cleaned_text:
                continue
            pub_str = f"Published: {s.date_published.isoformat()}" if getattr(s, "date_published", None) else ""
            header = f"Source: {s.publisher or ''} | {s.article_title or ''} | {s.url or ''} {pub_str}".strip()
            parts.append(f"{header}\n\n{s.cleaned_text}")
        combined_text = "\n\n---\n\n".join(parts) if parts else (latest.cleaned_text or "")
    else:
        combined_text = latest.cleaned_text or ""

    deterministic = {
        "jurisdiction": ev.jurisdiction,
        "location_name": ev.location_name,
        "peak_name": ev.peak_name,
        "activity": ev.activity,
        "cause_primary": getattr(ev, "cause_primary", None),
        "contributing_factors": getattr(ev, "contributing_factors", None),
        "n_fatalities": ev.n_fatalities,
        "date_event_start": getattr(ev, "date_event_start", None),
        "date_event_end": getattr(ev, "date_event_end", None),
        "date_of_death": ev.date_of_death,
        "names_all": getattr(ev, "names_all", None),
        "names_deceased": getattr(ev, "names_deceased", None),
        "names_relatives": getattr(ev, "names_relatives", None),
        "names_responders": getattr(ev, "names_responders", None),
        "names_spokespersons": getattr(ev, "names_spokespersons", None),
        "names_medics": getattr(ev, "names_medics", None),
    }

    refined = refine_with_llm(combined_text, deterministic)
    merged = merge_event_fields(deterministic, refined)

    update_event_fields(db, event_id, merged)

    if merged.get("summary_bullets") or merged.get("quoted_evidence"):
        update_source_annotations(
            db,
            latest.source_id,
            quoted_evidence=merged.get("quoted_evidence"),
            summary_bullets=merged.get("summary_bullets"),
        )

    if merged.get("source_publisher") or merged.get("source_title") or merged.get("source_date_published"):
        update_source_metadata(
            db,
            latest.source_id,
            publisher=merged.get("source_publisher"),
            article_title=merged.get("source_title"),
            date_published=merged.get("source_date_published"),
        )

    if merged.get("sar"):
        delete_sar_ops_for_event(db, event_id)
        insert_sar_segments(db, event_id, merged["sar"]) 

    return {"status": "augmented", "event_id": str(event_id)}

@router.post("/events/{event_id}/augment/preview")
def augment_preview(event_id: UUID, multi: bool = Query(True, description="Use all sources as context"), db: Session = Depends(get_db)):
    ev, srcs = get_event_with_sources(db, event_id)
    if not ev:
        raise HTTPException(status_code=404, detail="event not found")
    latest = get_latest_source_for_event(db, event_id)
    if not latest or not latest.cleaned_text:
        raise HTTPException(status_code=404, detail="no source text to augment")

    if multi and srcs:
        parts = []
        for s in srcs:
            if not s.cleaned_text:
                continue
            pub_str = f"Published: {s.date_published.isoformat()}" if getattr(s, "date_published", None) else ""
            header = f"Source: {s.publisher or ''} | {s.article_title or ''} | {s.url or ''} {pub_str}".strip()
            parts.append(f"{header}\n\n{s.cleaned_text}")
        combined_text = "\n\n---\n\n".join(parts) if parts else (latest.cleaned_text or "")
    else:
        combined_text = latest.cleaned_text or ""

    deterministic = {
        "jurisdiction": ev.jurisdiction,
        "location_name": ev.location_name,
        "peak_name": ev.peak_name,
        "activity": ev.activity,
        "cause_primary": getattr(ev, "cause_primary", None),
        "contributing_factors": getattr(ev, "contributing_factors", None),
        "n_fatalities": ev.n_fatalities,
        "date_event_start": getattr(ev, "date_event_start", None),
        "date_event_end": getattr(ev, "date_event_end", None),
        "date_of_death": ev.date_of_death,
        "names_all": getattr(ev, "names_all", None),
        "names_deceased": getattr(ev, "names_deceased", None),
        "names_relatives": getattr(ev, "names_relatives", None),
        "names_responders": getattr(ev, "names_responders", None),
        "names_spokespersons": getattr(ev, "names_spokespersons", None),
        "names_medics": getattr(ev, "names_medics", None),
    }

    refined = refine_with_llm(combined_text, deterministic)

    return {
        "event_id": str(event_id),
        "sources_count": len(srcs or []),
        "context_len": len(combined_text or ""),
        "deterministic": deterministic,
        "refined": getattr(refined, "model_dump", lambda: refined.dict())(),
    }

@router.post("/events/augment_missing")
def augment_missing(
    jurisdiction: Optional[str] = Query(None, description="Optional jurisdiction filter (BC, AB, WA)"),
    limit: int = Query(50, ge=1, le=500, description="How many recent events to consider"),
    force: bool = Query(False, description="Augment regardless of existing fields"),
    db: Session = Depends(get_db),
):
    # Select recent events, optionally filtered
    q = select(Event.event_id)
    if jurisdiction:
        q = q.where(Event.jurisdiction == jurisdiction)
    ids = [row[0] for row in db.execute(q.order_by(Event.created_at.desc()).limit(limit)).all()]

    ok, skipped, errors = [], [], []

    for uid in ids:
        try:
            ev, srcs = get_event_with_sources(db, uid)
            if not ev:
                skipped.append({"event_id": str(uid), "reason": "not found"})
                continue
            latest = get_latest_source_for_event(db, uid)
            if not latest or not latest.cleaned_text:
                skipped.append({"event_id": str(uid), "reason": "no source text"})
                continue

            if not force:
                # Skip if already has core fields
                if ev.location_name and ev.n_fatalities is not None and ev.jurisdiction and ev.activity:
                    skipped.append({"event_id": str(uid), "reason": "already populated"})
                    continue

            # Build context with published date
            if srcs:
                parts = []
                for s in srcs:
                    if not s.cleaned_text:
                        continue
                    pub_str = f"Published: {s.date_published.isoformat()}" if getattr(s, "date_published", None) else ""
                    header = f"Source: {s.publisher or ''} | {s.article_title or ''} | {s.url or ''} {pub_str}".strip()
                    parts.append(f"{header}\n\n{s.cleaned_text}")
                combined_text = "\n\n---\n\n".join(parts) if parts else (latest.cleaned_text or "")
            else:
                combined_text = latest.cleaned_text or ""

            deterministic = {
                "jurisdiction": ev.jurisdiction,
                "location_name": ev.location_name,
                "peak_name": ev.peak_name,
                "route_name": getattr(ev, "route_name", None),
                "activity": ev.activity,
                "cause_primary": getattr(ev, "cause_primary", None),
                "contributing_factors": getattr(ev, "contributing_factors", None),
                "n_fatalities": ev.n_fatalities,
                "date_event_start": getattr(ev, "date_event_start", None),
                "date_event_end": getattr(ev, "date_event_end", None),
                "date_of_death": ev.date_of_death,
                "names_all": getattr(ev, "names_all", None),
                "names_deceased": getattr(ev, "names_deceased", None),
                "names_relatives": getattr(ev, "names_relatives", None),
                "names_responders": getattr(ev, "names_responders", None),
                "names_spokespersons": getattr(ev, "names_spokespersons", None),
                "names_medics": getattr(ev, "names_medics", None),
            }

            refined = refine_with_llm(combined_text, deterministic)
            merged = merge_event_fields(deterministic, refined)

            update_event_fields(db, uid, merged)

            if merged.get("summary_bullets") or merged.get("quoted_evidence"):
                update_source_annotations(
                    db,
                    latest.source_id,
                    quoted_evidence=merged.get("quoted_evidence"),
                    summary_bullets=merged.get("summary_bullets"),
                )

            if merged.get("source_publisher") or merged.get("source_title") or merged.get("source_date_published"):
                update_source_metadata(
                    db,
                    latest.source_id,
                    publisher=merged.get("source_publisher"),
                    article_title=merged.get("source_title"),
                    date_published=merged.get("source_date_published"),
                )

            if merged.get("sar"):
                delete_sar_ops_for_event(db, uid)
                insert_sar_segments(db, uid, merged["sar"]) 

            ok.append(str(uid))
        except Exception as ex:
            errors.append({"event_id": str(uid), "error": str(ex)})

    return {"attempted": len(ids), "ok": len(ok), "skipped": len(skipped), "errors": len(errors), "ok_ids": ok, "skipped": skipped, "errors_detail": errors}
