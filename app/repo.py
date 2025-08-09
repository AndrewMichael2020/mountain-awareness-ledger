from __future__ import annotations

from datetime import datetime, date as date_type, date
import uuid
from typing import Optional, List, Dict, Any, Iterable
import json
import re

from sqlalchemy import select, text
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from .models import Event, Source


def _to_pg_text_array(values: Optional[List[str]]) -> Optional[str]:
    if not values:
        return None
    parts: List[str] = []
    for v in values:
        if v is None:
            continue
        s = str(v)
        s = s.replace("\\", "\\\\").replace('"', '\\"')
        parts.append(f'"{s}"')
    return "{" + ",".join(parts) + "}"


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


def _sanitize_place_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return name
    s = str(name)
    s = s.replace("\n", " ")
    s = re.sub(r"\s+", " ", s).strip()
    # Remove any trailing ", near ..." fragment
    s = re.sub(r",?\s*near\b.*$", "", s, flags=re.IGNORECASE)
    s = s.strip(" ,;-")
    return s or None


def _norm_names(val: Any) -> list[str] | None:
    if val is None:
        return None
    out: list[str] = []
    def _add(s: str):
        s2 = (s or "").strip()
        if s2:
            out.append(s2)
    if isinstance(val, str):
        for p in val.replace(";", ",").split(","):
            _add(p)
    elif isinstance(val, Iterable):
        for p in val:
            if isinstance(p, str):
                _add(p)
    return out or None


def update_event_fields(db: Session, event_id: uuid.UUID, fields: Dict[str, Any]) -> None:
    """Update event columns with extracted fields; ignore keys not present."""
    updatable = {
        "jurisdiction", "iso_country", "admin_area", "location_name", "peak_name",
        "event_type", "activity", "n_fatalities", "date_event_start", "date_event_end",
        "date_of_death", "cause_primary", "contributing_factors", "phase", "tz_local",
        "names_all", "names_deceased", "names_relatives", "names_responders", "names_spokespersons", "names_medics",
    }
    # Sanitize place-like strings
    fields = dict(fields or {})
    for key in ("location_name", "peak_name"):
        if key in fields and isinstance(fields.get(key), str):
            fields[key] = _sanitize_place_name(fields.get(key))

    # Normalize names
    for k in ("names_all", "names_deceased", "names_relatives", "names_responders", "names_spokespersons", "names_medics"):
        if k in fields:
            fields[k] = _norm_names(fields.get(k))

    data = {k: v for k, v in fields.items() if k in updatable and v is not None}
    if not data:
        return
    e = db.get(Event, event_id)
    if e is not None and all(hasattr(e, k) for k in data.keys()):
        for k, v in data.items():
            setattr(e, k, v)
        db.commit()
        return
    sets_parts: List[str] = []
    params: Dict[str, Any] = {"event_id": str(event_id)}
    for k, v in data.items():
        if k == "contributing_factors" and isinstance(v, list):
            arr = _to_pg_text_array(v)
            if arr:
                sets_parts.append(f"{k} = CAST(:{k} AS text[])")
                params[k] = arr
            else:
                sets_parts.append(f"{k} = NULL")
        else:
            sets_parts.append(f"{k} = :{k}")
            params[k] = v
    sets_sql = ", ".join(sets_parts)
    stmt = text(f"UPDATE events SET {sets_sql}, updated_at = now() WHERE event_id = :event_id")
    db.execute(stmt, params)
    db.commit()


def update_source_annotations(db: Session, source_id: uuid.UUID,
                              quoted_evidence: Optional[Dict[str, Any]] = None,
                              summary_bullets: Optional[List[str]] = None) -> None:
    if quoted_evidence is None and summary_bullets is None:
        return
    s = db.get(Source, source_id)
    if s is not None and hasattr(s, "quoted_evidence") and hasattr(s, "summary_bullets"):
        if quoted_evidence is not None:
            s.quoted_evidence = quoted_evidence
        if summary_bullets is not None:
            s.summary_bullets = summary_bullets
        db.commit()
        return
    sets = []
    params: Dict[str, Any] = {"source_id": str(source_id)}
    if quoted_evidence is not None:
        sets.append("quoted_evidence = CAST(:quoted_evidence AS jsonb)")
        params["quoted_evidence"] = json.dumps(quoted_evidence)
    if summary_bullets is not None:
        arr = _to_pg_text_array(summary_bullets)
        if arr:
            sets.append("summary_bullets = CAST(:summary_bullets AS text[])")
            params["summary_bullets"] = arr
        else:
            sets.append("summary_bullets = NULL")
    if sets:
        stmt = text(f"UPDATE sources SET {', '.join(sets)} WHERE source_id = :source_id")
        db.execute(stmt, params)
        db.commit()


def insert_sar_segments(db: Session, event_id: uuid.UUID, segments: List[Dict[str, Any]]) -> None:
    if not segments:
        return
    stmt = text(
        """
        INSERT INTO sar_ops (sar_id, event_id, agency, op_type, started_at, ended_at, outcome, notes)
        VALUES (CAST(:sar_id AS uuid), CAST(:event_id AS uuid), :agency, :op_type, :started_at, :ended_at, :outcome, :notes)
        """
    )
    for seg in segments:
        params = {
            "sar_id": str(uuid.uuid4()),
            "event_id": str(event_id),
            "agency": seg.get("agency"),
            "op_type": seg.get("op_type"),
            "started_at": seg.get("started_at"),
            "ended_at": seg.get("ended_at"),
            "outcome": seg.get("outcome"),
            "notes": seg.get("notes"),
        }
        db.execute(stmt, params)
    db.commit()


def get_event_with_sources(db: Session, event_id: uuid.UUID) -> tuple[Event | None, list[Source]]:
    e = db.get(Event, event_id)
    if not e:
        return None, []
    sources = db.execute(
        select(Source).where(Source.event_id == event_id).order_by(Source.date_published.is_(None), Source.date_published.desc())
    ).scalars().all()
    return e, sources


def get_sar_ops(db: Session, event_id: Optional[uuid.UUID] = None, limit: int = 200) -> List[Dict[str, Any]]:
    where = ""
    params: Dict[str, Any] = {"limit": limit}
    if event_id:
        where = "WHERE event_id = :event_id"
        params["event_id"] = str(event_id)
    rows = db.execute(text(f"""
        SELECT sar_id::text, event_id::text, agency, op_type, started_at, ended_at, outcome, notes
        FROM sar_ops
        {where}
        ORDER BY COALESCE(started_at, ended_at) DESC NULLS LAST
        LIMIT :limit
    """), params).mappings().all()
    return [dict(r) for r in rows]


def set_event_geocode(db: Session, event_id: uuid.UUID, hit: Dict[str, Any]) -> None:
    # Requires PostGIS geography(Point,4326) column geom
    stmt = text(
        """
        UPDATE events
        SET geom = ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
            iso_country = COALESCE(:iso_country, iso_country),
            admin_area = COALESCE(:admin_area, admin_area),
            tz_local = COALESCE(:tz_local, tz_local),
            updated_at = now()
        WHERE event_id = :event_id
        """
    )
    db.execute(stmt, {
        "event_id": str(event_id),
        "lat": hit.get("lat"),
        "lon": hit.get("lon"),
        "iso_country": hit.get("iso_country"),
        "admin_area": hit.get("admin_area"),
        "tz_local": hit.get("tz_local"),
    })
    db.commit()


def get_latest_source_for_event(db: Session, event_id: uuid.UUID) -> Optional[Source]:
    return db.execute(
        select(Source)
        .where(Source.event_id == event_id)
        .order_by(Source.date_published.is_(None), Source.date_published.desc(), Source.date_scraped.desc())
        .limit(1)
    ).scalar_one_or_none()


def delete_sar_ops_for_event(db: Session, event_id: uuid.UUID) -> None:
    db.execute(text("DELETE FROM sar_ops WHERE event_id = :event_id"), {"event_id": str(event_id)})
    db.commit()


def update_source_metadata(db: Session, source_id: uuid.UUID, *, publisher: Optional[str] = None, article_title: Optional[str] = None, date_published: Optional[date] = None) -> None:
    from .models import Source
    s = db.get(Source, source_id)
    if not s:
        return
    changed = False
    if publisher is not None and publisher != s.publisher:
        s.publisher = publisher
        changed = True
    if article_title is not None and article_title != s.article_title:
        s.article_title = article_title
        changed = True
    if date_published is not None and date_published != s.date_published:
        s.date_published = date_published
        changed = True
    if changed:
        db.add(s)
        db.commit()
