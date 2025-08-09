from fastapi import APIRouter, HTTPException, Query, status, Depends, Response
from sqlalchemy import text, select
from sqlalchemy.orm import Session
import io, csv, json

from ..db import engine, get_db
from ..db_migrations import run_safe_migrations
from ..models import Event

router = APIRouter()

@router.get("/health")
def health():
    return {"ok": True}

@router.get("/db/health")
def db_health():
    try:
        with engine.connect() as conn:
            pg_version = conn.execute(text("SELECT version()")).scalar()
            try:
                postgis = conn.execute(text("SELECT postgis_version()")).scalar()
            except Exception:
                postgis = None
    except Exception as e:
        return {"ok": False, "error": str(e)}
    return {"ok": True, "pg_version": pg_version, "postgis": postgis}

@router.post("/db/migrate")
def migrate_now():
    try:
        run_safe_migrations(engine)
        return {"status": "ok"}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

@router.post("/db/reset")
def db_reset(confirm: bool = Query(False, description="Must be true to run destructive reset")):
    if not confirm:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Set confirm=true to reset database")
    errors = []
    truncated = []
    with engine.begin() as conn:
        for table in ("sar_ops", "sources", "events"):
            try:
                conn.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"))
                truncated.append(table)
            except Exception as ex:
                errors.append({"table": table, "error": str(ex)})
    return {"status": "ok", "truncated": truncated, "errors": errors}

@router.get("/export.csv")
def export_csv(db: Session = Depends(get_db)):
    return export_events_csv(db)

@router.get("/export/events.csv")
def export_events_csv(db: Session = Depends(get_db)):
    items: list[Event] = db.execute(select(Event).order_by(Event.created_at.desc())).scalars().all()
    buf = io.StringIO()
    w = csv.writer(buf)
    headers = [
        "event_id",
        "jurisdiction",
        "location_name",
        "peak_name",
        "route_name",
        "activity",
        "cause_primary",
        "contributing_factors",
        "n_fatalities",
        "n_injured",
        "party_size",
        "date_event_start",
        "date_event_end",
        "date_of_death",
        "admin_area",
        "iso_country",
        "tz_local",
        "phase",
        "names_all",
        "names_deceased",
        "names_relatives",
        "names_responders",
        "names_spokespersons",
        "names_medics",
        "created_at",
        "updated_at",
    ]
    w.writerow(headers)
    for e in items:
        row = [
            str(e.event_id),
            getattr(e, "jurisdiction", None),
            getattr(e, "location_name", None),
            getattr(e, "peak_name", None),
            getattr(e, "route_name", None),
            getattr(e, "activity", None),
            getattr(e, "cause_primary", None),
            json.dumps(getattr(e, "contributing_factors", None) or []),
            getattr(e, "n_fatalities", None),
            getattr(e, "n_injured", None),
            getattr(e, "party_size", None),
            getattr(e, "date_event_start", None),
            getattr(e, "date_event_end", None),
            getattr(e, "date_of_death", None),
            getattr(e, "admin_area", None),
            getattr(e, "iso_country", None),
            getattr(e, "tz_local", None),
            getattr(e, "phase", None),
            json.dumps(getattr(e, "names_all", None) or []),
            json.dumps(getattr(e, "names_deceased", None) or []),
            json.dumps(getattr(e, "names_relatives", None) or []),
            json.dumps(getattr(e, "names_responders", None) or []),
            json.dumps(getattr(e, "names_spokespersons", None) or []),
            json.dumps(getattr(e, "names_medics", None) or []),
            e.created_at.isoformat() if getattr(e, "created_at", None) else None,
            getattr(e, "updated_at", None),
        ]
        w.writerow(row)
    csv_bytes = buf.getvalue()
    return Response(content=csv_bytes, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=events.csv"})

@router.get("/export/sources.csv")
def export_sources_csv():
    with engine.connect() as conn:
        res = conn.execute(text(
            """
            SELECT
              source_id::text,
              event_id::text,
              url,
              publisher,
              article_title,
              date_published,
              CAST(summary_bullets AS TEXT) AS summary_bullets,
              CAST(quoted_evidence AS TEXT) AS quoted_evidence,
              created_at,
              updated_at