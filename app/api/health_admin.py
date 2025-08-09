from fastapi import APIRouter, HTTPException, Query, status
from sqlalchemy import text

from ..db import engine
from ..db_migrations import run_safe_migrations

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
def export_csv():
    # TODO: stream CSV export
    return {"todo": True}
