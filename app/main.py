from fastapi import FastAPI

from .db import engine
from .db_migrations import run_safe_migrations
from .api.health_admin import router as health_admin_router
from .api.ingest_jobs import router as ingest_jobs_router
from .api.events import router as events_router

app = FastAPI(title="Alpine Disasters: Agentic Ledger API", version="0.1")

app.include_router(health_admin_router)
app.include_router(ingest_jobs_router)
app.include_router(events_router)

@app.on_event("startup")
def _startup_migrate():
    try:
        run_safe_migrations(engine)
    except Exception as ex:
        import logging
        logging.getLogger(__name__).exception("startup migration failed: %s", ex)
