# Mountain Awareness Ledger

This service discovers, ingests, and structures mountain-incident reports from the web, then optionally refines fields with an LLM.

Core flow
- Discover: query the web for candidate articles (URLs)
- Ingest: fetch, clean, and persist Sources and Events from those URLs
- Augment (LLM): refine and fill gaps in event fields using model output

Quick start
1) Prerequisites
- Dev container: Ubuntu 24.04.2 LTS
- Docker running a Postgres container named alpine-pg
- Python deps installed (via the dev container)

2) Environment
- Copy .env.local template (already present) and set secrets. Minimum:
  - DATABASE_URL=postgresql+psycopg2://postgres:postgres@localhost:5432/alpine
  - TAVILY_API_KEY=... (your key)
  - OPENAI_API_KEY=... (for LLM steps)
  - TAVILY_AUTH_STYLE=bearer (recommended for dev keys)
- Load env in shell
  set -a && source .env.local && set +a

3) Database
- Ensure DB and PostGIS exist inside alpine-pg
  docker exec -it alpine-pg psql -U postgres -c "CREATE DATABASE alpine;"  # ok if exists
  docker exec -it alpine-pg psql -U postgres -d alpine -c "CREATE EXTENSION IF NOT EXISTS postgis;"
- Create app tables
  python -c 'from app.db import engine; from app.models import Base; Base.metadata.create_all(bind=engine); print("Schema created")'
- Verify
  docker exec -it alpine-pg psql -U postgres -d alpine -c "\dt public.*"

4) Run API
- Start FastAPI with your env file
  uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload --env-file .env.local
- Health
  curl -s http://127.0.0.1:8000/health | jq
- Docs
  $BROWSER http://127.0.0.1:8000/docs

Workflow: Discovery → Ingest → LLM
Why ingest after discovery?
- Discovery returns candidate URLs only. Ingest fetches each page, cleans text, extracts baseline fields, and persists a Source linked to an Event. LLM then operates on the persisted text and updates the Event.

A) Discovery
- Endpoint: POST /discover
- Params: jurisdiction (BC|AB|WA), years, mode (allowlist|broad|both), strict (bool), max_results_per_query
- Example
  curl -s -X POST "http://127.0.0.1:8000/discover?jurisdiction=WA&years=5&mode=broad&strict=false&max_results_per_query=20" | jq

Notes
- The service builds boolean and NL queries. It filters results by jurisdiction tokens when strict=true.
- Tavily API auth style is configurable: TAVILY_AUTH_STYLE=header|bearer|body. If header 401s with your key, set bearer.
- Time range handling: when start/end dates are provided, time_range is omitted per Tavily requirements.

B) Ingest (from discovered URLs)
- Single: POST /ingest/url
  Body: {"url":"https://example.com/article"}
  curl -s -X POST http://127.0.0.1:8000/ingest/url -H "Content-Type: application/json" -d '{"url":"https://example.org/article"}' | jq
- Batch: POST /ingest/batch
  Body: {"urls":["https://...","https://..."]}
  URLS=$(curl -s -X POST "http://127.0.0.1:8000/discover?jurisdiction=WA&years=5&mode=broad&strict=false&max_results_per_query=10" | jq -r '.items[].url' | head -n 5 | jq -Rc 'inputs' | jq -sc '{urls:.}')
  curl -s -X POST http://127.0.0.1:8000/ingest/batch -H "Content-Type: application/json" -d "$URLS" | jq

After ingest
- List events
  curl -s http://127.0.0.1:8000/events | jq
- Get event details
  EVENT_ID=$(curl -s http://127.0.0.1:8000/events | jq -r '.items[0].event_id')
  curl -s http://127.0.0.1:8000/events/$EVENT_ID | jq
- Get sources (optionally include cleaned text)
  curl -s "http://127.0.0.1:8000/events/$EVENT_ID/sources?text=false" | jq

C) LLM Augmentation
- Preview (no DB writes)
  curl -s -X POST "http://127.0.0.1:8000/events/$EVENT_ID/augment/preview?multi=true" | jq
- Persist augmentation
  curl -s -X POST "http://127.0.0.1:8000/events/$EVENT_ID/augment?multi=true" | jq
- Batch augment recent
  curl -s -X POST "http://127.0.0.1:8000/events/augment_missing?jurisdiction=WA&limit=50&force=false" | jq

Requirements for LLM steps
- Set OPENAI_API_KEY in .env.local
- Optional: OPENAI_MODEL (default gpt-4o-mini)

Relevant endpoints (selected)
- GET /events – list recent events (with filters)
- GET /events/{event_id} – detailed event (+ verbose excerpt)
- GET /events/{event_id}/sources – list sources (+ full text optional)
- POST /events/{event_id}/reprocess – re-run LLM on an event
- POST /events/{event_id}/augment – apply LLM augmentation
- POST /events/{event_id}/augment/preview – dry run
- POST /events/augment_missing – augment multiple
- POST /discover – run discovery queries
- POST /ingest/url – ingest a single URL
- POST /ingest/batch – ingest many URLs

Tavily tips (manual curl)
- Use bearer auth if header fails for your key:
  curl -i -X POST https://api.tavily.com/search \
    -H "Authorization: Bearer $TAVILY_API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"query":"test","max_results":3}'
- Escape inner quotes in JSON or build with jq:
  jq -nc --arg q '(died OR dies OR fatal OR killed) AND (Washington OR "Washington State" OR WA)' '{"query":$q,"max_results":5,"search_depth":"advanced","include_answer":false}' | \
  curl -i -X POST https://api.tavily.com/search -H "Authorization: Bearer $TAVILY_API_KEY" -H "Content-Type: application/json" -d @-

Troubleshooting
- 401 Unauthorized from Tavily
  - Try TAVILY_AUTH_STYLE=bearer; reload env; restart API
  - Validate with curl bearer; rotate key if still failing
- 400 Invalid time_range from Tavily
  - Ensure the app omits time_range when using start_date/end_date (fixed)
- Invalid JSON in curl
  - Escape quotes or use jq to construct JSON
- DB missing tables
  - Run schema creation one-liner and verify with \dt public.*
- API logs
  - Run uvicorn in the foreground to see lines like "tavily_http: ..."

Notes
- .env.local is ignored by git. Do not commit real secrets.
- Data lives under DATA_DIR (see .env.local).

License
- See project license if provided.

---

# Alpine Disasters: Agentic Ledger for Fatal Alpine Incidents
_Last updated: 2025-08-08 23:30 UTC_

This guide is the working plan for building an agentic news and incident ledger for deaths of alpinists in BC, AB, and WA. It targets step-by-step implementation with GitHub Copilot and human review. Scope will expand later to near-misses and other platforms.

---

## Goals and scope
- Regions: British Columbia, Alberta, Washington State. Treat region as a parameter.
- Time window: default 10 years. Treat years as a parameter.
- Activity focus: alpinism, climbing, ski-mountaineering, scrambling, hiking (filterable).
- Output: a deterministic, queryable core plus a rich narrative per incident.
- Auditability: every key field has provenance quotes tied to a source.

---

## Parameters
- jurisdictions: array in { "BC", "AB", "WA" }
- years_lookback: integer (default 10)
- Optional filters: activity, cause_primary, season, publisher_whitelist, publisher_blacklist
- Query override: text include or exclude keyword lists

---

## Data model (PostgreSQL + PostGIS)
Minimal viable DDL. Extend as needed.

```sql
CREATE TABLE events (
  event_id UUID PRIMARY KEY,
  jurisdiction TEXT CHECK (jurisdiction IN ('BC','AB','WA')),
  iso_country TEXT,
  admin_area TEXT,
  location_name TEXT,
  peak_name TEXT,
  route_name TEXT,
  geom GEOGRAPHY(Point, 4326),
  elevation_m INTEGER,
  event_type TEXT CHECK (event_type IN ('fatality')),
  activity TEXT CHECK (activity IN ('alpinism','climbing','hiking','scrambling','ski-mountaineering','unknown')),
  n_fatalities SMALLINT,
  n_injured SMALLINT,
  party_size SMALLINT,
  date_event_start DATE,
  date_event_end DATE,
  date_of_death DATE,
  tz_local TEXT,
  cause_primary TEXT,
  contributing_factors TEXT[],
  weather_context_id UUID,
  avalanche_context_id UUID,
  dedupe_cluster_id UUID,
  extraction_conf NUMERIC(4,3),
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE sources (
  source_id UUID PRIMARY KEY,
  event_id UUID REFERENCES events(event_id) ON DELETE CASCADE,
  publisher TEXT,
  article_title TEXT,
  author TEXT,
  url TEXT UNIQUE,
  url_canonical_hash TEXT,
  date_published DATE,
  date_scraped TIMESTAMPTZ,
  paywalled BOOLEAN DEFAULT FALSE,
  license TEXT,
  cleaned_text TEXT,
  summary_bullets TEXT[],
  quoted_evidence JSONB
);

CREATE TABLE sar_ops (
  sar_id UUID PRIMARY KEY,
  event_id UUID REFERENCES events(event_id) ON DELETE CASCADE,
  agency TEXT,
  op_type TEXT CHECK (op_type IN ('search','recovery','rescue')),
  started_at TIMESTAMPTZ,
  ended_at TIMESTAMPTZ,
  outcome TEXT,
  notes TEXT
);

CREATE TABLE persons_public (
  person_id UUID PRIMARY KEY,
  event_id UUID REFERENCES events(event_id) ON DELETE CASCADE,
  role TEXT CHECK (role IN ('deceased','injured','companion')),
  age SMALLINT,
  sex TEXT,
  hometown TEXT,
  name_redacted BOOLEAN DEFAULT TRUE,
  source_id UUID REFERENCES sources(source_id),
  notes TEXT
);

CREATE TABLE enrich_weather (
  weather_context_id UUID PRIMARY KEY,
  provider TEXT,
  ref_time_local TIMESTAMPTZ,
  temp_c NUMERIC(5,2),
  precip_mm NUMERIC(6,2),
  wind_mps NUMERIC(5,2),
  wx_summary TEXT
);

CREATE TABLE enrich_avalanche (
  avalanche_context_id UUID PRIMARY KEY,
  provider TEXT,
  danger_rating TEXT,
  problems TEXT[],
  bulletin_url TEXT
);
```

Deterministic fields to aim for every time
- Dates: date_published, date_of_death or date_event_start and date_event_end
- SAR timeline: started_at, ended_at, op_type, agency
- Location: jurisdiction, geom, location_name, peak_name
- Counts: n_fatalities, party_size
- Cause: cause_primary and contributing_factors
- Provenance: quoted_evidence with exact sentences used for each field

---

## Agentic plan with LangGraph
Model the pipeline as a directed graph. Each node is an atomic tool. Retries and fallbacks are baked in. Human review can be inserted between nodes.

Nodes
1. Plan: expand user parameters into concrete discovery queries and region keyword packs.
2. Discover: Tavily and curated feeds search. Output candidate URLs with time and keyword scores.
3. Fetch: HTTP client with retry and robots respect. Store raw HTML in Cloud Storage.
4. Clean: trafilatura or readability-lxml to produce cleaned text and metadata.
5. Dedupe: URL canonicalization + text similarity (simhash or MinHash) to cluster stories.
6. Extract-deterministic: regex and rule-based extraction for dates, agencies, counts, obvious locations.
7. Extract-LLM: function-calling with a strict Pydantic schema; produce ISO dates and provenance quotes.
8. Geocode: gazetteer lookup within jurisdiction polygon; validate point-in-polygon.
9. Enrich: Meteostat and Avalanche Canada or NWAC lookup by time and zone.
10. Validate: cross-field checks (e.g., date_of_death ≤ date_published; geom inside region).
11. Persist: upsert into Postgres, store artifacts, update cluster links.
12. Review: queue if confidence below threshold or geocode ambiguous.
13. Summarize: short bullet summary and timeline paragraph from deterministic fields.

Retry and fallback rules
- Only invoke LLM if deterministic extraction confidence is low or fields missing.
- If geocode ambiguous, try alternate gazetteer or widen context to trailheads and parks.
- If dedupe uncertain, park in review rather than auto-merge.

LangGraph edge policy
- Edges carry a context dict with current artifacts: raw_html_uri, clean_text, regex_hits, llm_json, geo_candidate, validation_report.

---

## Extraction details
- Regex/rule layer: date phrases, “pronounced dead”, “recovery operation”, agency acronyms, patterns like “near”, “on”, “at” for location phrases.
- NER: spaCy for ORG and GPE to assist agency and place extraction.
- LLM contract: strict JSON schema with enumerations and ISO-8601 dates. Always request a list of Evidence(field, quote, source_offset) pairs for audit.
- Geocoding: Nominatim or Pelias with a region-bounded search. Peak matches prefer local gazetteers. Validate with point-in-polygon.
- Dedupe: cluster on rounded lat-lon, date window, and text fingerprint distance. Prefer earliest publication for canonical facts.

---

## Sources and connectors
- Primary: Tavily for web news. Parameterize by region and time.
- Secondary: Google News RSS, GDELT events filter, official press releases, SAR pages.
- Planned: Reddit and Facebook connectors after the backbone is stable. Respect rate limits and terms.

---

## Tech stack
- Language: Python 3.11
- Agent framework: LangGraph
- HTTP: httpx
- Parsing: trafilatura, beautifulsoup4 (targeted fixes)
- NER: spaCy
- Embeddings: sentence-transformers (Hugging Face) for clustering and near-duplicate checks
- LLM: OpenAI function-calling; Hugging Face models for offline summarization and embeddings as needed
- DB: Cloud SQL for PostgreSQL + PostGIS; DuckDB for local QA
- Object storage: GCS for raw HTML, cleaned text, JSON artifacts
- Jobs and queue: Cloud Run Jobs or Pub/Sub triggered workers; Cloud Scheduler for cron
- Secrets: Secret Manager
- Observability: Cloud Logging and Error Reporting; optional BigQuery sink

---

## API surface (FastAPI)
- POST /discover?jurisdiction=BC&years=10 enqueues discovery jobs.
- POST /ingest with { "url": "<news url>" } runs the full pipeline for a single story.
- GET /events filterable by region, date, activity, cause.
- GET /events/{id} with sources, SAR segments, enrichment.
- GET /export.csv or .parquet for downstream analysis.

---

## Local development quickstart
```bash
python -m venv .venv && source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt

# optional models
python -m spacy download en_core_web_sm

# env
cp .env.example .env.local

# run
make db-up && make migrate && make run
```

---

## GCP deployment sketch (no Terraform)
- Cloud SQL Postgres with PostGIS enabled.
- Cloud Storage bucket alpine-ledger-artifacts.
- Cloud Run service ingestion-api (FastAPI) with Secret Manager mounted for API keys.
- Pub/Sub topic news-discovery.
- Cloud Scheduler jobs per region that publish to Pub/Sub with payload { "jurisdiction": "BC", "years": 10 }.
- Cloud Run Job pipeline-worker that processes items from a pull subscription.

CI/CD with GitHub Actions
- Lint, unit tests, build container, gcloud run deploy, run migrations with Alembic.

---

## Testing and QA
- Build a 30 to 50 item golden set with mixed outlets and data richness.
- Unit tests for regex extractors.
- Integration tests for end-to-end ingest of a known URL with fixed outputs.
- Validation checks: date monotonicity, point-in-region, cluster stability across re-runs.
- Snapshot tests for cleaned HTML to detect parser regressions.

---

## Privacy and ethics
- Only store names if widely and publicly released. Otherwise store counts and demographics if disclosed.
- Short quotes for provenance only. Link to originals.
- Respect paywalls and robots. Prefer official releases for sensitive details.

---

## Roadmap
- Phase 1: DB schema, ingestion, cleaner, single URL processing, manual API.
- Phase 2: Regex extractor, geocoder, dedupe, minimal UI.
- Phase 3: LLM extraction with provenance, SAR timeline, weather and avalanche enrichment.
- Phase 4: Review queue UI, export, scheduled discovery per region.
- Phase 5: Reddit and Facebook connectors.

---

## Copilot-ready tasks
- Scaffold FastAPI app and config loader
- Alembic migrations for schema above
- Tavily client and discovery planner
- Fetcher with httpx + retry + robots check
- Cleaner with trafilatura, store raw and clean artifacts
- URL canonicalizer and text fingerprinting
- Regex and rule extractors with tests
- LLM function-calling schema and validator
- Geocoder service with region-bounded lookups and point-in-polygon check
- Enrichment adapters: Meteostat, Avalanche Canada, NWAC
- Dedupe and cluster merge logic
- Persister to Postgres with upsert semantics
- Validation report generator
- Review queue API and minimal UI
- CSV/Parquet export endpoint
- Scheduler wiring and Pub/Sub handlers
- Observability dashboard

---

## Example golden test case (fictional)
Use this as one of the first integration tests.

- Headline: Community remembers three climbers after avalanche near Cedar Valley
- Publisher: Harbor Times (Northfield Media)
- Byline: Jordan Kestrel
- Published date: 2024-07-21
- Incident: Avalanche on Raven Tooth Peak near Cedar Valley. Three mountaineers died. Missing since 2024-06-02. Recovery operation completed 2024-07-08.
- Agencies: Pine Valley Search and Rescue (PVSR), Provincial Police Service (PPS).
- Jurisdiction: BC
- Expected fields:
  - date_of_death: 2024-06-02 (or date_event_start 2024-06-02 if explicit date_of_death not stated)
  - sar_ops: search early June, suspension and resumption updates, recovery on 2024-07-08
  - location_name: Raven Tooth Peak, Aurora Provincial Park, near Cedar Valley
  - activity: alpinism or mountaineering
  - n_fatalities: 3

Links for the case
- Harbor Times coverage and syndications (record all URLs in sources)
- Evening News coverage
- Coastline Reporter recap
- Official Cedar Valley PPS or PVSR updates if available

---

## Minimal code interfaces

### Pydantic extraction payload
```python
from pydantic import BaseModel, Field, HttpUrl
from typing import List, Optional, Literal
from datetime import date, datetime

class Evidence(BaseModel):
    field: str
    quote: str
    source_offset: Optional[int] = None

class SARSegment(BaseModel):
    agency: Optional[str]
    op_type: Literal['search','recovery','rescue']
    started_at: Optional[datetime]
    ended_at: Optional[datetime]
    outcome: Optional[str]

class ExtractionPayload(BaseModel):
    jurisdiction: Literal['BC','AB','WA']
    location_name: Optional[str]
    peak_name: Optional[str]
    route_name: Optional[str]
    lat: Optional[float]
    lon: Optional[float]
    activity: Optional[Literal['alpinism','climbing','hiking','scrambling','ski-mountaineering','unknown']]
    cause_primary: Optional[str]
    n_fatalities: Optional[int]
    n_injured: Optional[int]
    party_size: Optional[int]
    date_event_start: Optional[date]
    date_event_end: Optional[date]
    date_of_death: Optional[date]
    sar: List[SARSegment] = []
    summary_bullets: List[str] = []
    evidence: List[Evidence] = []
    extraction_conf: float = Field(ge=0.0, le=1.0)
```

### LangGraph node signatures (sketch)
```python
class NodeContext(BaseModel):
    params: dict
    raw_html_uri: str | None = None
    clean_text: str | None = None
    regex_hits: dict = {}
    llm_json: dict | None = None
    geo_candidate: dict | None = None
    validation_report: dict | None = None
    cluster_key: str | None = None
```

---

## Future work
- Expand to near-miss incidents and injuries.
- Add name redaction policy controls.
- Add UI map with MapLibre and TanStack Table.
- Support BigQuery export for analytics.

---

## Notes
- Keep em dashes rare. Use ISO dates. Keep quotes short and necessary for provenance.
- Prefer deterministic extraction first. Use LLMs when the rule layer leaves gaps.
- Cache geocoding and weather. Control cost by batching where safe.

---

## People, Organizations, Relationships, Mentions (fictional example)

We will store people, orgs, roles, relationships, and mentions as first‑class entities. Postgres remains the source of truth; add a vector index later for retrieval.

### Worked example: Raven Tooth Peak avalanche (Farrow / Dalen), BC, 2024

Inputs: Harbor Times piece by Jordan Kestrel (Jul 21, 2024, Northfield Media) and a community fundraiser text provided by user.

### Deterministic extraction (event core)
```json
{
  "jurisdiction": "BC",
  "iso_country": "CA",
  "admin_area": "British Columbia",
  "location_name": "Raven Tooth Peak, Aurora Provincial Park, near Cedar Valley",
  "peak_name": "Raven Tooth Peak",
  "event_type": "fatality",
  "activity": "alpinism",
  "n_fatalities": 3,
  "date_event_start": "2024-06-02",
  "date_event_end": "2024-06-02",
  "date_of_death": "2024-06-02",
  "cause_primary": "avalanche",
  "contributing_factors": ["cornices (typical)","spring snowmelt/warming","steep terrain"],
  "phase": "descent",
  "tz_local": "America/Vancouver"
}
```

### SAR timeline (from narrative)
- 2024-06-02 evening — Police notified when party failed to return; Pine Valley SAR initiated search.
- Early June (several days) — Storm with rain/fog hampers access; ground teams reached Ridge Shelter; aircraft repeatedly grounded.
- ~1 week after disappearance — Weather clears; aerial reconnaissance resumes.
- 2024-07-08 — Recovery of three deceased mountaineers.
- Agencies involved: Pine Valley Search and Rescue (PVSR), North Fork SAR, Cedar Valley PPS, with provincial air support.

### People and roles (public, fictional)
- Alex Farrow — role: deceased (name_public=TRUE); hometown: Northfield; notes: experienced mountaineer.
- Mika Dalen — role: deceased (name_public=TRUE); aliases: “Mikael Dalen,” “Mik Dalen”; hometown: Northfield; profession: photographer.
- Unnamed third mountaineer — role: deceased (name_public=FALSE).
- Riley Shore — role: spokesperson / rescuer; affiliation: Pine Valley SAR; quote: “still winter in the high country.”
- Sam Calder — role: spokesperson; affiliation: North Fork SAR; observation about avalanche evidence.
- Jordan Kestrel — role: journalist; affiliation: Harbor Times / Northfield Media.

### Organizations (fictional)
- Pine Valley Search and Rescue (PVSR): type SAR.
- North Fork SAR: type SAR.
- Cedar Valley PPS: type Police.
- Harbor Times / Northfield Media: type Media.

### Community response (fictional)
- Fund purpose: Celebration of life + support for PVSR.
- Target: $10,000.
- Allocation: at least $5,000 to PVSR.
- Event: Celebration of Life on 2024-08-18, 13:30–17:30, Meadow Hall, Cedar Valley.

### Evidence snippets (provenance, fictional)
- “caught in a catastrophic avalanche on their descent.” (fundraiser)
- “bodies ... recovered on July 8.” (news & fundraiser)
- “team lead Riley Shore said it was ‘still winter in the high country.’” (news)
- “North Fork SAR coordinator Sam Calder ... saw evidence of avalanche activity.” (news)

### Example inserts (pseudo‑SQL; IDs generated in code, fictional)
```sql
-- people
INSERT INTO people (person_id, full_name, name_public, hometown) VALUES
  ('<id_farrow>', 'Alex Farrow', TRUE, 'Northfield, BC'),
  ('<id_dalen>', 'Mika Dalen', TRUE, 'Northfield, BC'),
  ('<id_unknown>', NULL, FALSE, NULL),
  ('<id_shore>', 'Riley Shore', TRUE, NULL),
  ('<id_calder>', 'Sam Calder', TRUE, NULL),
  ('<id_kestrel>', 'Jordan Kestrel', TRUE, NULL);

-- aliases
INSERT INTO person_alias (alias_id, person_id, alias, source) VALUES
  ('<id_d_alias1>','<id_dalen>','Mikael Dalen','fundraiser'),
  ('<id_d_alias2>','<id_dalen>','Mik Dalen','fundraiser');

-- orgs
INSERT INTO organizations (org_id, org_name, org_type) VALUES
  ('<id_pvsr>','Pine Valley Search and Rescue','SAR'),
  ('<id_nf>','North Fork SAR','SAR'),
  ('<id_pps>','Cedar Valley PPS','Police'),
  ('<id_media>','Harbor Times / Northfield Media','Media');

-- affiliations
INSERT INTO person_affiliation (person_id, org_id, title, valid_from) VALUES
  ('<id_shore>','<id_pvsr>','team lead', '2024-01-01'),
  ('<id_calder>','<id_nf>','air operations coordinator','2024-01-01');

-- event roles (assuming event_id = <id_event>)
INSERT INTO person_event_role (person_id, event_id, role) VALUES
  ('<id_farrow>','<id_event>','deceased'),
  ('<id_dalen>','<id_event>','deceased'),
  ('<id_unknown>','<id_event>','deceased'),
  ('<id_shore>','<id_event>','spokesperson'),
  ('<id_calder>','<id_event>','spokesperson'),
  ('<id_kestrel>','<id_event>','journalist');
```

---

## Dev helpers (new)
- Makefile targets:
  - make db-up — start local PostGIS
  - make migrate — run Alembic migrations
  - make seed — insert a sample event
  - make run — start FastAPI
- DB health endpoint: GET /db/health returns Postgres and PostGIS versions.

Quickstart refresher:
```bash
cp .env.example .env.local
make db-up && make migrate && make seed && make run
# then visit http://127.0.0.1:8000/db/health and /events
```

---

## Minimal ingest stub
- POST /ingest with { "url": "https://example.com/story" } creates a stub event and attaches the URL as a source.
- GET /events/{id} returns the event plus attached sources.

Example:
```bash
curl -X POST -H 'Content-Type: application/json' \
  -d '{"url":"https://example.com/story"}' \
  http://127.0.0.1:8000/ingest
```

---

- Ingest now fetches the URL, cleans content (trafilatura), and stores cleaned_text and scrape time in sources.

---

# Alpine Disasters

Alpine Disasters is a community project to carefully document fatal incidents in the mountains of British Columbia, Alberta, and Washington. Its purpose is to honor those who were lost, support those who rescue, and help others learn. By gathering what is publicly known in one place, we aim to reduce confusion, improve clarity, and foster understanding.

The system collects public reports, cleans and organizes them, and preserves concise facts with clear attribution to original sources. Each entry focuses on dates, places, agency actions, and conditions—never speculation—so that the record remains reliable over time. Geospatial context and simple summaries make the information easier to explore while keeping the story grounded in verifiable details.

This effort serves families, friends, rescuers, journalists, researchers, and policymakers. It offers an open API and exportable data so communities can study patterns, strengthen prevention, and support responsible storytelling. As the project matures, it will add careful enrichment such as weather and avalanche context to help situate events within their environment.

We work with humility and restraint. Names appear only when widely and publicly released. Short quotes are used solely for provenance, and paywalls and robots are respected. When uncertainty arises, the project favors caution and review over assumption. Feedback and corrections are welcome, and removal requests are handled with care.

Above all, this ledger is a quiet tribute—to those who ventured into high places, to those who searched, and to the communities who carry their memory. May the record help cultivate respect for the mountains, compassion for one another, and wisdom for the journeys ahead.
