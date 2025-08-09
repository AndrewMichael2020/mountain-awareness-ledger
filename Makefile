URL ?= https://example.com/news

.PHONY: install db-up db-down migrate seed run dev

DB_SERVICE=db
PORT ?= 8000
HOST ?= 0.0.0.0

install:
	pip install -U pip
	pip install -r requirements.txt

db-up:
	docker compose up -d $(DB_SERVICE)

db-down:
	docker compose down $(DB_SERVICE)

migrate:
	alembic upgrade head

seed:
	PYTHONPATH=. python scripts/seed_local.py

run:
	uvicorn app.main:app --host $(HOST) --port $(PORT) --reload

dev: install db-up migrate seed run

ingest:
	curl -s -X POST http://localhost:8000/ingest -H "content-type: application/json" \
		-d '{"url":"$(URL)"}' | jq

# Run the provided Tavily JSON sample through the deterministic pipeline
sample:
	PYTHONPATH=. python scripts/run_sample.py samples/gulka_tavily.json

# Aggregated view across all sample results
sample-agg:
	PYTHONPATH=. python scripts/run_sample.py samples/gulka_tavily.json --aggregate

# Quick health check against a running server
health:
	curl -s http://localhost:8000/health | jq
