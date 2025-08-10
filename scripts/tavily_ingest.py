#!/usr/bin/env python3
"""
Query Tavily via the official client (header style handled internally), then POST URLs to /ingest/batch.
Usage:
  python scripts/tavily_ingest.py --jurisdiction BC --years 3 --max 6
Requires:
  - TAVILY_API_KEY in env or .env.local
  - Local API running at http://127.0.0.1:8000
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import List

try:
    from tavily import TavilyClient
except Exception as e:  # pragma: no cover
    print("Missing tavily-python; please install dependencies.", file=sys.stderr)
    raise

import requests
from dotenv import load_dotenv


def build_query(jurisdiction: str) -> tuple[str, str]:
    juris = jurisdiction.upper()
    if juris == "BC":
        return "(climber OR mountaineer OR hiker OR skier) AND (died OR fatal OR killed OR avalanche) AND (British Columbia OR BC)", "canada"
    if juris == "AB":
        return "(climber OR mountaineer OR hiker OR skier) AND (died OR fatal OR killed OR avalanche) AND (Alberta OR AB)", "canada"
    if juris == "WA":
        return "(climber OR mountaineer OR hiker OR skier) AND (died OR fatal OR killed OR avalanche) AND (Washington State OR WA)", "united states"
    raise SystemExit(f"Unknown jurisdiction: {jurisdiction}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--jurisdiction", "-j", choices=["BC", "AB", "WA"], default="BC")
    ap.add_argument("--years", "-y", type=int, default=3)
    ap.add_argument("--max", "-m", type=int, default=6)
    ap.add_argument("--api", default="http://127.0.0.1:8000")
    args = ap.parse_args()

    # Load env (supports .env.local)
    load_dotenv(".env.local")
    api_key = os.getenv("TAVILY_API_KEY")
    if not api_key:
        raise SystemExit("TAVILY_API_KEY not set in env/.env.local")

    query, country = build_query(args.jurisdiction)
    time_range = "year" if args.years >= 1 else "month"

    client = TavilyClient(api_key)
    resp = client.search(
        query=query,
        search_depth="advanced",
        time_range=time_range,
        max_results=min(max(args.max, 1), 10),  # keep it small/polite
        country=country,
        include_domains=None,
    )
    results = resp.get("results", []) if isinstance(resp, dict) else []
    urls: List[str] = []
    for r in results:
        u = r.get("url")
        if u and u not in urls:
            urls.append(u)

    print(f"Tavily results: {len(urls)}", file=sys.stderr)
    if not urls:
        print("No URLs to ingest.")
        return

    payload = {"urls": urls[: args.max]}
    r = requests.post(
        f"{args.api}/ingest/batch",
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload),
        timeout=30,
    )
    try:
        body = r.json()
    except Exception:
        body = {"status": r.status_code, "body": r.text[:400]}
    print(json.dumps(body, indent=2))


if __name__ == "__main__":
    main()
