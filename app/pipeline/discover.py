from __future__ import annotations

from typing import List, Dict, Any, Optional, Iterable
from dataclasses import dataclass
import os
import json
import itertools
import logging
from datetime import date, timedelta

import yaml
import requests

from app.config import get_tavily_api_key

try:
    from tavily import TavilyClient  # type: ignore
except Exception:  # pragma: no cover
    TavilyClient = None  # type: ignore

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclass
class SearchParams:
    jurisdiction: str
    years: int = 10
    activity: Optional[str] = None
    mode: str = "broad"  # "broad", "allowlist", or "both"
    max_results_per_query: int = 20


def _load_yaml() -> Dict[str, Any]:
    cfg_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "config", "search.yml")
    with open(cfg_path, "r") as f:
        return yaml.safe_load(f)


def _quote(tokens: Iterable[str]) -> str:
    return " OR ".join([f'"{t}"' if " " in t else t for t in tokens])


def _jurisdiction_full(j: str) -> str:
    return {"BC": "British Columbia", "AB": "Alberta", "WA": "Washington State"}.get(j, j)


def _country_for_juris(j: str) -> Optional[str]:
    if j == "WA":
        return "united states"
    if j in ("BC", "AB"):
        return "canada"
    return None


def _date_range_from_years(years: int) -> tuple[str, str]:
    yrs = max(1, years)
    end = date.today()
    start = end - timedelta(days=yrs * 365)
    return (start.isoformat(), end.isoformat())


def _time_range_from_years(years: int) -> str:
    # Tavily only accepts: day, week, month, year
    if years <= 0:
        return "month"
    return "year"


def build_queries(params: SearchParams) -> List[str]:
    cfg = _load_yaml()
    juris_tokens = cfg["jurisdictions"].get(params.jurisdiction, {}).get("tokens", [])
    acts: List[str] = cfg.get("activities", {}).get(params.activity, []) if params.activity else list(set(itertools.chain.from_iterable(cfg.get("activities", {}).values())))
    triggers: List[str] = cfg.get("fatality_triggers", [])
    queries: List[str] = []

    regions = _quote(juris_tokens)
    actors = _quote(acts)
    trig = _quote(triggers)

    # Actor + trigger + region
    queries.append(f"({actors}) AND ({trig}) AND ({regions})")
    # Actor + simple trigger
    queries.append(f"({actors}) AND (died OR dies OR fatal OR killed) AND ({regions})")
    # Recovery/body phrasing
    queries.append(f"({actors}) AND (body recovered OR recovery operation) AND ({regions})")
    # Region-only fatality phrasing (fallback)
    queries.append(f"({trig}) AND ({regions})")
    queries.append(f"(died OR dies OR fatal OR killed) AND ({regions})")

    # Semantic NL variant
    full = _jurisdiction_full(params.jurisdiction)
    act = params.activity or "mountain activities"
    queries.append(f"What fatal {act} incidents occurred in {full} in the last {params.years} years?")

    # Focused GoFundMe memorial pages (if domain is in allowlist)
    allowlist_raw = cfg.get("allowlist_sites", [])
    allow_domains = []
    for e in allowlist_raw:
        if isinstance(e, dict) and e.get("domain"):
            allow_domains.append(e["domain"])
        elif isinstance(e, str):
            allow_domains.append(e)
    if "gofundme.com" in allow_domains:
        memorial_trig = _quote(["in memory of", "memorial", "celebration of life", "tragically died", "fundraiser for family"])
        actor_terms = _quote(["climber", "alpinist", "mountaineer", "rock climber"])
        queries.append(f"site:gofundme.com ({actor_terms}) AND (died OR passed away OR {memorial_trig}) AND ({regions})")

    # Compact overly long boolean queries to avoid Tavily 400 (max 400 chars)
    simple_regions = _quote(juris_tokens[:5])
    simple_actors = _quote(acts[:5]) if acts else "climber OR hiker OR skier"
    compacted: List[str] = []
    for q in queries:
        qq = q
        if len(qq) > 380:
            # Prefer a simpler boolean form
            qq = f"({simple_actors}) AND ({trig}) AND ({simple_regions})"
            if len(qq) > 380:
                qq = f"({trig}) AND ({simple_regions})"
        compacted.append(qq)

    compacted = list(dict.fromkeys(q for q in compacted if q and q.strip()))
    logger.info("discovery.build_queries: jurisdiction=%s activity=%s -> %d queries", params.jurisdiction, params.activity, len(compacted))
    for i, q in enumerate(compacted, 1):
        logger.info("Q%d: %s", i, q)
    return compacted


def tavily_search(
    query: str,
    days_back: int,
    max_results: int,
    include_domains: Optional[List[str]] = None,
    exclude_domains: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    country: Optional[str] = None,
) -> List[Dict[str, Any]]:
    key = get_tavily_api_key()
    if not key:
        logger.warning("tavily_search: missing API key")
        return []

    time_range = _time_range_from_years(max(1, days_back // 365))

    if TavilyClient is not None:
        try:
            client = TavilyClient(key)
            logger.info("tavily_client: depth=advanced tr=%s include_domains=%s max=%d", time_range, bool(include_domains), max_results)
            kwargs = dict(
                query=query,
                topic="news",
                search_depth="advanced",
                include_answer="advanced",
                include_raw_content="text",
                include_domains=include_domains or None,
                exclude_domains=exclude_domains or None,
                max_results=max(1, min(max_results, 20)),
                start_date=start_date,
                end_date=end_date,
                country=country,
            )
            if not (start_date or end_date):
                kwargs["time_range"] = time_range
            resp = client.search(**kwargs)
            items = resp.get("results", []) if isinstance(resp, dict) else []
            logger.info("tavily_client: got %d results", len(items))
            return [
                {
                    "url": item.get("url"),
                    "title": item.get("title"),
                    "content": item.get("content"),
                    "published_date": item.get("published_date"),
                }
                for item in items
            ]
        except Exception as ex:
            logger.exception("tavily_client error: %s", ex)
            # fall through to HTTP

    # HTTP fallback
    url = "https://api.tavily.com/search"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {key}"}
    payload: Dict[str, Any] = {
        "query": query,
        "topic": "news",
        "search_depth": "advanced",
        "include_answer": "advanced",
        "include_raw_content": "text",
        "max_results": max(1, min(max_results, 20)),
    }
    if not (start_date or end_date):
        payload["time_range"] = time_range
    if include_domains:
        payload["include_domains"] = include_domains
    if exclude_domains:
        payload["exclude_domains"] = exclude_domains
    if start_date:
        payload["start_date"] = start_date
    if end_date:
        payload["end_date"] = end_date
    if country:
        payload["country"] = country
    try:
        logger.info("tavily_http: POST %s payload_keys=%s", url, list(payload.keys()))
        r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=20)
        logger.info("tavily_http: status=%s", r.status_code)
        r.raise_for_status()
        data = r.json()
        items = data.get("results", [])
        logger.info("tavily_http: got %d results", len(items))
        return [
            {
                "url": item.get("url"),
                "title": item.get("title"),
                "content": item.get("content"),
                "published_date": item.get("published_date"),
            }
            for item in items
        ]
    except Exception as ex:
        try:
            logger.error("tavily_http error: %s body=%s", ex, r.text if 'r' in locals() else None)
        except Exception:
            logger.error("tavily_http error: %s", ex)
        return []


def _dedupe(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out: List[Dict[str, Any]] = []
    for r in results:
        u = (r.get("url") or "").strip()
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(r)
    return out


def run_discovery(params: SearchParams) -> Dict[str, Any]:
    cfg = _load_yaml()
    queries = build_queries(params)
    days_back = params.years * 365
    start_iso, end_iso = _date_range_from_years(params.years)
    country = _country_for_juris(params.jurisdiction)

    results: List[Dict[str, Any]] = []

    # Extract domain names from allowlist_sites
    allowlist_raw = cfg.get("allowlist_sites", [])
    allowlist = []
    for entry in allowlist_raw:
        if isinstance(entry, dict) and "domain" in entry:
            allowlist.append(entry["domain"])
        elif isinstance(entry, str):
            allowlist.append(entry)

    exclude = cfg.get("exclude_domains", [])

    if params.mode in ("allowlist", "both") and allowlist:
        for q in queries:
            chunk = tavily_search(q, days_back=days_back, max_results=params.max_results_per_query, include_domains=allowlist, exclude_domains=exclude, start_date=start_iso, end_date=end_iso, country=country)
            logger.info("allowlist query returned %d", len(chunk))
            results.extend(chunk)

    if params.mode in ("broad", "both"):
        for q in queries:
            chunk = tavily_search(q, days_back=days_back, max_results=params.max_results_per_query, exclude_domains=exclude, start_date=start_iso, end_date=end_iso, country=country)
            logger.info("broad query returned %d", len(chunk))
            results.extend(chunk)

    results = _dedupe(results)
    logger.info("discovery.total_results=%d", len(results))
    return {"queries": queries, "items": results}
