from __future__ import annotations

from typing import List, Dict, Any, Optional, Iterable
from dataclasses import dataclass
import os
import json
import itertools

import yaml
import requests

from app.config import get_tavily_api_key


@dataclass
class SearchParams:
    jurisdiction: str
    years: int = 10
    activity: Optional[str] = None
    mode: str = "broad"  # "broad", "allowlist", or "both"
    max_results_per_query: int = 8


def _load_yaml() -> Dict[str, Any]:
    cfg_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "config", "search.yml")
    with open(cfg_path, "r") as f:
        return yaml.safe_load(f)


def _quote(tokens: Iterable[str]) -> str:
    return " OR ".join([f'"{t}"' if " " in t else t for t in tokens])


def build_queries(params: SearchParams) -> List[str]:
    cfg = _load_yaml()
    juris_tokens = cfg["jurisdictions"].get(params.jurisdiction, {}).get("tokens", [])
    acts: List[str] = cfg.get("activities", {}).get(params.activity, []) if params.activity else list(set(itertools.chain.from_iterable(cfg.get("activities", {}).values())))
    triggers: List[str] = cfg.get("fatality_triggers", [])
    queries: List[str] = []

    regions = _quote(juris_tokens)
    actors = _quote(acts)
    trig = _quote(triggers)

    queries.append(f"({actors}) AND ({trig}) AND ({regions})")
    queries.append(f"({actors}) AND (died OR fatal OR killed) AND ({regions})")
    queries.append(f"({actors}) AND (body recovered OR recovery operation) AND ({regions})")
    return list(dict.fromkeys(queries))


def tavily_search(query: str, days_back: int, max_results: int, include_domains: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    key = get_tavily_api_key()
    if not key:
        return []
    url = "https://api.tavily.com/search"
    headers = {"Content-Type": "application/json", "X-API-Key": key}
    payload: Dict[str, Any] = {
        "query": query,
        "topic": "news",
        "days": max(1, min(days_back, 3650)),
        "max_results": max(1, min(max_results, 20)),
        "search_depth": "basic",
        "include_answer": False,
    }
    if include_domains:
        payload["include_domains"] = include_domains
    try:
        r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=20)
        r.raise_for_status()
        data = r.json()
        return [
            {
                "url": item.get("url"),
                "title": item.get("title"),
                "content": item.get("content"),
                "published_date": item.get("published_date"),
            }
            for item in data.get("results", [])
        ]
    except Exception:
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

    results: List[Dict[str, Any]] = []

    allowlist = cfg.get("allowlist_sites", [])

    if params.mode in ("allowlist", "both"):
        for q in queries:
            results.extend(tavily_search(q, days_back=days_back, max_results=params.max_results_per_query, include_domains=allowlist))

    # Fallback broad if allowlist empty or yielded nothing
    if (params.mode in ("broad", "both")) and not results:
        for q in queries:
            results.extend(tavily_search(q, days_back=days_back, max_results=params.max_results_per_query))

    results = _dedupe(results)
    return {"queries": queries, "items": results}
