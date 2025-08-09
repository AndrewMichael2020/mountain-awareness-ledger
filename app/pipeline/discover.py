from __future__ import annotations

from typing import List, Dict, Any, Optional, Iterable
from dataclasses import dataclass
import os
import json
import itertools
import logging
from datetime import date, timedelta
import time

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
    years: int
    activity: str | None = None
    mode: str = "both"
    max_results_per_query: int | None = None
    strict: bool = True


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
    *,
    days_back: int,
    max_results: int | None,
    include_domains: list[str] | None = None,
    exclude_domains: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    country: str | None = None,
) -> list[dict]:
    api_key = os.environ.get("TAVILY_API_KEY") or os.environ.get("TAVILY_APIKEY") or os.environ.get("TAVILY_KEY")
    if not api_key:
        logger.warning("tavily_search: missing TAVILY_API_KEY; returning empty list")
        return []
    url = os.environ.get("TAVILY_API_URL", "https://api.tavily.com/search")
    auth_style = os.environ.get("TAVILY_AUTH_STYLE", "header").lower()  # header | bearer | body

    # Normalize max_results (Tavily caps at 20)
    try:
        mr = int(max_results) if max_results is not None else 10
    except Exception:
        mr = 10
    mr = max(1, min(mr, 20))

    # Choose time_range; if explicit start/end present, omit time_range and use custom dates
    time_range = None
    if start_date and end_date:
        time_range = "custom"
    else:
        if days_back >= 365:
            time_range = "year"
        elif days_back >= 30:
            time_range = "month"
        elif days_back >= 7:
            time_range = "week"
        else:
            time_range = "day"

    payload = {
        "query": query,
        "search_depth": "advanced",
        "max_results": mr,
        "include_answer": "advanced",
        "include_raw_content": "text",
    }
    if country:
        payload["country"] = country
    if include_domains:
        payload["include_domains"] = include_domains
    if exclude_domains:
        payload["exclude_domains"] = exclude_domains

    if time_range == "custom":
        payload["start_date"] = start_date
        payload["end_date"] = end_date
    else:
        payload["time_range"] = time_range

    headers = {"Content-Type": "application/json"}
    if auth_style == "bearer":
        headers["Authorization"] = f"Bearer {api_key}"
    elif auth_style == "header":
        headers["x-api-key"] = api_key
    elif auth_style == "body":
        payload["api_key"] = api_key

    logger.info("tavily_http: POST %s auth_style=%s payload_keys=%s", url, auth_style, list(payload.keys()))
    timeout = float(os.environ.get("TAVILY_TIMEOUT", "20"))
    retries = int(os.environ.get("TAVILY_RETRIES", "2"))
    backoff = float(os.environ.get("TAVILY_BACKOFF", "2"))
    for attempt in range(retries + 1):
        try:
            r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=timeout)
            if r.status_code != 200:
                body = r.text[:300].replace("\n", " ") if hasattr(r, "text") else ""
                logger.error("tavily_http: status=%s attempt=%d/%d error_body=%s", r.status_code, attempt + 1, retries + 1, body)
                if r.status_code in (429, 500, 502, 503, 504) and attempt < retries:
                    time.sleep(backoff * (attempt + 1))
                    continue
                return []
            data = r.json() if hasattr(r, "json") else {}
            break
        except Exception as ex:
            logger.exception("tavily_http: request failed (attempt %d/%d): %s", attempt + 1, retries + 1, ex)
            if attempt < retries:
                time.sleep(backoff * (attempt + 1))
                continue
            return []

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


def _config_path() -> str:
    here = os.path.dirname(__file__)
    return os.path.normpath(os.path.join(here, "../../config/search.yml"))


# Built-in jurisdiction tokens used when config/search.yml omits them
_DEFAULT_JUR_TOKENS: dict[str, list[str]] = {
    "BC": [
        "british columbia", "bc", "coast mountains", "squamish", "whistler", "garibaldi",
        "vancouver", "north vancouver", "west vancouver", "north shore", "north shore mountains",
        "grouse mountain", "cypress mountain", "mount seymour", "sea to sky", "pemberton",
        "lions bay", "howe sound", "golden ears", "chilliwack", "fraser valley", "coquihalla",
        "manning park", "sunshine coast",
    ],
    "AB": [
        "alberta", "ab", "banff", "jasper", "kananaskis", "canmore", "rockies",
        "peter lougheed provincial park", "k-country", "calgary", "edmonton", "lake louise",
        "bow valley", "spray valley", "highway 40", "yamnuska", "mount yamanuska", "ghost",
        "waterton", "castle mountain", "crowsnest pass",
    ],
    "WA": [
        "washington", "washington state", "wa", "north cascades", "mount rainier", "rainier",
        "mount baker", "baker", "snohomish county", "del campo", "del campo peak", "monte christo",
    ],
}


def _jurisdiction_tokens(jur: str) -> list[str]:
    try:
        with open(_config_path(), "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        toks = cfg.get("jurisdictions", {}).get(jur, {}).get("tokens", []) or []
        toks = [t.lower() for t in toks]
        if toks:
            return toks
    except Exception:
        pass
    return _DEFAULT_JUR_TOKENS.get(jur, [])


def _matches_tokens(title: str | None, content: str | None, url: str | None, tokens: list[str]) -> bool:
    blob = " ".join([s or "" for s in (title, content, url)]).lower()
    return any(tok in blob for tok in tokens) if tokens else True


def run_discovery(params: SearchParams) -> dict:
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
    out = {"queries": queries, "items": results}
    items = out.get("items", []) or []
    if params.strict:
        toks = _jurisdiction_tokens(params.jurisdiction)
        if toks:
            items = [it for it in items if _matches_tokens(it.get("title"), it.get("content"), it.get("url"), toks)]
            out["filtered_count"] = len(items)
            out["filter_tokens"] = toks[:10]
    out["items"] = items
    return out
