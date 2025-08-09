#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from alpine import clean, extract_det, fetch
from alpine.config import DATA_DIR, USER_AGENT, TIMEOUT_S


def _load_tavily_json(path: Path) -> Dict[str, Any]:
    raw = path.read_text(encoding="utf-8")
    # drop // comment lines to tolerate annotated samples
    lines = [ln for ln in raw.splitlines() if not ln.strip().startswith("//")]
    return json.loads("\n".join(lines))


def main() -> None:
    ap = argparse.ArgumentParser(description="Run deterministic extract on a Tavily sample JSON.")
    ap.add_argument("sample", type=Path, help="Path to samples/*.json from Tavily")
    ap.add_argument("--aggregate", action="store_true", help="Print a merged event JSON from all results")
    args = ap.parse_args()

    payload = _load_tavily_json(args.sample)
    if not payload.get("results"):
        raise SystemExit("No results[] in sample JSON")

    results = payload["results"]
    print(f"Found {len(results)} results in sample")
    per_source: List[Dict[str, Any]] = []
    for idx, item in enumerate(results, start=1):
        url = item.get("url") or ""
        raw_content = (item.get("raw_content") or "").strip()

        # If Tavily provided real content, persist and extract directly.
        fetched = None
        if not raw_content or raw_content in {"...", "[truncated]"}:
            try:
                fetched = fetch.get(url, ua=USER_AGENT, timeout_s=TIMEOUT_S)
                raw_html = fetched["raw_html"]
                clean_obj = clean.clean_html(raw_html)
                sha = fetched["sha256"]
                final_url = fetched["final_url"]
                meta = clean.persist_artifacts(DATA_DIR, sha, final_url, raw_html, clean_obj)
            except Exception as e:
                print(f"[{idx}] FAIL fetch: {url} :: {e}")
                continue
        else:
            # Treat raw_content as ready-to-clean text
            clean_obj = {"text": raw_content, "title": None, "author": None, "published": None}
            sha = hashlib.sha256(raw_content.encode("utf-8")).hexdigest()
            meta = clean.persist_artifacts(DATA_DIR, sha, url, raw_content, clean_obj)

        det = extract_det.extract_core_fields(clean_obj["text"], meta.get("published"))
        # carry the source's published date into the record for better aggregation
        det["published_dt"] = meta.get("published")
        per_source.append(det)

        print(f"[{idx}] OK processed")
        print(f"URL: {meta['url']}")
        print(f"Artifact folder: {meta['folder']}")
        print("Deterministic extract:")
        for k in ["n_fatalities", "activity", "cause_primary", "date_of_death", "date_event_start", "date_event_end"]:
            print(f"  {k}: {det.get(k)}")
        if det.get("summary_bullets"):
            print("  bullets:")
            for b in det["summary_bullets"][:5]:
                print(f"    - {b}")

    if args.aggregate and per_source:
        # Simple merge: prefer earliest event date, max fatalities, union factors, pick non-null for others
        def pick_first(keys: List[str]) -> Optional[Any]:
            for d in per_source:
                for k in keys:
                    v = d.get(k)
                    if v:
                        return v
            return None

        event_dates = [d.get("date_event_start") for d in per_source if d.get("date_event_start")]
        event_date = min(event_dates) if event_dates else None
        # pick recovery date as earliest date between event_date and published_dt (if available)
        cand = []
        for d in per_source:
            r = d.get("date_recovery")
            if not r:
                continue
            pub = d.get("published_dt")
            if event_date and r < event_date:
                continue
            if pub is not None:
                try:
                    pub_date = pub.date() if hasattr(pub, "date") else None
                except Exception:
                    pub_date = None
                if pub_date and r > pub_date:
                    continue
            cand.append(r)
        recovery_date = min(cand) if cand else None
        fatalities = [int(d["n_fatalities"]) for d in per_source if d.get("n_fatalities") is not None]
        n_fatalities = max(fatalities) if fatalities else None
        contributing = []
        for d in per_source:
            cf = d.get("contributing_factors") or []
            for x in cf:
                if x not in contributing:
                    contributing.append(x)

        # prefer alpinism if any source has it
        activity = pick_first(["activity"]) or None
        if any(d.get("activity") == "alpinism" for d in per_source):
            activity = "alpinism"

        # enhance location_name if any source indicates Garibaldi Park
        base_loc = pick_first(["location_name"]) or ""
        if base_loc and "Garibaldi Provincial Park" not in base_loc and any(
            (d.get("location_name") and "Garibaldi" in str(d.get("location_name"))) for d in per_source
        ):
            base_loc = base_loc.replace("Atwell Peak", "Atwell Peak, Garibaldi Provincial Park")

        # time to recovery in days
        ttr_days = None
        if event_date and recovery_date:
            try:
                delta = (recovery_date - event_date).days
                if delta >= 0:
                    ttr_days = delta
            except Exception:
                pass

        merged = {
            "jurisdiction": pick_first(["jurisdiction"]),
            "iso_country": pick_first(["iso_country"]),
            "admin_area": pick_first(["admin_area"]),
            "location_name": base_loc or None,
            "peak_name": pick_first(["peak_name"]),
            "event_type": pick_first(["event_type"]) or ("fatality" if (n_fatalities and n_fatalities > 0) else None),
            "activity": activity,
            "n_fatalities": n_fatalities,
            "date_event_start": event_date,
            "date_event_end": event_date,
            "date_of_death": event_date,
            "date_recovery": recovery_date,
            "cause_primary": pick_first(["cause_primary"]),
            "contributing_factors": contributing or None,
            "phase": pick_first(["phase"]),
            "tz_local": pick_first(["tz_local"]),
            "multi_agency": any(d.get("multi_agency") for d in per_source) or None,
            "time_to_recovery_days": ttr_days,
        }

        print("\nAggregated event summary:")
        # default=str to serialize date objects
        print(json.dumps(merged, default=str, indent=2))


if __name__ == "__main__":
    main()
