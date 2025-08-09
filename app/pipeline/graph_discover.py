from __future__ import annotations

from typing import Dict, Any, List
from dataclasses import dataclass
import logging

from sqlalchemy.orm import Session
from langgraph.graph import StateGraph, END

from .discover import SearchParams, run_discovery
from .graph import run_ingest_graph_url
from ..repo import get_latest_source_for_event, update_event_fields, update_source_annotations, delete_sar_ops_for_event, insert_sar_segments, get_event_with_sources
from .llm_refine import refine_with_llm, merge_event_fields


logger = logging.getLogger(__name__)

@dataclass
class DGState:
    params: SearchParams
    queries: List[str]
    items: List[Dict[str, Any]]
    selected_urls: List[str]
    results: List[Dict[str, Any]]
    event_ids: List[str]
    stats: Dict[str, Any]


def run_discover_graph(db: Session, params: SearchParams, max_urls: int = 10, augment: bool = False) -> Dict[str, Any]:
    sg = StateGraph(dict)

    def _merge(state: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
        new_state = dict(state or {})
        new_state.update(updates or {})
        return new_state

    def plan_node(state: Dict[str, Any]) -> Dict[str, Any]:
        return _merge(state, {"params": params, "augment": augment})

    def discover_node(state: Dict[str, Any]) -> Dict[str, Any]:
        out = run_discovery(state["params"])  # use params from state
        return _merge(state, {"queries": out.get("queries", []), "items": out.get("items", [])})

    def select_node(state: Dict[str, Any]) -> Dict[str, Any]:
        items = state.get("items", [])
        urls: List[str] = []
        seen = set()
        for it in items:
            u = (it.get("url") or "").strip()
            if not u or u in seen:
                continue
            seen.add(u)
            urls.append(u)
            if len(urls) >= max_urls:
                break
        return _merge(state, {"selected_urls": urls})

    def ingest_node(state: Dict[str, Any]) -> Dict[str, Any]:
        urls: List[str] = state.get("selected_urls", [])
        results: List[Dict[str, Any]] = []
        event_ids: List[str] = []
        for u in urls:
            try:
                out = run_ingest_graph_url(db, url=u)
                eid = out.get("event_id") if isinstance(out, dict) else None
                if eid:
                    event_ids.append(str(eid))
                results.append({"url": u, **(out or {})})
            except Exception as ex:
                results.append({"url": u, "error": str(ex)})
        return _merge(state, {"results": results, "event_ids": event_ids})

    def augment_node(state: Dict[str, Any]) -> Dict[str, Any]:
        if not state.get("augment"):
            return state
        augmented: List[str] = []
        aug_errors: List[Dict[str, Any]] = []
        for eid in state.get("event_ids", []) or []:
            try:
                from uuid import UUID as _UUID
                uid = _UUID(eid)
                ev, srcs = get_event_with_sources(db, uid)
                if not ev:
                    logger.warning("augment: event not found %s", eid)
                    continue
                parts = []
                for s in srcs or []:
                    if not getattr(s, "cleaned_text", None):
                        continue
                    header = f"Source: {s.publisher or ''} | {s.article_title or ''} | {s.url or ''}".strip()
                    parts.append(f"{header}\n\n{s.cleaned_text}")
                combined_text = "\n\n---\n\n".join(parts)
                logger.info("augment: event=%s sources=%d context_len=%d", eid, len(srcs or []), len(combined_text or ""))
                if not combined_text:
                    continue
                deterministic = {
                    "jurisdiction": ev.jurisdiction,
                    "location_name": ev.location_name,
                    "peak_name": ev.peak_name,
                    "activity": ev.activity,
                    "cause_primary": getattr(ev, "cause_primary", None),
                    "contributing_factors": getattr(ev, "contributing_factors", None),
                    "n_fatalities": ev.n_fatalities,
                    "date_event_start": getattr(ev, "date_event_start", None),
                    "date_event_end": getattr(ev, "date_event_end", None),
                    "date_of_death": ev.date_of_death,
                    "names_all": getattr(ev, "names_all", None),
                    "names_deceased": getattr(ev, "names_deceased", None),
                    "names_relatives": getattr(ev, "names_relatives", None),
                    "names_responders": getattr(ev, "names_responders", None),
                    "names_spokespersons": getattr(ev, "names_spokespersons", None),
                    "names_medics": getattr(ev, "names_medics", None),
                }
                refined = refine_with_llm(combined_text, deterministic)
                merged = merge_event_fields(deterministic, refined)
                update_event_fields(db, uid, merged)
                if merged.get("summary_bullets") or merged.get("quoted_evidence"):
                    latest = get_latest_source_for_event(db, uid)
                    if latest:
                        update_source_annotations(db, latest.source_id, quoted_evidence=merged.get("quoted_evidence"), summary_bullets=merged.get("summary_bullets"))
                if merged.get("sar"):
                    delete_sar_ops_for_event(db, uid)
                    insert_sar_segments(db, uid, merged["sar"]) 
                augmented.append(eid)
            except Exception as ex:
                db.rollback()
                logger.exception("augment: failed for %s: %s", eid, ex)
                aug_errors.append({"event_id": eid, "error": str(ex)})
        return _merge(state, {"augmented_ids": augmented, "augment_errors": aug_errors})

    def summarize_node(state: Dict[str, Any]) -> Dict[str, Any]:
        stats = {
            "n_queries": len(state.get("queries", [])),
            "n_found": len(state.get("items", [])),
            "n_selected": len(state.get("selected_urls", [])),
            "n_results": len(state.get("results", [])),
            "n_ingested": len(state.get("event_ids", [])),
            "n_augmented": len(state.get("augmented_ids", [])) if state.get("augment") else 0,
            "n_augment_errors": len(state.get("augment_errors", [])) if state.get("augment") else 0,
        }
        return _merge(state, {"stats": stats})

    sg.add_node("plan", plan_node)
    sg.add_node("discover", discover_node)
    sg.add_node("select", select_node)
    sg.add_node("ingest", ingest_node)
    sg.add_node("augment", augment_node)
    sg.add_node("summarize", summarize_node)

    sg.set_entry_point("plan")
    sg.add_edge("plan", "discover")
    sg.add_edge("discover", "select")
    sg.add_edge("select", "ingest")
    sg.add_edge("ingest", "augment")
    sg.add_edge("augment", "summarize")
    sg.add_edge("summarize", END)

    graph = sg.compile()
    final = graph.invoke({})

    return {
        "queries": final.get("queries", []),
        "selected_urls": final.get("selected_urls", []),
        "results": final.get("results", []),
        "event_ids": final.get("event_ids", []),
        "augmented_ids": final.get("augmented_ids", []),
        "augment_errors": final.get("augment_errors", []),
        "stats": final.get("stats", {}),
    }
