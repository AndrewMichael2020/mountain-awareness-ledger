from __future__ import annotations

from typing import Optional, List, Literal, Dict, Any
from datetime import date
import os
import re
import json
import logging

from pydantic import BaseModel, Field

try:
    from openai import OpenAI  # modern SDK
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore


logger = logging.getLogger(__name__)


def _sanitize_place(s: Optional[str]) -> Optional[str]:
    if not s:
        return s
    # strip ", near ..." and collapse whitespace/newlines
    s2 = re.sub(r",\s*near\b.*$", "", s, flags=re.IGNORECASE | re.DOTALL)
    s2 = re.sub(r"\s+", " ", s2).strip()
    s2 = s2.strip(" ,;-")
    return s2 or None


class Evidence(BaseModel):
    field: str
    quote: str
    source_offset: Optional[int] = None


class SARSegment(BaseModel):
    agency: Optional[str] = None
    op_type: Literal["search", "recovery", "rescue"]
    started_at: Optional[date] = None
    ended_at: Optional[date] = None
    outcome: Optional[str] = None


class ExtractionPayload(BaseModel):
    jurisdiction: Optional[Literal["BC", "AB", "WA"]] = None
    location_name: Optional[str] = None
    peak_name: Optional[str] = None
    route_name: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    activity: Optional[Literal["alpinism", "climbing", "hiking", "scrambling", "ski-mountaineering", "unknown"]] = None
    cause_primary: Optional[str] = None
    contributing_factors: List[str] = Field(default_factory=list)
    n_fatalities: Optional[int] = None
    n_injured: Optional[int] = None
    party_size: Optional[int] = None
    date_event_start: Optional[date] = None
    date_event_end: Optional[date] = None
    date_of_death: Optional[date] = None
    sar: List[SARSegment] = Field(default_factory=list)
    summary_bullets: List[str] = Field(default_factory=list)
    evidence: List[Evidence] = Field(default_factory=list)
    extraction_conf: float = Field(0.0, ge=0.0, le=1.0)
    # Categorized names
    names_all: List[str] = Field(default_factory=list)
    names_deceased: List[str] = Field(default_factory=list)
    names_relatives: List[str] = Field(default_factory=list)
    names_responders: List[str] = Field(default_factory=list)
    names_spokespersons: List[str] = Field(default_factory=list)
    names_medics: List[str] = Field(default_factory=list)
    # Source-level overrides
    publisher: Optional[str] = None
    article_title: Optional[str] = None
    date_published: Optional[date] = None


def refine_with_llm(cleaned_text: str, deterministic: Dict[str, Any]) -> ExtractionPayload:
    """Use an LLM to fill gaps and fix obvious errors. If OPENAI_API_KEY missing, return minimal payload."""
    api_key = os.environ.get("OPENAI_API_KEY")
    text_len = len(cleaned_text or "")
    model_name = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
    if not api_key or OpenAI is None or not cleaned_text:
        logger.warning("llm_refine: skipping LLM (key=%s, sdk=%s, text_len=%d)", bool(api_key), bool(OpenAI), text_len)
        return ExtractionPayload(
            jurisdiction=deterministic.get("jurisdiction"),
            location_name=_sanitize_place(deterministic.get("location_name")),
            peak_name=_sanitize_place(deterministic.get("peak_name")),
            activity=deterministic.get("activity") or "unknown",
            date_of_death=deterministic.get("date_of_death"),
            n_fatalities=deterministic.get("n_fatalities"),
            extraction_conf=0.0,
        )

    logger.info("llm_refine: invoking model=%s text_len=%d", model_name, text_len)
    client = OpenAI(api_key=api_key)

    system = (
        "You are an alpine-incident information extractor. "
        "Extract only facts present in the passage. If a field is unknown, leave it null. "
        "Prefer Canadian/US mountain contexts; do not invent places. "
        "Return STRICT JSON matching the schema keys."
    )

    # Clip to ~8000 chars to stay under limits
    passage = cleaned_text[:8000]

    prompt = {
        "role": "user",
        "content": (
            "Passage:\n```\n" + passage + "\n```\n\n" +
            "Deterministic extraction (may be incomplete):\n" + json.dumps(deterministic, default=str) + "\n\n" +
            "Instructions:\n"
            "- Your output will OVERRIDE existing values: if you can improve or correct, do so.\n"
            "- Correct jurisdiction (BC/AB/WA), location_name, and infer the nearest named peak if present.\n"
            "- If a trail/route name is present (e.g., Pacific Crest Trail), set route_name accordingly.\n"
            "- Prefer the article's Published date year when normalizing event dates if the passage omits a year.\n"
            "- Set activity to one of: alpinism, climbing, hiking, scrambling, ski-mountaineering, unknown.\n"
            "- Determine n_fatalities and date_of_death from the passage if available.\n"
            "- Provide concise summary_bullets (3-6) and evidence quotes. Include at least one evidence quote for any field you set.\n"
            "- Populate SAR segments if mentioned.\n"
            "- Include categorized names (deceased, relatives, responders, spokespersons, medics).\n"
            "- Also output publisher and article_title if apparent from the passage or URL.\n"
        ),
    }

    try:
        resp = client.chat.completions.create(
            model=model_name,
            temperature=0.2,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system},
                prompt,
            ],
        )
        content = resp.choices[0].message.content
        parsed = json.loads(content)
        # Normalize null arrays to empty lists to satisfy schema and coerce strings to lists
        if isinstance(parsed, dict):
            list_keys = [
                "contributing_factors",
                "summary_bullets",
                "evidence",
                "sar",
                "names_all",
                "names_deceased",
                "names_relatives",
                "names_responders",
                "names_spokespersons",
                "names_medics",
            ]
            for k in list_keys:
                val = parsed.get(k)
                if val is None:
                    parsed[k] = []
                elif k in (
                    "contributing_factors",
                    "summary_bullets",
                    "names_all",
                    "names_deceased",
                    "names_relatives",
                    "names_responders",
                    "names_spokespersons",
                    "names_medics",
                ):
                    # Coerce single string to list, and normalize list elements to strings
                    if isinstance(val, str):
                        parsed[k] = [val.strip()]
                    elif isinstance(val, list):
                        parsed[k] = [str(x).strip() for x in val if x is not None]
                elif k == "evidence":
                    # If a single dict, wrap; if string, drop to avoid schema error
                    if isinstance(val, dict):
                        parsed[k] = [val]
                    elif isinstance(val, str):
                        parsed[k] = []
                    elif isinstance(val, list):
                        parsed[k] = val
                elif k == "sar":
                    if isinstance(val, dict):
                        parsed[k] = [val]
                    elif isinstance(val, list):
                        parsed[k] = val
            # Safety: if evidence accidentally a dict after prior logic
            if isinstance(parsed.get("evidence"), dict):
                parsed["evidence"] = [parsed["evidence"]]
        payload = ExtractionPayload(**parsed)
        logger.info("llm_refine: parsed payload with keys=%s", list(parsed.keys()))
    except Exception as ex:
        logger.exception("llm_refine: error during LLM call or parse: %s", ex)
        payload = ExtractionPayload(
            jurisdiction=deterministic.get("jurisdiction"),
            location_name=_sanitize_place(deterministic.get("location_name")),
            peak_name=_sanitize_place(deterministic.get("peak_name")),
            activity=deterministic.get("activity") or "unknown",
            date_of_death=deterministic.get("date_of_death"),
            n_fatalities=deterministic.get("n_fatalities"),
            extraction_conf=0.0,
        )

    # Final sanitization
    payload.location_name = _sanitize_place(payload.location_name)
    payload.peak_name = _sanitize_place(payload.peak_name)
    return payload


def merge_event_fields(deterministic: Dict[str, Any], refined: ExtractionPayload) -> Dict[str, Any]:
    """Prefer refined values when present; fall back to deterministic; include source overrides."""

    def prefer_refined(ref_val, det_val, sanitizer=lambda x: x):
        if ref_val is not None and ref_val != "" and ref_val != []:
            return sanitizer(ref_val)
        return det_val

    merged: Dict[str, Any] = {
        "jurisdiction": prefer_refined(refined.jurisdiction, deterministic.get("jurisdiction")),
        "location_name": prefer_refined(refined.location_name, deterministic.get("location_name"), _sanitize_place),
        "peak_name": prefer_refined(refined.peak_name, deterministic.get("peak_name"), _sanitize_place),
        "route_name": prefer_refined(getattr(refined, "route_name", None), deterministic.get("route_name"), _sanitize_place),
        "activity": prefer_refined(refined.activity, deterministic.get("activity")),
        "cause_primary": prefer_refined(refined.cause_primary, deterministic.get("cause_primary")),
        "contributing_factors": prefer_refined(refined.contributing_factors or None, deterministic.get("contributing_factors")),
        "n_fatalities": prefer_refined(refined.n_fatalities, deterministic.get("n_fatalities")),
        "date_event_start": prefer_refined(refined.date_event_start, deterministic.get("date_event_start")),
        "date_event_end": prefer_refined(refined.date_event_end, deterministic.get("date_event_end")),
        "date_of_death": prefer_refined(refined.date_of_death, deterministic.get("date_of_death")),
        # categorized names
        "names_all": prefer_refined(refined.names_all or None, deterministic.get("names_all")),
        "names_deceased": prefer_refined(refined.names_deceased or None, deterministic.get("names_deceased")),
        "names_relatives": prefer_refined(refined.names_relatives or None, deterministic.get("names_relatives")),
        "names_responders": prefer_refined(refined.names_responders or None, deterministic.get("names_responders")),
        "names_spokespersons": prefer_refined(refined.names_spokespersons or None, deterministic.get("names_spokespersons")),
        "names_medics": prefer_refined(refined.names_medics or None, deterministic.get("names_medics")),
        # source annotations
        "summary_bullets": refined.summary_bullets or None,
        "quoted_evidence": {
            "cause_primary": next((e.get("quote") if isinstance(e, dict) else getattr(e, "quote", None) for e in (refined.evidence or []) if (isinstance(e, dict) and e.get("field") == "cause_primary") or (getattr(e, "field", None) == "cause_primary")), None),
            "date_of_death": next((e.get("quote") if isinstance(e, dict) else getattr(e, "quote", None) for e in (refined.evidence or []) if (isinstance(e, dict) and e.get("field") == "date_of_death") or (getattr(e, "field", None) == "date_of_death")), None),
            "n_fatalities": next((e.get("quote") if isinstance(e, dict) else getattr(e, "quote", None) for e in (refined.evidence or []) if (isinstance(e, dict) and e.get("field") == "n_fatalities") or (getattr(e, "field", None) == "n_fatalities")), None),
            "location_name": next((e.get("quote") if isinstance(e, dict) else getattr(e, "quote", None) for e in (refined.evidence or []) if (isinstance(e, dict) and e.get("field") == "location_name") or (getattr(e, "field", None) == "location_name")), None),
        },
        # SAR segments
        "sar": [s.model_dump() if hasattr(s, "model_dump") else s for s in (refined.sar or [])],
        # source overrides
        "source_publisher": refined.publisher,
        "source_title": refined.article_title,
        "source_date_published": refined.date_published,
    }
    return merged
