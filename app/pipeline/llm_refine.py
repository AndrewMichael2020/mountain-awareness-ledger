from __future__ import annotations

from typing import Optional, List, Literal, Dict, Any, Tuple
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


def build_llm_context(srcs: List[Any], multi: bool = True) -> Tuple[str, Dict[str, Any]]:
    """Build combined context text and publication metadata (from the most relevant source)."""
    if not srcs:
        return "", {}
    # Pick the source with latest date_published, else fallback to the first with text
    candidate = None
    for s in sorted(srcs, key=lambda x: getattr(x, "date_published", None) or date.min, reverse=True):
        if getattr(s, "cleaned_text", None):
            candidate = s
            break
    candidate = candidate or next((s for s in srcs if getattr(s, "cleaned_text", None)), srcs[0])

    pubmeta = {
        "publisher": getattr(candidate, "publisher", None),
        "article_title": getattr(candidate, "article_title", None),
        "date_published": getattr(candidate, "date_published", None),
        "url": getattr(candidate, "url", None),
    }

    if multi:
        parts: List[str] = []
        for s in srcs:
            if not getattr(s, "cleaned_text", None):
                continue
            pub_str = f"Published: {s.date_published.isoformat()}" if getattr(s, "date_published", None) else ""
            header = f"Source: {s.publisher or ''} | {s.article_title or ''} | {s.url or ''} {pub_str}".strip()
            parts.append(f"{header}\n\n{s.cleaned_text}")
        combined_text = "\n\n---\n\n".join(parts) if parts else (getattr(candidate, "cleaned_text", "") or "")
    else:
        combined_text = getattr(candidate, "cleaned_text", "") or ""

    return combined_text, pubmeta


def refine_with_llm(cleaned_text: str, pubmeta: Dict[str, Any], current_event: Optional[Dict[str, Any]] = None) -> ExtractionPayload:
    """Use an LLM to fill gaps and fix obvious errors. Publication metadata is deterministic and provided for context only. current_event holds existing event fields for validation, not fallback."""
    api_key = os.environ.get("OPENAI_API_KEY")
    text_len = len(cleaned_text or "")
    model_name = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
    if not api_key or OpenAI is None or not cleaned_text:
        logger.warning("llm_refine: skipping LLM (key=%s, sdk=%s, text_len=%d)", bool(api_key), bool(OpenAI), text_len)
        return ExtractionPayload()

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

    cur_json = json.dumps(current_event or {}, default=str)
    pub_json = json.dumps(pubmeta or {}, default=str)

    prompt = {
        "role": "user",
        "content": (
            "Passage:\n```\n" + passage + "\n```\n\n" +
            "Publication metadata (deterministic, for reference only):\n" + pub_json + "\n\n" +
            "Current event fields (for validation; correct them if wrong or incomplete):\n" + cur_json + "\n\n" +
            "Instructions:\n"
            "- Output a flat JSON object with the schema keys only (no nested levels beyond required lists/objects like SAR segments).\n"
            "- Your output will OVERRIDE existing values in the event if you have higher confidence based on the passage.\n"
            "- Set extraction_conf to a number in [0,1] indicating overall confidence.\n"
            "- For each evidence quote you provide, append a space and (XX%) showing confidence for that field, e.g., '...sentence.' (87%).\n"
            "- You may also append (XX%) at the end of each summary_bullets entry.\n"
            "- Correct jurisdiction (BC/AB/WA), location_name, and infer the nearest named peak if present.\n"
            "- If a trail/route name is present (e.g., Pacific Crest Trail), set route_name accordingly.\n"
            "- Prefer the article's Published date year when normalizing event dates if the passage omits a year.\n"
            "- Set activity to one of: alpinism, climbing, hiking, scrambling, ski-mountaineering, unknown.\n"
            "- Determine n_fatalities and date_of_death from the passage if available.\n"
            "- Provide concise summary_bullets (3-6) and evidence quotes. Include at least one evidence quote for any field you set.\n"
            "- Populate SAR segments if mentioned.\n"
            "- Include categorized names (deceased, relatives, responders, spokespersons, medics).\n"
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
        if isinstance(parsed, dict):
            parsed = _normalize_parsed(parsed)
        # Normalize null arrays to empty lists to satisfy schema and coerce strings to lists
        if isinstance(parsed, dict):
            # Coerce bad jurisdiction strings to None
            jur = parsed.get("jurisdiction")
            if isinstance(jur, str) and jur.strip().lower() in {"", "null", "none", "unknown", "n/a"}:
                parsed["jurisdiction"] = None
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
        payload = ExtractionPayload()

    # Final sanitization
    payload.location_name = _sanitize_place(payload.location_name)
    payload.peak_name = _sanitize_place(payload.peak_name)
    return payload


def merge_event_fields(deterministic: Dict[str, Any], refined: ExtractionPayload) -> Dict[str, Any]:
    """Use only LLM-derived values for event fields; keep publication metadata deterministic elsewhere."""

    def only_refined(ref_val, sanitizer=lambda x: x):
        if ref_val is not None and ref_val != "" and ref_val != []:
            return sanitizer(ref_val)
        return None

    merged: Dict[str, Any] = {
        # core event fields (LLM authoritative)
        "jurisdiction": only_refined(refined.jurisdiction),
        "location_name": only_refined(refined.location_name, _sanitize_place),
        "peak_name": only_refined(refined.peak_name, _sanitize_place),
        "route_name": only_refined(getattr(refined, "route_name", None), _sanitize_place),
        "activity": only_refined(refined.activity),
        "cause_primary": only_refined(refined.cause_primary),
        "contributing_factors": only_refined(refined.contributing_factors or None) or None,
        "n_fatalities": only_refined(refined.n_fatalities),
        "n_injured": only_refined(getattr(refined, "n_injured", None)),
        "party_size": only_refined(getattr(refined, "party_size", None)),
        "date_event_start": only_refined(refined.date_event_start),
        "date_event_end": only_refined(refined.date_event_end),
        "date_of_death": only_refined(refined.date_of_death),
        # categorized names
        "names_all": only_refined(refined.names_all or None),
        "names_deceased": only_refined(refined.names_deceased or None),
        "names_relatives": only_refined(refined.names_relatives or None),
        "names_responders": only_refined(refined.names_responders or None),
        "names_spokespersons": only_refined(refined.names_spokespersons or None),
        "names_medics": only_refined(refined.names_medics or None),
        # source annotations (kept for audit on sources)
        "summary_bullets": refined.summary_bullets or None,
        "quoted_evidence": {
            "cause_primary": next((e.get("quote") if isinstance(e, dict) else getattr(e, "quote", None) for e in (refined.evidence or []) if (isinstance(e, dict) and e.get("field") == "cause_primary") or (getattr(e, "field", None) == "cause_primary")), None),
            "date_of_death": next((e.get("quote") if isinstance(e, dict) else getattr(e, "quote", None) for e in (refined.evidence or []) if (isinstance(e, dict) and e.get("field") == "date_of_death") or (getattr(e, "field", None) == "date_of_death")), None),
            "n_fatalities": next((e.get("quote") if isinstance(e, dict) else getattr(e, "quote", None) for e in (refined.evidence or []) if (isinstance(e, dict) and e.get("field") == "n_fatalities") or (getattr(e, "field", None) == "n_fatalities")), None),
            "location_name": next((e.get("quote") if isinstance(e, dict) else getattr(e, "quote", None) for e in (refined.evidence or []) if (isinstance(e, dict) and e.get("field") == "location_name") or (getattr(e, "field", None) == "location_name")), None),
        },
        # SAR segments
        "sar": [s.model_dump() if hasattr(s, "model_dump") else s for s in (refined.sar or [])],
    }
    return merged


from typing import Any, Dict

ALLOWED_JURS = {"BC", "AB", "WA"}
ALLOWED_ACTIVITIES = {
    "alpinism",
    "climbing",
    "hiking",
    "scrambling",
    "ski-mountaineering",
    "unknown",
}

_SYN_ACTIVITY = {
    "heli-skiing": "ski-mountaineering",
    "heli skiing": "ski-mountaineering",
    "skiing": "ski-mountaineering",
    "backcountry skiing": "ski-mountaineering",
    "bc skiing": "ski-mountaineering",
    "mountaineering": "alpinism",
    "alpine climbing": "alpinism",
    "rock climbing": "climbing",
    "ice climbing": "climbing",
    "scramble": "scrambling",
}


def _coerce_activity(val: Any) -> str | None:
    if val is None:
        return None
    if isinstance(val, str):
        v = val.strip().lower()
        v = _SYN_ACTIVITY.get(v, v)
        if v in ALLOWED_ACTIVITIES:
            return v
        return "unknown"
    return None


def _normalize_parsed(parsed: Dict[str, Any]) -> Dict[str, Any]:
    # Jurisdiction: drop anything outside allowed
    jur = parsed.get("jurisdiction")
    if isinstance(jur, str):
        ju = jur.strip().upper()
        if ju not in ALLOWED_JURS:
            parsed["jurisdiction"] = None
        else:
            parsed["jurisdiction"] = ju
    elif jur is not None:
        parsed["jurisdiction"] = None

    # Activity: map synonyms, clamp to allowed
    act = parsed.get("activity")
    parsed["activity"] = _coerce_activity(act)

    # Evidence: ensure list[dict]
    ev = parsed.get("evidence") or parsed.get("quoted_evidence")
    if ev is not None:
        fixed = []
        if isinstance(ev, list):
            for item in ev:
                if isinstance(item, dict):
                    fixed.append(item)
                elif isinstance(item, str):
                    # Drop freeform strings to avoid schema errors
                    continue
        elif isinstance(ev, dict):
            fixed = [ev]
        else:
            fixed = []
        parsed["evidence"] = fixed
        # Maintain alias field if model expects quoted_evidence
        parsed["quoted_evidence"] = fixed

    # Summary bullets: list[str]
    sb = parsed.get("summary_bullets")
    if sb is None:
        parsed["summary_bullets"] = []
    elif isinstance(sb, str):
        parsed["summary_bullets"] = [sb.strip()]
    elif isinstance(sb, list):
        parsed["summary_bullets"] = [str(x).strip() for x in sb if x is not None]
    else:
        parsed["summary_bullets"] = []

    # Names lists: coerce to list[str]
    for k in (
        "names_all",
        "names_deceased",
        "names_relatives",
        "names_responders",
        "names_spokespersons",
        "names_medics",
        "contributing_factors",
    ):
        val = parsed.get(k)
        if val is None:
            parsed[k] = []
        elif isinstance(val, str):
            parsed[k] = [val.strip()]
        elif isinstance(val, list):
            parsed[k] = [str(x).strip() for x in val if x is not None]
        else:
            parsed[k] = []

    # SAR segments: list[dict]
    sar = parsed.get("sar")
    if sar is None:
        parsed["sar"] = []
    elif isinstance(sar, dict):
        parsed["sar"] = [sar]
    elif isinstance(sar, list):
        parsed["sar"] = [x for x in sar if isinstance(x, dict)]
    else:
        parsed["sar"] = []

    return parsed
