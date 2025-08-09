from __future__ import annotations

import re
from datetime import datetime, date
from typing import Dict, Any, Optional, List, Tuple

try:  # optional, improves parsing of partial dates like "May 31"
    from dateparser import parse as _dateparse  # type: ignore
except Exception:  # pragma: no cover
    _dateparse = None  # type: ignore


MONTHS = (
    "January|February|March|April|May|June|July|August|September|October|November|December"
)
RE_ISO = re.compile(r"\b(20\d{2})-(\d{2})-(\d{2})\b")
RE_LONG = re.compile(rf"\b({MONTHS})\s+(\d{{1,2}})(?:,\s*(20\d{{2}}))?\b", re.I)

# New: generic location patterns
RE_PEAK = re.compile(r"\b(?:Mount|Mt\.?|Peak|Pinnacle|Butte|Spire|Col|Couloir|Glacier|Pass)\s+([A-Z][A-Za-z\-]+(?:\s+[A-Z][A-Za-z\-]+){0,3})")
RE_PARK = re.compile(r"\b([A-Z][A-Za-z\-]+(?:\s+[A-Z][A-ZaZh\-]+){0,3})\s+(Provincial|National|State)\s+Park\b")
RE_PLACE_NEAR = re.compile(r"\b(?:near|in|at|on|above|below|around)\s+([A-Z][A-Za-z\-]+(?:\s+[A-Z][A-Za-z\-]+){0,3})")
SOCIAL_NOISE = {"Facebook", "Twitter", "X", "Instagram", "YouTube", "Email", "SMS", "Reddit"}

# New: jurisdiction mapping by region cues
JURIS_CUES = [
    ("BC", {
        "tokens": ["british columbia", "b.c.", "garibaldi", "kootenay", "okanagan", "squamish", "whistler", "vancouver"],
        "iso": "CA", "admin": "British Columbia", "tz": "America/Vancouver"
    }),
    ("AB", {
        "tokens": ["alberta", "banff", "jasper", "kananaskis", "canmore", "calgary", "peter lougheed", "lougheed provincial park"],
        "iso": "CA", "admin": "Alberta", "tz": "America/Edmonton"
    }),
    ("WA", {
        "tokens": ["washington state", "mt. rainier", "mount rainier", "olympic national park", "north cascades", "seattle", "bellingham", "everett"],
        "iso": "US", "admin": "Washington", "tz": "America/Los_Angeles"
    }),
]

# New: expanded activity and cause vocab
ACTIVITY_MAP = [
    ("alpinism", ["mountaineer", "alpinist", "alpinism", "mountaineering", "alpine route"]),
    ("climbing", ["climbing", "rock climb", "ice climb", "mixed climb", "scramble", "scrambling"]),
    ("ski-mountaineering", ["backcountry ski", "ski touring", "skied", "splitboard", "skin track", "avalanche terrain"]),
    ("hiking", ["hike", "hiking", "trail", "trek"]),
]

CAUSE_MAP = [
    ("avalanche", ["avalanche", "slab release", "cornice collapse", "loose wet", "storm slab", "wind slab", "serac collapse"]),
    ("fall", ["fell", "fall", "slipped", "plunged", "tumbled"]),
    ("rockfall", ["rockfall", "fell rocks", "rock slide", "stonefall"]),
    ("crevasse", ["crevasse", "snow bridge"]),
    ("hypothermia", ["hypothermia", "exposure"]),
    ("drowning", ["drowned", "drowning", "river crossing"]),
    ("lightning", ["lightning", "struck by lightning"]),
    ("tree well", ["tree well"]),
]


def _all_dates_with_spans(s: str, ref_dt: Optional[datetime] = None) -> List[Tuple[date, Tuple[int, int]]]:
    res: List[Tuple[date, Tuple[int, int]]] = []
    for m in RE_ISO.finditer(s):
        y, mo, d = m.groups()
        try:
            res.append((date(int(y), int(mo), int(d)), m.span()))
        except Exception:
            pass
    for m in RE_LONG.finditer(s):
        mon, d, y = m.groups()
        try:
            if y:
                dt = datetime.strptime(f"{mon} {int(d)}, {int(y)}", "%B %d, %Y").date()
            else:
                if ref_dt is not None:
                    yr = ref_dt.year
                    dt = datetime.strptime(f"{mon} {int(d)}, {yr}", "%B %d, %Y").date()
                else:
                    if _dateparse is not None:
                        p = _dateparse(f"{mon} {int(d)}")
                        if p:
                            dt = p.date()
                        else:
                            continue
                    else:
                        continue
            res.append((dt, m.span()))
        except Exception:
            continue
    return res


def _date_near(text: str, keywords: List[str], penalize_published: bool = True, ref_dt: Optional[datetime] = None) -> Optional[date]:
    """Pick a date associated with incident/recovery keywords.

    Scores dates within +/- 150 chars of any keyword; penalizes 'published'/'updated' contexts.
    """
    t = text
    pairs = _all_dates_with_spans(t, ref_dt=ref_dt)
    if not pairs:
        return None
    best: Tuple[int, date] | None = None
    for dt, (a, b) in pairs:
        center = (a + b) // 2
        window_start = max(0, center - 150)
        window_end = min(len(t), center + 150)
        window = t[window_start:window_end].lower()
        score = 0
        # keyword proximity
        for kw in keywords:
            if kw in window:
                score += 3
        # nearby action words
        if any(w in window for w in ["avalanche", "descent", "missing", "disappeared", "failed to return", "search", "rescue", "recovered", "recovery", "bodies", "pronounced dead", "killed", "died"]):
            score += 1
        # penalize published/updated contexts
        if penalize_published and any(w in window for w in ["published", "updated", "posted"]):
            score -= 4
        # tie-breaker: prefer earlier dates if similar score
        score2 = (score * 10_000) - int(dt.strftime("%s"))
        if best is None or score2 > best[0]:
            best = (score2, dt)
    return best[1] if best else None


def _first_date(text: str, ref_dt: Optional[datetime]) -> Optional[date]:
    pairs = _all_dates_with_spans(text, ref_dt=ref_dt)
    return pairs[0][0] if pairs else None


def _explicit_date_with_keywords(text: str, keywords: List[str], ref_dt: Optional[datetime]) -> Optional[date]:
    """Find dates tightly bound to specific keywords, favoring patterns like 'recovered on July 8'."""
    t = text
    month_day = re.compile(rf"({MONTHS})\s+\d{{1,2}}(?:,\s*(20\d{{2}}))?", re.I)
    for kw in keywords:
        # Look for 'kw ... Month Day[, Year]' within a small window
        for m in re.finditer(rf"{kw}.{{0,40}}", t, flags=re.I):
            span = m.span()
            window = t[span[1]: span[1] + 60]
            md = month_day.search(window)
            if md:
                mon = md.group(1)
                # reconstruct the matched date string
                rest = md.group(0)[len(mon):].strip()
                date_str = f"{mon} {rest}"
                try:
                    if "," in date_str or any(ch.isdigit() for ch in date_str):
                        if ref_dt and "," not in date_str:
                            yr = ref_dt.year
                            dt = datetime.strptime(f"{md.group(1)} {re.search(r'\d+', rest).group(0)}, {yr}", "%B %d, %Y").date()
                        else:
                            # let dateparser handle if present, else try strict
                            if _dateparse:
                                p = _dateparse(date_str)
                                if p:
                                    return p.date()
                            # fallback: attempt strict with current/unknown year not supported
                            continue
                        return dt
                except Exception:
                    continue
        # Look for 'Month Day[, Year] ... kw'
        for m in month_day.finditer(t):
            md_span = m.span()
            window = t[md_span[1]: md_span[1] + 40].lower()
            if kw.lower() in window:
                try:
                    mon, d, y = m.groups()
                    if y:
                        return datetime.strptime(f"{mon} {int(d)}, {int(y)}", "%B %d, %Y").date()
                    if ref_dt:
                        yr = ref_dt.year
                        return datetime.strptime(f"{mon} {int(d)}, {yr}", "%B %d, %Y").date()
                except Exception:
                    continue
    return None


def _num_from_words_or_digits(segment: str) -> Optional[int]:
    words = {
        "one": 1,
        "two": 2,
        "three": 3,
        "four": 4,
        "five": 5,
        "six": 6,
        "seven": 7,
        "eight": 8,
        "nine": 9,
        "ten": 10,
    }
    seg = segment.lower()
    for w, n in words.items():
        if re.search(rf"\b{w}\b", seg):
            return n
    m = re.search(r"\b(\d{1,2})\b", seg)
    if m:
        return int(m.group(1))
    return None


def _to_dt(d: date) -> datetime:
    return datetime(d.year, d.month, d.day)


def _phase_from_text(t_lower: str) -> Optional[str]:
    # Broadened phase cues
    if any(p in t_lower for p in ["on the descent", "on descent", "descending", "descent", "after summiting", "after summit", "heading down", "returning", "on return"]):
        return "descent"
    if any(p in t_lower for p in ["on the ascent", "on ascent", "ascending", "ascent", "approach", "en route to", "on the way up", "summit bid"]):
        return "ascent"
    if any(p in t_lower for p in ["on the summit", "at the summit", "summiting", "summit day"]):
        return "summit"
    return None


def _extract_location(text: str) -> Tuple[Optional[str], Optional[str]]:
    """Return (peak_name, location_name) assembled from peak/park/place phrases."""
    t = text or ""
    peak = None
    park = None
    near = None
    m = RE_PEAK.search(t)
    if m:
        head = m.group(0)
        peak = head.strip()
    mp = RE_PARK.search(t)
    if mp:
        park = f"{mp.group(1)} {mp.group(2)} Park"
    mn = RE_PLACE_NEAR.search(t)
    if mn:
        cand = mn.group(1).strip()
        if cand not in SOCIAL_NOISE:
            near = cand
    location_name = None
    parts: List[str] = []
    if peak:
        parts.append(peak)
    if park:
        parts.append(park)
    if near and (peak or park):
        parts.append(f"near {near}")
    if parts:
        location_name = ", ".join(parts)
    return peak, location_name


def _jurisdiction(text: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    t = (text or "").lower()
    best: tuple[int, str, str, str, str] | None = None  # (score, code, iso, admin, tz)
    for code, meta in JURIS_CUES:
        score = 0
        for tok in meta["tokens"]:
            # count occurrences; multi-word tokens benefit
            score += t.count(tok)
        if score > 0:
            cand = (score, code, meta["iso"], meta["admin"], meta["tz"])
            if best is None or cand[0] > best[0]:
                best = cand
    if best:
        _, code, iso, admin, tz = best
        return code, iso, admin, tz
    return None, None, None, None


def _sar_segments(text: str, ref_dt: Optional[datetime]) -> List[Dict[str, Any]]:
    t = text or ""
    segments: List[Dict[str, Any]] = []

    # Recovery
    rec_date = (
        _explicit_date_with_keywords(t, ["recovered", "recovery", "located", "found"], ref_dt)
        or _date_near(t, ["recovered", "recovery", "located", "found", "bodies"], ref_dt=ref_dt)
    )
    if rec_date:
        segments.append({
            "op_type": "recovery",
            "started_at": _to_dt(rec_date),
            "ended_at": None,
            "agency": None,
            "outcome": "recovered",
        })

    # Search began/resumed/suspended
    verbs = ["began", "resumed", "initiated", "launched", "started", "suspended", "paused", "continued"]
    month_day = re.compile(rf"({MONTHS})\s+\d{{1,2}}(?:,\s*(20\d{{2}}))?", re.I)

    for m in re.finditer(r"search.{0,100}", t, flags=re.I):
        span = m.span()
        window = t[span[0]: span[1] + 120]
        vhit = None
        for v in verbs:
            if v in window.lower():
                vhit = v
                break
        if vhit:
            md = month_day.search(window)
            sdate = None
            if md:
                mon, d, y = md.groups()
                try:
                    if y:
                        sdate = datetime.strptime(f"{mon} {int(d)}, {int(y)}", "%B %d, %Y").date()
                    elif ref_dt:
                        sdate = datetime.strptime(f"{mon} {int(d)}, {ref_dt.year}", "%B %d, %Y").date()
                except Exception:
                    sdate = None
            if sdate:
                outcome = None
                if vhit in ["suspended", "paused"]:
                    outcome = "suspended"
                elif vhit in ["resumed", "continued"]:
                    outcome = "resumed"
                segments.append({
                    "op_type": "search",
                    "started_at": _to_dt(sdate),
                    "ended_at": None,
                    "agency": None,
                    "outcome": outcome,
                })
                break

    # Rescue (optional): "rescued on <date>" / "airlifted"
    res_date = _explicit_date_with_keywords(t, ["rescued", "airlifted", "evacuated"], ref_dt)
    if res_date:
        segments.append({
            "op_type": "rescue",
            "started_at": _to_dt(res_date),
            "ended_at": None,
            "agency": None,
            "outcome": "rescued",
        })

    return segments


def evidence_snippets(text: str) -> Dict[str, str]:
    """Return short supporting sentences for key fields if present in text."""
    t = text or ""
    rules = {
        "cause_primary": r"(?i)(catastrophic\s+)?avalanche|slab|cornice|rockfall|fell|fall|crevasse|hypothermia",
        "date_of_death": r"(?i)(on|the (morning|evening) of)\s+[A-Z][a-z]{2,8}\s+\d{1,2}(,\s*\d{4})?",
        "search_started": r"(?i)(search|rescue)\s+(began|started|launched|initiated|resumed|continued|suspended)",
        "recovery": r"(?i)(recovered|recovery|located|found)\b",
    }

    def _sentence_around(idx: int) -> str:
        start = t.rfind(".", 0, idx)
        start_q = t.rfind("\n", 0, idx)
        if start_q > start:
            start = start_q
        end = t.find(".", idx)
        end_n = t.find("\n", idx)
        if end == -1 or (end_n != -1 and end_n < end):
            end = end_n
        if start == -1:
            start = 0
        else:
            start += 1
        if end == -1:
            end = len(t)
        return t[start:end].strip()

    snippets: Dict[str, str] = {}
    for field, pat in rules.items():
        m = re.search(pat, t)
        if m:
            snippets[field] = _sentence_around(m.start())
    return snippets


def extract_core_fields(text: str, published_dt: Optional[datetime]) -> Dict[str, Any]:
    """Deterministic extraction from text with generalized heuristics."""
    t = text or ""
    t_lower = t.lower()

    # Fatalities
    n_fatalities = None
    fat_patterns = [
        r"\b(\w+|\d+)\s+(?:men|women|people|persons|climbers|mountaineers|skiers|hikers)\s+(?:killed|dead|deceased|lost|missing|perished)\b",
        r"\b(pronounced dead|died|killed)\b.{0,30}\b(\w+|\d+)\b",
        r"\bbodies?\b.{0,10}\b(\w+|\d+)\b",
        r"\b(\w+|\d+)\s+(?:bodies|victims|fatalities)\b",
    ]
    for pat in fat_patterns:
        m = re.search(pat, t_lower)
        if m:
            n = _num_from_words_or_digits(m.group(m.lastindex or 1))
            if n:
                n_fatalities = n
                break

    # Activity
    activity = None
    for label, keys in ACTIVITY_MAP:
        if any(k in t_lower for k in keys):
            activity = label
            break

    # Cause
    cause_primary = None
    for label, keys in CAUSE_MAP:
        if any(k in t_lower for k in keys):
            cause_primary = label
            break

    # Location & jurisdiction
    peak_name, location_name = _extract_location(t)
    jurisdiction, iso_country, admin_area, tz_local = _jurisdiction(t)

    # Phase
    phase = _phase_from_text(t_lower)

    # Contributing factors
    contributing_factors: List[str] = []
    if "cornice" in t_lower:
        contributing_factors.append("cornices (typical)")
    if any(w in t_lower for w in ["warming", "spring snowmelt", "spring conditions", "heat wave"]):
        contributing_factors.append("spring snowmelt/warming")
    if any(w in t_lower for w in ["steep", "steep terrain", "steep faces", "volcanic", "icefall", "serac"]):
        contributing_factors.append("steep/technical terrain")

    # Dates
    event_date = _date_near(t, ["avalanche", "disappeared", "descent", "missing", "failed to return", "last seen", "accident"], ref_dt=published_dt) or _first_date(t, ref_dt=published_dt)
    recovery_date = (
        _explicit_date_with_keywords(t, ["recovered", "recovery", "bodies", "located", "found"], published_dt)
        or _date_near(t, ["recovered", "recovery", "bodies", "located", "found"], ref_dt=published_dt)
        or None
    )
    date_of_death = event_date
    date_event_start = event_date
    date_event_end = event_date

    time_to_recovery_days = None
    if event_date and recovery_date:
        try:
            delta = (recovery_date - event_date).days
            if delta >= 0:
                time_to_recovery_days = delta
        except Exception:
            pass

    # Agencies (generic signals)
    agencies_found: List[str] = []
    if any(x in t_lower for x in ["search and rescue", " sar ", " s.a.r "]):
        agencies_found.append("Search and Rescue")
    if any(x in t_lower for x in ["rcmp", "police", "sheriff", "state patrol", "mounties", "park rangers", "nps"]):
        agencies_found.append("Law/Agency")
    multi_agency = len(agencies_found) >= 2

    # Event type
    event_type = "fatality" if (n_fatalities and n_fatalities > 0) or ("bodies" in t_lower and "recovered" in t_lower) else None

    # Summary bullets
    bullets = []
    if n_fatalities is not None:
        bullets.append(f"Fatalities: {n_fatalities}")
    if cause_primary:
        bullets.append(f"Cause: {cause_primary}")
    if activity:
        bullets.append(f"Activity: {activity}")
    if published_dt:
        bullets.append(f"Published: {published_dt.date().isoformat()}")
    if event_date:
        bullets.append(f"Event date: {event_date.isoformat()}")
    if recovery_date:
        bullets.append(f"Recovery date: {recovery_date.isoformat()}")

    sar = _sar_segments(t, published_dt)

    # Evidence snippets for quick provenance
    quotes = evidence_snippets(t)

    return {
        "jurisdiction": jurisdiction,
        "iso_country": iso_country,
        "admin_area": admin_area,
        "location_name": location_name,
        "peak_name": peak_name,
        "event_type": event_type,
        "activity": activity,
        "n_fatalities": n_fatalities,
        "date_event_start": event_date,
        "date_event_end": event_date,
        "date_of_death": date_of_death,
        "date_recovery": recovery_date,
        "cause_primary": cause_primary,
        "contributing_factors": contributing_factors or None,
        "phase": phase,
        "tz_local": tz_local,
        "agencies_found": agencies_found or None,
        "multi_agency": multi_agency,
        "time_to_recovery_days": time_to_recovery_days,
        "summary_bullets": bullets,
        "sar": sar,
        "quoted_evidence": quotes or None,
    }
