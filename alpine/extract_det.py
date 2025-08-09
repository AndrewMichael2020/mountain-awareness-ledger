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


def _sar_segments(text: str, published_dt: datetime | None):
    segs = []
    # Recovery
    rec_date = (
        _explicit_date_with_keywords(text, ["recovered", "recovery", "located", "found"], published_dt)
        or _date_near(text, ["recovered", "recovery", "located", "found", "bodies"], ref_dt=published_dt)
    )
    if rec_date:
        segs.append({
            "op_type": "recovery",
            "started_at": _to_dt(rec_date),
            "ended_at": None,
            "agency": None,
            "outcome": "recovered",
        })

    # Search began/resumed/suspended
    verbs = ["began", "resumed", "initiated", "launched", "started", "suspended", "paused", "continued"]
    month_day = re.compile(rf"({MONTHS})\s+\d{{1,2}}(?:,\s*(20\d{{2}}))?", re.I)

    for m in re.finditer(r"search.{0,100}", text, flags=re.I):
        span = m.span()
        window = text[span[0]: span[1] + 120]
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
                    elif published_dt:
                        sdate = datetime.strptime(f"{mon} {int(d)}, {published_dt.year}", "%B %d, %Y").date()
                except Exception:
                    sdate = None
            if sdate:
                outcome = None
                if vhit in ["suspended", "paused"]:
                    outcome = "suspended"
                elif vhit in ["resumed", "continued"]:
                    outcome = "resumed"
                segs.append({
                    "op_type": "search",
                    "started_at": _to_dt(sdate),
                    "ended_at": None,
                    "agency": None,
                    "outcome": outcome,
                })
                break

    # Rescue (optional): "rescued on <date>" / "airlifted"
    res_date = _explicit_date_with_keywords(text, ["rescued", "airlifted", "evacuated"], published_dt)
    if res_date:
        segs.append({
            "op_type": "rescue",
            "started_at": _to_dt(res_date),
            "ended_at": None,
            "agency": None,
            "outcome": "rescued",
        })

    for m in re.finditer(r"\b([A-Z][a-z]+)\s+(\d{1,2})(?:,\s*(\d{4}))?\b", text):
        try:
            mon, d, y = m.groups()
            if not y and published_dt:
                y = str(published_dt.year)
            if not y:
                continue
            try:
                dt = datetime.strptime(f"{mon} {d}, {y}", "%B %d, %Y")
            except Exception:
                continue
            segs.append({
                "op_type": "recovery",
                "started_at": dt,
                "ended_at": None,
                "outcome": "recovered",
                "agency": None,
                "notes": None,
            })
        except Exception:
            continue
    return segs


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
    """Deterministic extraction is limited to pubmeta elsewhere. Delegate enrichment to LLM.

    Returning an empty dict prevents heuristic field writes during ingest; the LLM
    augmentation flow will populate fields like activity, cause, dates, names, SAR, etc.
    """
    return {}
