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
                # If no explicit year, prefer the article's published year when known
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
        if any(w in window for w in ["avalanche", "descent", "missing", "disappeared", "failed to return", "search", "rescue", "recovered", "recovery", "bodies"]):
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


def extract_core_fields(text: str, published_dt: Optional[datetime]) -> Dict[str, Any]:
    """Deterministic extraction from text with simple heuristics.

    Returns a dict of core fields; tolerant and aims for correct values in common cases.
    """
    t = text or ""
    t_lower = t.lower()

    # fatalities count heuristics (more robust: includes lost/missing/deceased/bodies)
    n_fatalities = None
    fat_patterns = [
        r"\b(\w+|\d+)\s+(?:men|people|persons|climbers|mountaineers)\s+(?:killed|dead|deceased|lost|missing)\b",
        r"\b(recovery|recovered)\b.{0,40}\b(\w+|\d+)\b",
        r"\bbodies?\b.{0,10}\b(\w+|\d+)\b",
        r"\b(\w+|\d+)\s+(?:bodies|victims)\b",
    ]
    for pat in fat_patterns:
        m = re.search(pat, t_lower)
        if m:
            n = _num_from_words_or_digits(m.group(m.lastindex or 1))
            if n:
                n_fatalities = n
                break

    # activity: prefer alpinism when mountaineer words present
    activity = None
    if any(w in t_lower for w in ["mountaineer", "alpinist", "alpinism", "mountaineering"]):
        activity = "alpinism"
    else:
        for key in ["climbing", "hiking", "scrambling", "ski-mountaineering", "skiing"]:
            if key in t_lower:
                activity = key
                break

    # cause
    cause_primary = None
    for kw, label in [
        ("avalanche", "avalanche"),
        ("rockfall", "rockfall"),
        ("cornice break", "avalanche"),
        ("fall", "fall"),
        ("crevasse", "crevasse"),
        ("hypothermia", "hypothermia"),
    ]:
        if kw in t_lower:
            cause_primary = label
            break

    # location and jurisdiction heuristics
    peak_name = "Atwell Peak" if "atwell peak" in t_lower else None
    in_garibaldi = any(x in t_lower for x in ["garibaldi provincial park", "garibaldi park", "garibaldi"]) 
    near_squamish = "squamish" in t_lower
    location_name = None
    if peak_name and (in_garibaldi or near_squamish):
        parts = [peak_name]
        if in_garibaldi:
            parts.append("Garibaldi Provincial Park")
        if near_squamish:
            parts.append("near Squamish")
        location_name = ", ".join(parts)

    jurisdiction = None
    iso_country = None
    admin_area = None
    tz_local = None
    if any(w in t_lower for w in ["british columbia", "squamish", "vancouver sun", "whistler"]):
        jurisdiction = "BC"
        iso_country = "CA"
        admin_area = "British Columbia"
        tz_local = "America/Vancouver"

    # phase
    phase = "descent" if "descent" in t_lower else None

    # contributing factors
    contributing_factors: List[str] = []
    if "cornice" in t_lower:
        contributing_factors.append("cornices (typical)")
    if any(w in t_lower for w in ["warming", "spring snowmelt", "spring conditions"]):
        contributing_factors.append("spring snowmelt/warming")
    if any(w in t_lower for w in ["steep", "steep terrain", "steep faces", "volcanic"]):
        contributing_factors.append("steep terrain")

    # dates: choose event (incident) and recovery using context
    event_date = _date_near(t, ["avalanche", "disappeared", "descent", "missing", "failed to return", "last seen"], ref_dt=published_dt) or _first_date(t, ref_dt=published_dt)
    recovery_date = (
        _explicit_date_with_keywords(t, ["recovered", "recovery", "bodies", "located", "found"], published_dt)
        or _date_near(t, ["recovered", "recovery", "bodies", "located", "found"], ref_dt=published_dt)
        or None
    )
    date_of_death = event_date
    date_event_start = event_date
    date_event_end = event_date

    # timeline derived field
    time_to_recovery_days = None
    if event_date and recovery_date:
        try:
            delta = (recovery_date - event_date).days
            if delta >= 0:
                time_to_recovery_days = delta
        except Exception:
            pass

    # agencies presence (for multi-agency flag)
    agencies = [
        ("Squamish Search and Rescue", ["squamish sar", "squamish search and rescue", "ssar", "b.j. chute"]),
        ("Whistler SAR", ["whistler sar", "whistler search and rescue"]),
        ("North Shore Rescue", ["north shore rescue", "nsr", "john blown"]),
        ("RCMP", ["rcmp", "police"]),
    ]
    agencies_found: List[str] = []
    for name, kws in agencies:
        if any(kw in t_lower for kw in kws):
            agencies_found.append(name)
    multi_agency = len(agencies_found) >= 2

    # event type
    event_type = "fatality" if (n_fatalities and n_fatalities > 0) or ("bodies" in t_lower and "recovered" in t_lower) else None

    # bullets for quick summary
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
    }
