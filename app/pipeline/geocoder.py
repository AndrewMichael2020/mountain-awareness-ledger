from __future__ import annotations

from typing import Optional, Dict, Any, Tuple

from geopy.geocoders import Nominatim


# Bounding boxes (min_lon, min_lat, max_lon, max_lat)
BOUNDS = {
    "BC": (-139.06, 48.30, -114.05, 60.00),
    "AB": (-120.00, 48.99, -110.00, 60.00),
    "WA": (-125.00, 45.50, -116.50, 49.05),
}

TZ_BY_JURIS = {
    "BC": "America/Vancouver",
    "WA": "America/Vancouver",
    "AB": "America/Edmonton",
}

ALIAS_NORMALIZE = {
    "Lougheed Provincial Park": "Peter Lougheed Provincial Park",
    "PLPP": "Peter Lougheed Provincial Park",
    "Peter Lougheed Park": "Peter Lougheed Provincial Park",
}

# Known centroids for key places when geocoder misses
ALIAS_COORDS: Dict[str, Dict[str, Any]] = {
    # Approximate centroid within Kananaskis Country, Alberta
    "Peter Lougheed Provincial Park": {
        "lat": 50.72,
        "lon": -115.35,
        "iso_country": "CA",
        "admin_area": "Alberta",
    }
}


def _geolocator() -> Nominatim:
    return Nominatim(user_agent="alpine-ledger/0.1", timeout=10)


def _normalize_query(query: str) -> str:
    if not query:
        return query
    q = query.strip()
    low = q.lower()
    if "lougheed" in low and "provincial park" in low:
        return "Peter Lougheed Provincial Park"
    if low == "plpp":
        return "Peter Lougheed Provincial Park"
    return ALIAS_NORMALIZE.get(q, q)


def geocode_place(query: str, jurisdiction: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if not query:
        return None
    q = _normalize_query(query)
    if jurisdiction in ("BC", "AB", "WA") and jurisdiction not in q:
        suffix = {"BC": "British Columbia", "AB": "Alberta", "WA": "Washington State"}[jurisdiction]
        q_full = f"{q}, {suffix}"
    else:
        q_full = q

    geo = _geolocator()
    kwargs: Dict[str, Any] = {"addressdetails": True, "country_codes": "ca,us"}
    if jurisdiction and jurisdiction in BOUNDS:
        min_lon, min_lat, max_lon, max_lat = BOUNDS[jurisdiction]
        kwargs["viewbox"] = [(min_lon, min_lat), (max_lon, max_lat)]
        kwargs["bounded"] = True

    # Try bounded first
    try:
        loc = geo.geocode(q_full, **kwargs)
    except Exception:
        loc = None

    # Alberta-specific helper: add Kananaskis if no hit
    if not loc and jurisdiction == "AB" and "Kananaskis" not in q_full:
        try:
            loc = geo.geocode(f"{q_full}, Kananaskis", **kwargs)
        except Exception:
            loc = None

    # Retry without bounds
    if not loc:
        try:
            loc = geo.geocode(q_full, addressdetails=True, country_codes="ca,us")
        except Exception:
            loc = None

    if not loc:
        # Fallback to known centroids
        coords = ALIAS_COORDS.get(_normalize_query(q))
        if coords:
            return {
                "lat": coords["lat"],
                "lon": coords["lon"],
                "display_name": q,
                "iso_country": coords.get("iso_country"),
                "admin_area": coords.get("admin_area"),
            }
        return None

    addr = getattr(loc, "raw", {}).get("address", {})
    iso = addr.get("country_code", "").upper() or None
    admin = addr.get("state") or addr.get("region") or addr.get("province")
    return {
        "lat": loc.latitude,
        "lon": loc.longitude,
        "display_name": getattr(loc, "address", None) or getattr(loc, "raw", {}).get("display_name"),
        "iso_country": iso,
        "admin_area": admin,
    }


def geocode_from_extracted(fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    juris = fields.get("jurisdiction")
    # Prefer peak_name then location_name
    for key in ("peak_name", "location_name"):
        q = fields.get(key)
        if q:
            hit = geocode_place(q, jurisdiction=juris)
            if hit:
                hit["tz_local"] = TZ_BY_JURIS.get(juris)
                return hit
    return None
