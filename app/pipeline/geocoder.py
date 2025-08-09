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
}


def _geolocator() -> Nominatim:
    return Nominatim(user_agent="alpine-ledger/0.1", timeout=10)


def geocode_place(query: str, jurisdiction: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if not query:
        return None
    # Normalize known aliases
    q = ALIAS_NORMALIZE.get(query, query)
    if jurisdiction in ("BC", "AB", "WA") and jurisdiction not in q:
        # Help disambiguate with region keyword
        suffix = {"BC": "British Columbia", "AB": "Alberta", "WA": "Washington State"}[jurisdiction]
        q = f"{q}, {suffix}"
    geo = _geolocator()
    kwargs: Dict[str, Any] = {"addressdetails": True, "country_codes": "ca,us"}
    if jurisdiction and jurisdiction in BOUNDS:
        min_lon, min_lat, max_lon, max_lat = BOUNDS[jurisdiction]
        kwargs["viewbox"] = [(min_lon, min_lat), (max_lon, max_lat)]
        kwargs["bounded"] = True
    try:
        loc = geo.geocode(q, **kwargs)
    except Exception:
        return None
    if not loc:
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
                # Set tz from jurisdiction if known
                hit["tz_local"] = TZ_BY_JURIS.get(juris)
                return hit
    return None
