from __future__ import annotations

import hashlib
import re
from typing import Any, Dict
from urllib.parse import urlsplit, urlunsplit

import httpx


def canonicalize_url(url: str) -> str:
    parts = urlsplit(url)
    # strip tracking query params
    q = re.sub(r"(?:^|&)(utm_[^=]+|fbclid|gclid|mc_cid|mc_eid)=[^&]*", "", parts.query)
    q = re.sub(r"^&|&$", "", q)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, q, ""))


def get(url: str, ua: str, timeout_s: int = 20) -> Dict[str, Any]:
    """Fetch a URL and return raw HTML, sha256 of content, and final URL.

    Args:
        url: The URL to fetch.
        ua: User-Agent header value.
        timeout_s: Request timeout in seconds.

    Returns:
        dict with keys: raw_html (str), sha256 (str), final_url (str)
    """
    cu = canonicalize_url(url)
    headers = {"User-Agent": ua, "Accept": "text/html,application/xhtml+xml"}
    try:
        with httpx.Client(follow_redirects=True, timeout=timeout_s, headers=headers) as client:
            resp = client.get(cu)
            resp.raise_for_status()
            content_bytes = resp.content or b""
            sha256 = hashlib.sha256(content_bytes).hexdigest()
            raw_html = resp.text or ""
            return {"raw_html": raw_html, "sha256": sha256, "final_url": str(resp.url)}
    except httpx.HTTPError as e:
        raise RuntimeError(f"Fetch failed: {e}") from e
