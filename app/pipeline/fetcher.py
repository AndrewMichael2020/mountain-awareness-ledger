from __future__ import annotations

import httpx
import urllib.robotparser as robotparser
from urllib.parse import urlparse
from typing import Optional, Tuple

DEFAULT_UA = "AlpineLedgerBot/0.1 (+https://github.com/your-org/alpine-ledger)"


def is_allowed(url: str, user_agent: str = DEFAULT_UA) -> bool:
    parsed = urlparse(url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    rp = robotparser.RobotFileParser()
    try:
        rp.set_url(robots_url)
        rp.read()
    except Exception:
        return True  # fail-open
    return rp.can_fetch(user_agent, url)


def fetch_url(url: str, timeout: float = 20.0, user_agent: str = DEFAULT_UA) -> Tuple[str, Optional[str]]:
    headers = {"User-Agent": user_agent, "Accept": "text/html,application/xhtml+xml"}
    if not is_allowed(url, user_agent):
        raise PermissionError("Blocked by robots.txt")
    with httpx.Client(follow_redirects=True, timeout=timeout, headers=headers) as client:
        resp = client.get(url)
        resp.raise_for_status()
        return resp.text, str(resp.url)
