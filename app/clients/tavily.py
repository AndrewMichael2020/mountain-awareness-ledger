from __future__ import annotations

import os
import httpx
from typing import List, Dict

TAVILY_API_URL = "https://api.tavily.com/search"


def search(query: str, max_results: int = 10) -> List[Dict]:
    api_key = os.getenv("TAVILY_API_KEY")
    if not api_key:
        raise RuntimeError("TAVILY_API_KEY not set")
    payload = {"api_key": api_key, "query": query, "max_results": max_results}
    with httpx.Client(timeout=20) as client:
        r = client.post(TAVILY_API_URL, json=payload)
        r.raise_for_status()
        data = r.json()
        return data.get("results", [])
