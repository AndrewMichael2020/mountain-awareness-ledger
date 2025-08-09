from __future__ import annotations

from typing import Optional, Tuple
import json
import trafilatura


def clean_html(html: str, url: Optional[str] = None) -> Tuple[Optional[str], dict]:
    # Ask for JSON output with metadata; fall back to plain text if unsupported
    try:
        downloaded = trafilatura.extract(
            html,
            include_comments=False,
            include_links=False,
            url=url,
            with_metadata=True,
            output_format="json",
        )
        if downloaded:
            data = json.loads(downloaded)
            text = data.get("text")
            meta = {
                "title": data.get("title"),
                "author": data.get("author"),
                "date": data.get("date"),
            }
            return text, meta
    except TypeError:
        # Older versions: retry without JSON formatting
        pass
    # Fallback: plain text only
    text = trafilatura.extract(html, include_comments=False, include_links=False, url=url)
    return text, {}
