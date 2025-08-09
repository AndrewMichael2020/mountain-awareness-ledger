from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Any

# Optional imports: trafilatura preferred, fallback to bs4
try:
    import trafilatura  # type: ignore
except Exception:  # pragma: no cover
    trafilatura = None  # type: ignore

try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:  # pragma: no cover
    BeautifulSoup = None  # type: ignore


def clean_html(raw_html: str) -> Dict[str, Any]:
    """Clean HTML and extract text and metadata.

    Returns a dict with keys: text, title, author, published (datetime|None)
    """
    text = ""
    title = None
    author = None
    published = None

    if trafilatura is not None:
        extracted = trafilatura.extract(raw_html, include_comments=False, include_links=False, with_metadata=True)
        if extracted:
            # When with_metadata=True, extract returns a string; use metadata separately
            meta = trafilatura.metadata.extract_metadata(raw_html)
            text = extracted if isinstance(extracted, str) else (extracted["text"] if isinstance(extracted, dict) else "")
            if meta:
                title = getattr(meta, "title", None) or (meta.get("title") if isinstance(meta, dict) else None)
                author = getattr(meta, "author", None) or (meta.get("author") if isinstance(meta, dict) else None)
                date_str = getattr(meta, "date", None) or (meta.get("date") if isinstance(meta, dict) else None)
                if date_str:
                    try:
                        published = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                    except Exception:
                        published = None
    if not text and BeautifulSoup is not None:
        soup = BeautifulSoup(raw_html, "html.parser")
        title = title or (soup.title.string.strip() if soup.title and soup.title.string else None)
        # crude author from meta
        if not author:
            meta_author = soup.find("meta", attrs={"name": "author"})
            author = meta_author["content"].strip() if meta_author and meta_author.get("content") else None
        # published date common meta tags
        if not published:
            for sel in [
                ("meta", {"property": "article:published_time"}),
                ("meta", {"name": "date"}),
                ("time", {"itemprop": "datePublished"}),
            ]:
                tag = soup.find(*sel)
                if tag:
                    val = tag.get("content") or tag.get("datetime") or tag.text
                    try:
                        published = datetime.fromisoformat(val.strip().replace("Z", "+00:00"))
                        break
                    except Exception:
                        continue
        # visible text fallback
        text = text or soup.get_text(" ", strip=True)

    return {"text": text or "", "title": title, "author": author, "published": published}


def persist_artifacts(base_dir: Path, sha256: str, url: str, raw_html: str, clean_obj: Dict[str, Any]) -> Dict[str, Any]:
    """Persist raw and cleaned artifacts under a content-addressed folder.

    Returns a dict with metadata and paths.
    """
    folder = base_dir / sha256[:2] / sha256
    folder.mkdir(parents=True, exist_ok=True)

    (folder / "raw.html").write_text(raw_html, encoding="utf-8")
    (folder / "clean.txt").write_text(clean_obj.get("text", ""), encoding="utf-8")

    meta = {
        "url": url,
        "title": clean_obj.get("title"),
        "author": clean_obj.get("author"),
        "published": clean_obj.get("published"),
        "folder": str(folder),
        "saved_at": datetime.utcnow().isoformat() + "Z",
    }
    (folder / "meta.json").write_text(__import__("json").dumps(meta, default=str, indent=2), encoding="utf-8")
    return meta
