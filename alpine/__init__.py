"""
Lightweight package scaffold for ingestion pipeline components.
"""
from . import fetch, clean, extract_det  # noqa: F401
from .models import IngestRequest  # noqa: F401

__all__ = ["fetch", "clean", "extract_det", "IngestRequest"]
