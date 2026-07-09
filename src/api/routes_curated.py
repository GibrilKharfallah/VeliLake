"""Endpoint /curated : lecture des documents enrichis (MongoDB)."""
from __future__ import annotations

from fastapi import APIRouter, Query

from src.storage import mongo_client

router = APIRouter()


@router.get("/curated")
def read_curated(
    station_id: str | None = Query(None),
    tension_level: str | None = Query(
        None, description="empty_or_almost_empty | low | normal | "
                            "full_or_almost_full | critical"),
    is_critical: bool | None = Query(None),
    limit: int = Query(50, ge=1, le=1000),
) -> dict:
    """Retourne des documents curated, filtrables par station / tension / criticite."""
    filters: dict = {}
    if station_id is not None:
        filters["station_id"] = station_id
    if tension_level is not None:
        filters["analytics.tension_level"] = tension_level
    if is_critical is not None:
        filters["analytics.is_critical"] = is_critical

    docs = mongo_client.find_documents(filters, limit)
    return {"count": len(docs), "documents": docs}