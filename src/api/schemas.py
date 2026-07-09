"""Schemas Pydantic de l'API.

Regroupe les modeles de requete/reponse. Les modeles d'ingestion (StationIn,
IngestRequest) sont utilises par les endpoints /ingest et /ingest_fast :
Pydantic valide et type chaque station recue avant tout traitement.
"""
from __future__ import annotations

from pydantic import BaseModel, Field


class StationIn(BaseModel):
    """Une station telle qu'acceptee par /ingest et /ingest_fast."""

    station_id: str
    station_name: str | None = None
    timestamp: str | None = None  # ISO 8601 ; defaut = maintenant si absent
    num_bikes_available: int = 0
    num_docks_available: int = 0
    num_ebikes_available: int = 0
    capacity: int | None = None
    lat: float | None = None
    lon: float | None = None
    is_installed: int = 1
    is_renting: int = 1
    is_returning: int = 1


class IngestRequest(BaseModel):
    """Payload d'ingestion : une source + un batch de stations."""

    source: str = "manual"
    data: list[StationIn] = Field(default_factory=list)


class IngestResponse(BaseModel):
    """Reponse chronometree des endpoints d'ingestion."""

    status: str
    records_processed: int
    duration_ms: float
    mode: str