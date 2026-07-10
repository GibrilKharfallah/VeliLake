"""Endpoints avances /ingest (standard) et /ingest_fast (optimise).

Les deux acceptent un batch JSON de stations (valide par Pydantic) et le
propagent dans les trois zones. La reponse indique le nombre de records
traites, le mode, et la duree mesuree cote serveur (base du benchmark).
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from src.api import ingest_service
from src.api.schemas import IngestRequest, IngestResponse

router = APIRouter()


@router.post("/ingest", response_model=IngestResponse)
def ingest(payload: IngestRequest) -> IngestResponse:
    """Ingestion record par record (baseline non optimisee)."""
    if not payload.data:
        raise HTTPException(400, "le champ 'data' est vide")
    n, ms = ingest_service.ingest_standard(payload)
    return IngestResponse(status="ok", records_processed=n,
                            duration_ms=round(ms, 2), mode="standard")


@router.post("/ingest_fast", response_model=IngestResponse)
def ingest_fast(payload: IngestRequest) -> IngestResponse:
    """Ingestion par operations groupees (batch, vectorisee)."""
    if not payload.data:
        raise HTTPException(400, "le champ 'data' est vide")
    n, ms = ingest_service.ingest_fast(payload)
    return IngestResponse(status="ok", records_processed=n,
                            duration_ms=round(ms, 2), mode="fast")