"""API Gateway VeliLake (FastAPI).

Expose les zones du data lake via une interface REST simple :
    GET /health   etat des services
    GET /raw      objets de la zone brute (S3)
    GET /staging  lignes de la zone staging (MySQL)
    GET /curated  documents enrichis (MongoDB)
    GET /stats    metriques de remplissage + indicateurs Velib

Les endpoints avances /ingest et /ingest_fast sont ajoutes separement
(routes_ingest) dans le bloc "niveau avance".

Lancement :
    uvicorn src.api.main:app --reload --port 8000
Documentation interactive : http://localhost:8000/docs
"""
from __future__ import annotations

from fastapi import FastAPI

from src.api import (
    routes_curated,
    routes_health,
    routes_raw,
    routes_staging,
    routes_stats,
)
from src.config import settings

app = FastAPI(
    title="VeliLake API",
    version=settings.pipeline_version,
    description="Data lake multi-zones pour la mobilite velo partagee (Velib).",
)

app.include_router(routes_health.router, tags=["health"])
app.include_router(routes_raw.router, tags=["raw"])
app.include_router(routes_staging.router, tags=["staging"])
app.include_router(routes_curated.router, tags=["curated"])
app.include_router(routes_stats.router, tags=["stats"])


@app.get("/", tags=["root"])
def root() -> dict:
    return {
        "name": "VeliLake API",
        "version": settings.pipeline_version,
        "docs": "/docs",
        "endpoints": ["/health", "/raw", "/staging", "/curated", "/stats"],
    }