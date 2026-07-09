"""Endpoint /health : etat de l'API et des trois services sous-jacents."""
from __future__ import annotations

from fastapi import APIRouter

from src.storage import mongo_client, mysql_client, s3_client
from src.utils.time_utils import utc_iso

router = APIRouter()


@router.get("/health")
def health() -> dict:
    """Verifie l'API et la connectivite S3 / MySQL / MongoDB.

    Retourne toujours 200 (l'endpoint lui-meme repond) ; le champ `status`
    passe a 'degraded' si au moins un service ne repond pas, avec le detail
    par service.
    """
    services = {
        "s3": s3_client.ping(),
        "mysql": mysql_client.ping(),
        "mongodb": mongo_client.ping(),
    }
    all_up = all(services.values())
    return {
        "status": "ok" if all_up else "degraded",
        "timestamp": utc_iso(),
        "services": {name: ("up" if ok else "down") for name, ok in services.items()},
    }