"""Client MongoDB pour la zone CURATED.

Stocke les documents enrichis (collection `station_analytics`). Encapsule
pymongo pour centraliser l'URI et proposer les operations utilisees par le
pipeline (insert_many) et l'API (/curated, /stats).
"""
from __future__ import annotations

from functools import lru_cache
from typing import Any

from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection

from src.config import settings
from src.utils.logging import get_logger

logger = get_logger(__name__)


@lru_cache
def _client() -> MongoClient:
    # serverSelectionTimeoutMS court : un /health ne doit pas bloquer 30s.
    return MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=3000)


def get_collection() -> Collection:
    return _client()[settings.mongo_database][settings.mongo_collection]


def ensure_indexes() -> None:
    """Cree les index utiles a l'API (idempotent)."""
    col = get_collection()
    col.create_index([("station_id", ASCENDING)])
    col.create_index([("timestamp", ASCENDING)])
    col.create_index([("analytics.tension_level", ASCENDING)])
    col.create_index([("analytics.is_critical", ASCENDING)])
    logger.info("Index MongoDB assures sur %s", settings.mongo_collection)


def insert_documents(documents: list[dict]) -> int:
    """Insertion batch via insert_many. Retourne le nombre insere."""
    if not documents:
        return 0
    result = get_collection().insert_many(documents)
    n = len(result.inserted_ids)
    logger.info("insert_many curated : %d documents", n)
    return n


def find_documents(filters: dict[str, Any] | None = None,
                   limit: int = 100) -> list[dict]:
    """Lecture filtree. Convertit ObjectId en str pour la serialisation JSON."""
    limit = max(1, min(int(limit), 1000))
    cursor = get_collection().find(filters or {}).limit(limit)
    out = []
    for doc in cursor:
        doc["_id"] = str(doc["_id"])
        out.append(doc)
    return out


def count_documents(filters: dict[str, Any] | None = None) -> int:
    return get_collection().count_documents(filters or {})


def aggregate(pipeline: list[dict]) -> list[dict]:
    """Expose l'aggregation pipeline pour /stats (top stations, moyennes...)."""
    return list(get_collection().aggregate(pipeline))


def ping() -> bool:
    """Test de sante MongoDB (pour /health)."""
    try:
        _client().admin.command("ping")
        return True
    except Exception as exc:  # noqa: BLE001
        logger.warning("MongoDB injoignable : %s", exc)
        return False
