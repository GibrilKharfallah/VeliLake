"""Endpoint /stats : metriques de remplissage et indicateurs metier.

Agrege les trois zones : comptage d'objets S3, lignes MySQL par table,
documents MongoDB, derniere ingestion, et statistiques Velib (occupation
moyenne, top stations vides/pleines, nombre de critiques).
"""
from __future__ import annotations

from fastapi import APIRouter

from src.storage import mongo_client, mysql_client, s3_client

router = APIRouter()


def _last_ingestion() -> dict | None:
    with mysql_client.get_connection() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT source, zone, status, records_count, finished_at "
            "FROM ingestion_runs ORDER BY id DESC LIMIT 1"
        )
        row = cur.fetchone()
        cur.close()
    return row


def _top_stations(order: int) -> list[dict]:
    """Top 5 stations par taux d'occupation (order=1 vides, order=-1 pleines)."""
    return mongo_client.aggregate([
        {"$match": {"availability.occupancy_rate": {"$ne": None}}},
        {"$sort": {"availability.occupancy_rate": order}},
        {"$limit": 5},
        {"$project": {"_id": 0, "station_id": 1, "station_name": 1,
                        "occupancy_rate": "$availability.occupancy_rate",
                        "num_bikes_available": "$availability.num_bikes_available"}},
    ])


@router.get("/stats")
def stats() -> dict:
    """Vue d'ensemble du remplissage du lake + indicateurs Velib."""
    raw_objects = s3_client.count_objects("raw/")
    mysql_counts = {t: mysql_client.count_rows(t)
                    for t in sorted(mysql_client.ALLOWED_TABLES)}
    curated_docs = mongo_client.count_documents()

    agg = mongo_client.aggregate([
        {"$group": {
            "_id": None,
            "avg_occupancy": {"$avg": "$availability.occupancy_rate"},
            "stations": {"$addToSet": "$station_id"},
        }},
    ])
    if agg:
        avg_occ = agg[0]["avg_occupancy"]
        avg_occ = round(avg_occ, 4) if avg_occ is not None else None
        station_count = len(agg[0]["stations"])
    else:
        avg_occ, station_count = None, 0

    critical_count = mongo_client.count_documents({"analytics.is_critical": True})

    return {
        "zones": {
            "raw_objects": raw_objects,
            "staging_rows": mysql_counts,
            "curated_documents": curated_docs,
        },
        "last_ingestion": _last_ingestion(),
        "velib": {
            "station_count": station_count,
            "avg_occupancy_rate": avg_occ,
            "critical_stations": critical_count,
            "top5_emptiest": _top_stations(1),
            "top5_fullest": _top_stations(-1),
        },
    }