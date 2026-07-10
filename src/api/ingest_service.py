"""Service d'ingestion temps reel pour /ingest et /ingest_fast.

Les deux fonctions poussent un batch de stations a travers les trois zones
(raw -> staging -> curated). Elles font le MEME travail logique ; seule la
strategie d'I/O change :

  ingest_standard (mode "standard") :
    - 1 objet S3 par record
    - INSERT + commit MySQL par record
    - replace_one MongoDB par record
    - features calculees record par record

  ingest_fast (mode "fast") :
    - 1 seul objet S3 pour tout le batch
    - executemany MySQL + 1 commit
    - bulk_write MongoDB (insert_many idempotent)
    - features en lot + score d'anomalie IsolationForest sur le batch

Le gain vient donc de la reduction des aller-retours reseau/disque (lecon TP2),
pas d'un bridage de la version standard.
"""
from __future__ import annotations

import pandas as pd

from src.api.schemas import IngestRequest, StationIn
from src.config import settings
from src.storage import mongo_client, mysql_client, s3_client
from src.transformation import features as F
from src.transformation.anomaly_detection import compute_anomaly_scores
from src.utils.logging import get_logger
from src.utils.time_utils import utcnow
from src.utils.timing import Timer

logger = get_logger(__name__)

_MYSQL_COLUMNS = [
    "station_id", "station_name", "timestamp", "num_bikes_available",
    "num_docks_available", "num_ebikes_available", "capacity", "lat", "lon",
    "is_installed", "is_returning", "is_renting", "source",
]
_INSERT_SQL = (
    "INSERT IGNORE INTO velib_station_status ("
    + ", ".join(f"`{c}`" for c in _MYSQL_COLUMNS)
    + ") VALUES (" + ", ".join(["%s"] * len(_MYSQL_COLUMNS)) + ")"
)


def _parse_ts(raw: str | None):
    """Parse un timestamp ISO en datetime naif ; defaut = maintenant."""
    if not raw:
        return utcnow()
    try:
        return pd.to_datetime(raw).to_pydatetime().replace(tzinfo=None)
    except Exception:  # noqa: BLE001
        return utcnow()


def _fetch_latest_weather() -> dict:
    with mysql_client.get_connection() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT temperature, relative_humidity, wind_speed "
            "FROM weather_snapshots ORDER BY timestamp DESC LIMIT 1"
        )
        row = cur.fetchone()
        cur.close()
    return row or {}


def _row(station: StationIn, ts, source: str) -> tuple:
    return (
        station.station_id, station.station_name, ts,
        station.num_bikes_available, station.num_docks_available,
        station.num_ebikes_available, station.capacity,
        station.lat, station.lon,
        station.is_installed, station.is_returning, station.is_renting, source,
    )


def _document(station: StationIn, ts, source: str, weather: dict,
              anomaly, processed_at) -> dict:
    occ = F.occupancy_rate(station.num_bikes_available, station.capacity)
    ebk = F.ebike_ratio(station.num_ebikes_available, station.num_bikes_available)
    dck = F.dock_ratio(station.num_docks_available, station.capacity)
    crit = F.is_critical(
        station.num_bikes_available, station.num_docks_available,
        station.is_renting, station.is_returning, station.is_installed,
    )
    return {
        "station_id": station.station_id,
        "station_name": station.station_name,
        "timestamp": ts,
        "availability": {
            "num_bikes_available": station.num_bikes_available,
            "num_ebikes_available": station.num_ebikes_available,
            "num_docks_available": station.num_docks_available,
            "capacity": station.capacity,
            "occupancy_rate": occ,
            "ebike_ratio": ebk,
            "dock_ratio": dck,
        },
        "location": {"lat": station.lat, "lon": station.lon},
        "weather": {
            "temperature": weather.get("temperature"),
            "humidity": weather.get("relative_humidity"),
            "wind_speed": weather.get("wind_speed"),
        },
        "analytics": {
            "tension_level": F.tension_level(occ, crit),
            "anomaly_score": anomaly,
            "is_critical": crit,
        },
        "metadata": {
            "source": source,
            "processed_at": processed_at,
            "pipeline_version": settings.pipeline_version,
        },
    }


# ---------------------------------------------------------------------------
# Version STANDARD : record par record
# ---------------------------------------------------------------------------
def ingest_standard(payload: IngestRequest) -> tuple[int, float]:
    """Traite le batch record par record. Retourne (nb_records, duree_ms)."""
    timer = Timer().start()
    source = payload.source
    processed_at = utcnow()
    weather = _fetch_latest_weather()
    day = processed_at.strftime("%Y-%m-%d")
    hms = processed_at.strftime("%H%M%S%f")

    collection = mongo_client.get_collection()
    with mysql_client.get_connection() as conn:
        cur = conn.cursor()
        for i, station in enumerate(payload.data):
            ts = _parse_ts(station.timestamp)
            # raw : un objet par record
            s3_client.put_json(
                f"raw/api/ingest/{day}/ingest_{hms}_{i}.json", station.model_dump()
            )
            # staging : execute + commit par record
            cur.execute(_INSERT_SQL, _row(station, ts, source))
            conn.commit()
            # curated : upsert par record (anomaly indisponible en unitaire)
            doc = _document(station, ts, source, weather, None, processed_at)
            collection.replace_one(
                {"station_id": doc["station_id"], "timestamp": doc["timestamp"]},
                doc, upsert=True,
            )
        cur.close()

    duration = timer.stop()
    logger.info("ingest standard : %d records en %.1f ms", len(payload.data), duration)
    return len(payload.data), duration


# ---------------------------------------------------------------------------
# Version FAST : batch
# ---------------------------------------------------------------------------
def ingest_fast(payload: IngestRequest) -> tuple[int, float]:
    """Traite le batch en operations groupees. Retourne (nb_records, duree_ms)."""
    timer = Timer().start()
    source = payload.source
    processed_at = utcnow()
    weather = _fetch_latest_weather()
    day = processed_at.strftime("%Y-%m-%d")
    hms = processed_at.strftime("%H%M%S%f")

    stations = payload.data
    parsed = [(_parse_ts(s.timestamp), s) for s in stations]

    # raw : un seul objet pour tout le batch
    s3_client.put_json(
        f"raw/api/ingest/{day}/ingest_batch_{hms}.json",
        {"source": source, "data": [s.model_dump() for s in stations]},
    )

    # staging : executemany + 1 commit
    rows = [_row(s, ts, source) for ts, s in parsed]
    mysql_client.insert_many(
        "velib_station_status", _MYSQL_COLUMNS, rows, ignore_duplicates=True
    )

    # curated : features en lot + anomalie sur le batch + bulk upsert
    feature_rows = [
        [F.occupancy_rate(s.num_bikes_available, s.capacity),
         F.ebike_ratio(s.num_ebikes_available, s.num_bikes_available),
         F.dock_ratio(s.num_docks_available, s.capacity)]
        for _, s in parsed
    ]
    scores = compute_anomaly_scores(feature_rows)
    docs = [
        _document(s, ts, source, weather, score, processed_at)
        for (ts, s), score in zip(parsed, scores)
    ]
    mongo_client.upsert_documents(docs, key_fields=["station_id", "timestamp"])

    duration = timer.stop()
    logger.info("ingest fast : %d records en %.1f ms", len(stations), duration)
    return len(stations), duration