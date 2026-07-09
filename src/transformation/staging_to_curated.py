"""Transformation STAGING (MySQL) -> CURATED (MongoDB).

Lit le dernier snapshot Velib en staging, calcule les features metier, y greffe
le contexte meteo global de la ville, score les anomalies sur l'ensemble du
batch, et upsert les documents enrichis dans MongoDB (collection
station_analytics).

Choix assume : la meteo est un contexte GLOBAL (un point Paris), greffe a
toutes les stations du meme snapshot. Ce n'est pas une jointure par station.
"""
from __future__ import annotations

from src.config import settings
from src.ingestion.run_tracker import track_run
from src.storage import mongo_client, mysql_client
from src.transformation import features as F
from src.transformation.anomaly_detection import compute_anomaly_scores
from src.utils.logging import get_logger
from src.utils.time_utils import utcnow

logger = get_logger(__name__)


def _fetch_latest_velib() -> list[dict]:
    """Lignes du dernier snapshot (timestamp max) de velib_station_status.

    Requete interne au pipeline, sans entree utilisateur : on utilise
    directement la connexion (pas la whitelist de l'API).
    """
    with mysql_client.get_connection() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT station_id, station_name, timestamp, num_bikes_available,
                   num_docks_available, num_ebikes_available, capacity, lat, lon,
                   is_installed, is_returning, is_renting
            FROM velib_station_status
            WHERE timestamp = (SELECT MAX(timestamp) FROM velib_station_status)
            """
        )
        rows = cur.fetchall()
        cur.close()
    return rows


def _fetch_latest_weather() -> dict:
    """Derniere ligne de weather_snapshots (ou {} si aucune)."""
    with mysql_client.get_connection() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT temperature, relative_humidity, wind_speed "
            "FROM weather_snapshots ORDER BY timestamp DESC LIMIT 1"
        )
        row = cur.fetchone()
        cur.close()
    return row or {}


def _build_document(station: dict, occ, ebk, dck, critical: bool,
                    anomaly: float, weather: dict, processed_at) -> dict:
    return {
        "station_id": station["station_id"],
        "station_name": station["station_name"],
        "timestamp": station["timestamp"],  # datetime natif (BSON date)
        "availability": {
            "num_bikes_available": station["num_bikes_available"],
            "num_ebikes_available": station["num_ebikes_available"],
            "num_docks_available": station["num_docks_available"],
            "capacity": station["capacity"],
            "occupancy_rate": occ,
            "ebike_ratio": ebk,
            "dock_ratio": dck,
        },
        "location": {"lat": station["lat"], "lon": station["lon"]},
        "weather": {
            "temperature": weather.get("temperature"),
            "humidity": weather.get("relative_humidity"),
            "wind_speed": weather.get("wind_speed"),
        },
        "analytics": {
            "tension_level": F.tension_level(occ, critical),
            "anomaly_score": anomaly,
            "is_critical": critical,
        },
        "metadata": {
            "source": "velib_api",
            "processed_at": processed_at,
            "pipeline_version": settings.pipeline_version,
        },
    }


def staging_to_curated() -> int:
    """Construit et upsert les documents enrichis. Retourne le nombre traite."""
    with track_run("velib_curated", "curated") as state:
        stations = _fetch_latest_velib()
        if not stations:
            raise ValueError("Aucune donnee Velib en staging (lance raw_to_staging)")
        weather = _fetch_latest_weather()

        # 1. features + criticite par station
        computed = []
        feature_rows = []
        for s in stations:
            occ = F.occupancy_rate(s["num_bikes_available"], s["capacity"])
            ebk = F.ebike_ratio(s["num_ebikes_available"], s["num_bikes_available"])
            dck = F.dock_ratio(s["num_docks_available"], s["capacity"])
            crit = F.is_critical(
                s["num_bikes_available"], s["num_docks_available"],
                s["is_renting"], s["is_returning"], s["is_installed"],
            )
            computed.append((s, occ, ebk, dck, crit))
            feature_rows.append([occ, ebk, dck])

        # 2. anomalies sur tout le batch
        scores = compute_anomaly_scores(feature_rows)

        # 3. documents
        processed_at = utcnow()
        documents = [
            _build_document(s, occ, ebk, dck, crit, score, weather, processed_at)
            for (s, occ, ebk, dck, crit), score in zip(computed, scores)
        ]

        mongo_client.upsert_documents(documents, key_fields=["station_id", "timestamp"])
        state["records"] = len(documents)
        logger.info("Curated : %d documents (meteo greffee: %s)",
                    len(documents), bool(weather))
    return state["records"]


if __name__ == "__main__":
    staging_to_curated()