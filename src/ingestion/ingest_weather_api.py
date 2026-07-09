"""Ingestion API METEO : Open-Meteo (meteo courante Paris) -> S3 raw.

Un point unique (coordonnees Paris depuis .env) a l'instant t. La meteo est une
variable GLOBALE ville : elle enrichira tous les snapshots de stations du meme
instant (choix assume, documente dans le README ; ce n'est pas une jointure
station par station).

Cle S3 produite :
    raw/api/weather/YYYY-MM-DD/weather_snapshot_HHMMSS.json
"""
from __future__ import annotations

from src.config import settings
from src.ingestion.run_tracker import track_run
from src.storage import s3_client
from src.utils.http import get_json
from src.utils.logging import get_logger
from src.utils.time_utils import utcnow

logger = get_logger(__name__)

# Variables meteo courantes demandees a Open-Meteo.
_CURRENT_VARS = "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation,weather_code"


def ingest_weather() -> int:
    """Recupere la meteo courante et l'ecrit dans S3 raw. Retourne 1."""
    with track_run("weather_api", "raw") as state:
        params = {
            "latitude": settings.weather_lat,
            "longitude": settings.weather_lon,
            "current": _CURRENT_VARS,
        }
        data = get_json(settings.weather_url, params=params)

        now = utcnow()
        day = now.strftime("%Y-%m-%d")
        hms = now.strftime("%H%M%S")
        s3_client.put_json(
            f"raw/api/weather/{day}/weather_snapshot_{hms}.json", data
        )
        state["records"] = 1
        logger.info("Meteo ingeree : %s", data.get("current", {}))
    return 1


if __name__ == "__main__":
    ingest_weather()