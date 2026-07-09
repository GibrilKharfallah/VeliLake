"""Ingestion API VELIB : flux GBFS temps reel -> S3 raw.

On recupere deux flux GBFS et on les stocke bruts, horodates et partitionnes
par jour :
  - station_status.json      : disponibilites temps reel (velos, bornes)
  - station_information.json  : nom, position, capacite (change rarement)

On garde les deux car le status seul ne contient ni le nom ni la position ni la
capacite : c'est station_information qui les porte. La jointure status x info
par station_id se fera en raw_to_staging.

Cles S3 produites :
    raw/api/velib/YYYY-MM-DD/velib_status_HHMMSS.json
    raw/api/velib/YYYY-MM-DD/velib_information_HHMMSS.json
"""
from __future__ import annotations

from src.config import settings
from src.ingestion.run_tracker import track_run
from src.storage import s3_client
from src.utils.http import get_json
from src.utils.logging import get_logger
from src.utils.time_utils import utcnow

logger = get_logger(__name__)


def ingest_velib() -> int:
    """Recupere les flux GBFS Velib et les ecrit dans S3 raw.

    Retourne le nombre de stations vues dans station_status.
    """
    with track_run("velib_api", "raw") as state:
        status = get_json(settings.velib_status_url)
        information = get_json(settings.velib_information_url)

        now = utcnow()
        day = now.strftime("%Y-%m-%d")
        hms = now.strftime("%H%M%S")

        s3_client.put_json(f"raw/api/velib/{day}/velib_status_{hms}.json", status)
        s3_client.put_json(
            f"raw/api/velib/{day}/velib_information_{hms}.json", information
        )

        stations = status.get("data", {}).get("stations", [])
        state["records"] = len(stations)
        logger.info("Velib : %d stations ingerees", len(stations))
    return state["records"]


if __name__ == "__main__":
    ingest_velib()
