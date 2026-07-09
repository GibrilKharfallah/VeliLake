"""Ingestion FICHIER : dataset UCI Bike Sharing (hour.csv / day.csv) -> S3 raw.

Les CSV locaux (poses dans data/raw_files/) sont televerses tels quels dans la
zone raw. Aucune transformation ici : le raw doit rester une copie fidele de la
source. Le nettoyage/typage se fera en raw_to_staging.

Cles S3 produites :
    raw/file/bike_sharing/hour.csv
    raw/file/bike_sharing/day.csv
"""
from __future__ import annotations

from pathlib import Path

from src.ingestion.run_tracker import track_run
from src.storage import s3_client
from src.utils.logging import get_logger

logger = get_logger(__name__)

LOCAL_DIR = Path("data/raw_files")
DEFAULT_FILES = ["hour.csv", "day.csv"]


def ingest_file(filenames: list[str] | None = None) -> int:
    """Televerse les CSV UCI presents localement vers S3 raw.

    Retourne le nombre de fichiers effectivement televerses.
    `records_count` = nombre de fichiers (le detail par lignes se mesure en
    staging, quand les donnees sont reellement parsees).
    """
    filenames = filenames or DEFAULT_FILES
    with track_run("bike_sharing_file", "raw") as state:
        uploaded = 0
        for name in filenames:
            path = LOCAL_DIR / name
            if not path.exists():
                logger.warning("Fichier absent, ignore : %s "
                               "(telecharge le dataset UCI, voir README)", path)
                continue
            key = f"raw/file/bike_sharing/{name}"
            s3_client.put_bytes(key, path.read_bytes(), content_type="text/csv")
            uploaded += 1
        state["records"] = uploaded
        if uploaded == 0:
            raise FileNotFoundError(
                f"Aucun CSV trouve dans {LOCAL_DIR}. Place hour.csv / day.csv."
            )
    return uploaded


if __name__ == "__main__":
    ingest_file()