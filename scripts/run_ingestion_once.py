"""Lance les trois ingestions vers la zone raw, en une passe.

Usage :
    python scripts/run_ingestion_once.py
    python scripts/run_ingestion_once.py --skip-file   # sans le CSV UCI

Chaque source est independante : si l'une echoue (ex. CSV absent), les autres
s'executent quand meme, et l'echec est trace dans ingestion_runs.
"""
from __future__ import annotations

import argparse

from src.ingestion.ingest_file import ingest_file
from src.ingestion.ingest_velib_api import ingest_velib
from src.ingestion.ingest_weather_api import ingest_weather
from src.utils.logging import get_logger

logger = get_logger("run_ingestion")


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingestion raw (fichier + APIs)")
    parser.add_argument("--skip-file", action="store_true",
                        help="ne pas ingerer le CSV UCI local")
    parser.add_argument("--skip-velib", action="store_true")
    parser.add_argument("--skip-weather", action="store_true")
    args = parser.parse_args()

    tasks = []
    if not args.skip_file:
        tasks.append(("fichier UCI", ingest_file))
    if not args.skip_velib:
        tasks.append(("Velib", ingest_velib))
    if not args.skip_weather:
        tasks.append(("Meteo", ingest_weather))

    for label, fn in tasks:
        try:
            n = fn()
            logger.info("[OK] %s : %s enregistrements", label, n)
        except Exception as exc:  # noqa: BLE001 -- on isole chaque source
            logger.error("[KO] %s : %s", label, exc)


if __name__ == "__main__":
    main()