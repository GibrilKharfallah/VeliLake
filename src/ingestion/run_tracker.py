"""Tracage des runs d'ingestion dans la table `ingestion_runs`.

Chaque tache d'ingestion s'execute dans le context manager `track_run`, qui
enregistre automatiquement : source, zone, statut (success/failed), nombre
d'enregistrements, horodatages debut/fin, duree ms et message d'erreur.

Usage :
    with track_run("velib_api", "raw") as state:
        ...
        state["records"] = nombre_traite
"""
from __future__ import annotations

from contextlib import contextmanager

from src.storage import mysql_client
from src.utils.logging import get_logger
from src.utils.time_utils import utcnow
from src.utils.timing import Timer

logger = get_logger(__name__)

_COLUMNS = [
    "source", "zone", "status", "records_count",
    "started_at", "finished_at", "duration_ms", "error_message",
]


@contextmanager
def track_run(source: str, zone: str):
    """Enregistre un run d'ingestion. `state["records"]` doit etre rempli."""
    started = utcnow()
    timer = Timer().start()
    state: dict = {"records": 0}
    status, error = "success", None
    try:
        yield state
    except Exception as exc:  # noqa: BLE001 -- on trace puis on relance
        status, error = "failed", str(exc)[:1000]
        logger.error("Run %s/%s en echec : %s", source, zone, exc)
        raise
    finally:
        finished = utcnow()
        duration_ms = int(timer.stop())
        try:
            mysql_client.insert_many(
                "ingestion_runs",
                _COLUMNS,
                [(source, zone, status, int(state.get("records", 0)),
                  started, finished, duration_ms, error)],
                ignore_duplicates=False,
            )
        except Exception as exc:  # noqa: BLE001
            # Ne jamais masquer l'erreur metier par une erreur de tracage.
            logger.error("Echec d'ecriture du run dans ingestion_runs : %s", exc)
        logger.info("Run %s/%s : %s, %d enregistrements, %d ms",
                    source, zone, status, state.get("records", 0), duration_ms)