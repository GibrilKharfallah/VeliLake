"""Transformation RAW (S3) -> STAGING (MySQL).

Trois chemins independants :
  1. Velib   : dernier couple status+information -> table velib_station_status
  2. Meteo   : dernier snapshot Open-Meteo       -> table weather_snapshots
  3. Fichier : hour.csv UCI                        -> table bike_history

Points cles :
  - Le champ imbrique `num_bikes_available_types` (liste [{mechanical: n},
    {ebike: n}]) est aplati pour extraire le nombre de velos electriques.
  - status (dispos) et information (nom, position, capacite) sont joints par
    station_id.
  - Le flux Velib melange snake_case et camelCase : on lit les deux.
  - Idempotence : velib/meteo via INSERT IGNORE sur les contraintes UNIQUE ;
    bike_history (dataset statique) charge une seule fois (skip si deja rempli).
"""
from __future__ import annotations

import io

import pandas as pd

from src.ingestion.run_tracker import track_run
from src.storage import mysql_client, s3_client
from src.utils.logging import get_logger
from src.utils.time_utils import from_epoch, utcnow

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Helpers de lecture defensive des champs GBFS
# ---------------------------------------------------------------------------
def _first(station: dict, *keys, default=None):
    """Retourne la premiere cle presente et non nulle (snake_case OU camelCase)."""
    for k in keys:
        val = station.get(k)
        if val is not None:
            return val
    return default


def _count_ebikes(types_list) -> int:
    """Somme les velos electriques dans num_bikes_available_types.

    Structure attendue : [{"mechanical": 3}, {"ebike": 2}]. Robuste aux
    entrees inattendues (liste vide, cles manquantes, valeurs nulles).
    """
    total = 0
    for entry in types_list or []:
        if isinstance(entry, dict):
            total += int(entry.get("ebike", 0) or 0)
    return total


# ---------------------------------------------------------------------------
# 1. Velib
# ---------------------------------------------------------------------------
_VELIB_COLUMNS = [
    "station_id", "station_name", "timestamp", "num_bikes_available",
    "num_docks_available", "num_ebikes_available", "capacity", "lat", "lon",
    "is_installed", "is_returning", "is_renting", "source",
]


def velib_raw_to_staging() -> int:
    """Transforme le dernier snapshot Velib en lignes velib_station_status."""
    with track_run("velib_api", "staging") as state:
        status_key = s3_client.find_latest_key("raw/api/velib/")
        # on ne garde que les cles de STATUS pour trouver le plus recent
        # (find_latest_key peut renvoyer un fichier information : on filtre)
        if status_key and "velib_status_" not in status_key:
            latest = None
            for obj in s3_client.list_objects("raw/api/velib/", limit=10000):
                if "velib_status_" in obj["key"]:
                    if latest is None or obj["last_modified"] > latest["last_modified"]:
                        latest = obj
            status_key = latest["key"] if latest else None

        if not status_key:
            raise FileNotFoundError("Aucun snapshot Velib dans raw/api/velib/")

        info_key = status_key.replace("velib_status_", "velib_information_")
        status = s3_client.get_json(status_key)
        try:
            information = s3_client.get_json(info_key)
        except KeyError:
            logger.warning("Information manquante (%s), noms/positions vides", info_key)
            information = {"data": {"stations": []}}

        # index information par station_id (str) pour la jointure
        info_by_id: dict[str, dict] = {}
        for st in information.get("data", {}).get("stations", []):
            sid = str(_first(st, "station_id", "stationCode", default=""))
            info_by_id[sid] = st

        # timestamp du snapshot (commun a toutes les stations de ce feed)
        epoch = _first(status, "lastUpdatedOther", "last_updated")
        ts = from_epoch(epoch) if epoch else utcnow()

        rows = []
        for st in status.get("data", {}).get("stations", []):
            sid = str(_first(st, "station_id", "stationCode", default=""))
            info = info_by_id.get(sid, {})
            rows.append((
                sid,
                _first(info, "name", default=None),
                ts,
                int(_first(st, "num_bikes_available", "numBikesAvailable", default=0)),
                int(_first(st, "num_docks_available", "numDocksAvailable", default=0)),
                _count_ebikes(st.get("num_bikes_available_types")),
                _first(info, "capacity", default=None),
                _first(info, "lat", default=None),
                _first(info, "lon", default=None),
                int(_first(st, "is_installed", default=0)),
                int(_first(st, "is_returning", default=0)),
                int(_first(st, "is_renting", default=0)),
                "velib_api",
            ))

        inserted = mysql_client.insert_many(
            "velib_station_status", _VELIB_COLUMNS, rows, ignore_duplicates=True
        )
        state["records"] = len(rows)
        logger.info("Velib staging : %d lignes soumises (nouvelles: %d)",
                    len(rows), inserted)
    return state["records"]


# ---------------------------------------------------------------------------
# 2. Meteo
# ---------------------------------------------------------------------------
_WEATHER_COLUMNS = [
    "timestamp", "temperature", "relative_humidity", "wind_speed",
    "precipitation", "weather_code", "source",
]


def weather_raw_to_staging() -> int:
    """Transforme le dernier snapshot meteo en ligne weather_snapshots."""
    with track_run("weather_api", "staging") as state:
        key = s3_client.find_latest_key("raw/api/weather/")
        if not key:
            raise FileNotFoundError("Aucun snapshot meteo dans raw/api/weather/")

        data = s3_client.get_json(key)
        current = data.get("current", {})

        # Open-Meteo renvoie current.time en 'YYYY-MM-DDTHH:MM'
        raw_time = current.get("time")
        try:
            ts = pd.to_datetime(raw_time).to_pydatetime() if raw_time else utcnow()
        except Exception:  # noqa: BLE001
            ts = utcnow()

        row = (
            ts,
            current.get("temperature_2m"),
            current.get("relative_humidity_2m"),
            current.get("wind_speed_10m"),
            current.get("precipitation"),
            current.get("weather_code"),
            "open_meteo",
        )
        mysql_client.insert_many(
            "weather_snapshots", _WEATHER_COLUMNS, [row], ignore_duplicates=True
        )
        state["records"] = 1
        logger.info("Meteo staging : 1 ligne (%s)", ts)
    return 1


# ---------------------------------------------------------------------------
# 3. Fichier UCI Bike Sharing
# ---------------------------------------------------------------------------
_BIKE_COLUMNS = [
    "datetime", "season", "year", "month", "hour", "holiday", "weekday",
    "workingday", "weather_situation", "temp", "atemp", "humidity",
    "windspeed", "casual", "registered", "count",
]


def bike_file_raw_to_staging(force: bool = False) -> int:
    """Charge hour.csv depuis S3 raw dans bike_history.

    Dataset statique -> charge une seule fois. `force=True` recharge malgre
    des lignes existantes (les doublons ne sont pas dedupliques : a reserver
    a un usage manuel apres TRUNCATE).
    """
    with track_run("bike_sharing_file", "staging") as state:
        if not force and mysql_client.count_rows("bike_history") > 0:
            logger.info("bike_history deja peuple, chargement ignore (force=False)")
            state["records"] = 0
            return 0

        raw = s3_client.get_object_bytes("raw/file/bike_sharing/hour.csv")
        df = pd.read_csv(io.BytesIO(raw))

        # datetime = dteday + heure (hr)
        dt = pd.to_datetime(df["dteday"]) + pd.to_timedelta(df["hr"], unit="h")

        rows = list(zip(
            dt.dt.strftime("%Y-%m-%d %H:%M:%S"),
            df["season"].astype(int), df["yr"].astype(int),
            df["mnth"].astype(int), df["hr"].astype(int),
            df["holiday"].astype(int), df["weekday"].astype(int),
            df["workingday"].astype(int), df["weathersit"].astype(int),
            df["temp"].astype(float), df["atemp"].astype(float),
            df["hum"].astype(float), df["windspeed"].astype(float),
            df["casual"].astype(int), df["registered"].astype(int),
            df["cnt"].astype(int),
        ))

        mysql_client.insert_many(
            "bike_history", _BIKE_COLUMNS, rows, ignore_duplicates=False
        )
        state["records"] = len(rows)
        logger.info("bike_history : %d lignes chargees", len(rows))
    return state["records"]


# ---------------------------------------------------------------------------
# Orchestration locale
# ---------------------------------------------------------------------------
def run() -> None:
    """Execute les trois transformations (isolees : un echec n'arrete pas les autres)."""
    for label, fn in [("Velib", velib_raw_to_staging),
                        ("Meteo", weather_raw_to_staging),
                        ("Fichier UCI", bike_file_raw_to_staging)]:
        try:
            n = fn()
            logger.info("[OK] %s -> staging : %s lignes", label, n)
        except Exception as exc:  # noqa: BLE001
            logger.error("[KO] %s -> staging : %s", label, exc)


if __name__ == "__main__":
    run()