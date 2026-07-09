"""Client MySQL pour la zone STAGING.

Responsabilites :
  - fournir une connexion (context manager),
  - creer le schema (tables + index) de maniere idempotente,
  - offrir des insertions batch (executemany) et des lectures parametrees.

Securite : toute lecture exposee via l'API passe par une WHITELIST de tables
et des requetes parametrees. On n'interpole JAMAIS d'entree utilisateur dans
une chaine SQL.
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterable, Sequence

import mysql.connector
from mysql.connector import Error as MySQLError

from src.config import settings
from src.utils.logging import get_logger

logger = get_logger(__name__)

# Tables consultables via l'API /staging. Rien d'autre n'est interrogeable.
ALLOWED_TABLES: set[str] = {
    "bike_history",
    "velib_station_status",
    "weather_snapshots",
    "ingestion_runs",
}

# ---------------------------------------------------------------------------
# DDL : creation du schema
# ---------------------------------------------------------------------------
_DDL_STATEMENTS: list[str] = [
    """
    CREATE TABLE IF NOT EXISTS bike_history (
        id            INT AUTO_INCREMENT PRIMARY KEY,
        datetime      DATETIME,
        season        TINYINT,
        year          SMALLINT,
        month         TINYINT,
        hour          TINYINT,
        holiday       TINYINT,
        weekday       TINYINT,
        workingday    TINYINT,
        weather_situation TINYINT,
        temp          FLOAT,
        atemp         FLOAT,
        humidity      FLOAT,
        windspeed     FLOAT,
        casual        INT,
        registered    INT,
        count         INT,
        INDEX idx_bike_datetime (datetime)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS velib_station_status (
        id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
        station_id          VARCHAR(64) NOT NULL,
        station_name        VARCHAR(255),
        timestamp           DATETIME NOT NULL,
        num_bikes_available INT,
        num_docks_available INT,
        num_ebikes_available INT,
        capacity            INT,
        lat                 DOUBLE,
        lon                 DOUBLE,
        is_installed        TINYINT,
        is_returning        TINYINT,
        is_renting          TINYINT,
        source              VARCHAR(64),
        -- empeche d'inserer deux fois le meme snapshot pour une station
        UNIQUE KEY uq_station_ts (station_id, timestamp),
        INDEX idx_velib_station (station_id),
        INDEX idx_velib_ts (timestamp)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS weather_snapshots (
        id                BIGINT AUTO_INCREMENT PRIMARY KEY,
        timestamp         DATETIME NOT NULL,
        temperature       FLOAT,
        relative_humidity FLOAT,
        wind_speed        FLOAT,
        precipitation     FLOAT,
        weather_code      INT,
        source            VARCHAR(64),
        UNIQUE KEY uq_weather_ts (timestamp),
        INDEX idx_weather_ts (timestamp)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS ingestion_runs (
        id             BIGINT AUTO_INCREMENT PRIMARY KEY,
        source         VARCHAR(64),
        zone           VARCHAR(32),
        status         VARCHAR(32),
        records_count  INT,
        started_at     DATETIME,
        finished_at    DATETIME,
        duration_ms    INT,
        error_message  TEXT,
        INDEX idx_run_source (source),
        INDEX idx_run_started (started_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
]


@contextmanager
def get_connection():
    """Context manager de connexion MySQL. Ferme toujours proprement."""
    conn = mysql.connector.connect(**settings.mysql_dsn)
    try:
        yield conn
    finally:
        conn.close()


def init_schema() -> None:
    """Cree toutes les tables et index (idempotent)."""
    with get_connection() as conn:
        cur = conn.cursor()
        for ddl in _DDL_STATEMENTS:
            cur.execute(ddl)
        conn.commit()
        cur.close()
    logger.info("Schema MySQL initialise (%d tables)", len(_DDL_STATEMENTS))


def insert_many(table: str, columns: Sequence[str],
                rows: Iterable[Sequence[Any]], ignore_duplicates: bool = True) -> int:
    """Insertion batch via executemany. Retourne le nombre de lignes traitees.

    `ignore_duplicates=True` utilise INSERT IGNORE : les collisions sur les
    contraintes UNIQUE (ex. meme station+timestamp) sont silencieusement
    ignorees, ce qui rend la reingestion idempotente.
    """
    if table not in ALLOWED_TABLES:
        raise ValueError(f"Table non autorisee : {table}")
    rows = list(rows)
    if not rows:
        return 0

    verb = "INSERT IGNORE" if ignore_duplicates else "INSERT"
    cols = ", ".join(f"`{c}`" for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"{verb} INTO `{table}` ({cols}) VALUES ({placeholders})"

    with get_connection() as conn:
        cur = conn.cursor()
        cur.executemany(sql, rows)
        conn.commit()
        affected = cur.rowcount
        cur.close()
    logger.info("insert_many %s : %d lignes soumises", table, len(rows))
    return affected


def fetch_rows(table: str, filters: dict[str, Any] | None = None,
               limit: int = 100) -> list[dict]:
    """Lecture parametree avec whitelist de table et de colonnes de filtre.

    `filters` : dict {colonne: valeur}. Les colonnes sont validees contre le
    schema reel de la table (INFORMATION_SCHEMA) pour bloquer toute injection
    via un nom de colonne. Les valeurs passent par des placeholders %s.
    """
    if table not in ALLOWED_TABLES:
        raise ValueError(f"Table non autorisee : {table}")
    limit = max(1, min(int(limit), 1000))
    filters = filters or {}

    with get_connection() as conn:
        # Colonnes reelles de la table -> whitelist dynamique
        cur = conn.cursor()
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s",
            (settings.mysql_database, table),
        )
        valid_columns = {r[0] for r in cur.fetchall()}
        cur.close()

        where_parts, params = [], []
        for col, val in filters.items():
            if col not in valid_columns:
                raise ValueError(f"Colonne de filtre non autorisee : {col}")
            where_parts.append(f"`{col}` = %s")
            params.append(val)

        where_sql = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
        sql = f"SELECT * FROM `{table}`{where_sql} LIMIT %s"
        params.append(limit)

        dict_cur = conn.cursor(dictionary=True)
        dict_cur.execute(sql, params)
        rows = dict_cur.fetchall()
        dict_cur.close()
    return rows


def count_rows(table: str) -> int:
    """Compte les lignes d'une table (pour /stats)."""
    if table not in ALLOWED_TABLES:
        raise ValueError(f"Table non autorisee : {table}")
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM `{table}`")
        (n,) = cur.fetchone()
        cur.close()
    return int(n)


def ping() -> bool:
    """Test de sante MySQL (pour /health)."""
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.close()
        return True
    except MySQLError as exc:
        logger.warning("MySQL injoignable : %s", exc)
        return False
