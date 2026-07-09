"""Endpoint /staging : lecture de la zone staging (MySQL).

Securite : `table` doit appartenir a la whitelist du client MySQL, les filtres
sont valides contre le schema reel et injectes en requete parametree. Aucune
requete SQL brute n'est acceptee de l'utilisateur.
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from src.storage import mysql_client

router = APIRouter()


@router.get("/staging")
def read_staging(
    table: str = Query("velib_station_status",
                       description="table autorisee de la zone staging"),
    station_id: str | None = Query(None),
    source: str | None = Query(None),
    limit: int = Query(50, ge=1, le=1000),
) -> dict:
    """Retourne des lignes d'une table staging, filtrables par station/source."""
    if table not in mysql_client.ALLOWED_TABLES:
        raise HTTPException(
            400, f"table non autorisee : {table} "
                    f"(autorisees: {sorted(mysql_client.ALLOWED_TABLES)})"
        )

    filters: dict = {}
    if station_id is not None:
        filters["station_id"] = station_id
    if source is not None:
        filters["source"] = source

    try:
        rows = mysql_client.fetch_rows(table, filters, limit)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc

    return {"table": table, "count": len(rows), "rows": rows}