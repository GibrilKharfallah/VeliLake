"""Endpoint /raw : exploration de la zone brute (S3 / LocalStack)."""
from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException, Query

from src.storage import s3_client

router = APIRouter()

# Raccourcis source -> prefixe S3
_SOURCE_PREFIX = {
    "file": "raw/file/",
    "velib": "raw/api/velib/",
    "weather": "raw/api/weather/",
}
_PREVIEW_MAX_CHARS = 2000


@router.get("/raw")
def list_raw(
    source: str | None = Query(None, description="file | velib | weather"),
    prefix: str | None = Query(None, description="prefixe S3 explicite (prioritaire)"),
    limit: int = Query(50, ge=1, le=1000),
    preview: bool = Query(False, description="renvoie un apercu du 1er objet"),
) -> dict:
    """Liste les objets de la zone raw, filtres par source ou prefixe."""
    if prefix:
        resolved = prefix
    elif source:
        if source not in _SOURCE_PREFIX:
            raise HTTPException(400, f"source invalide : {source} "
                                        f"(attendu: {list(_SOURCE_PREFIX)})")
        resolved = _SOURCE_PREFIX[source]
    else:
        resolved = "raw/"

    objects = s3_client.list_objects(resolved, limit=limit)
    result: dict = {"prefix": resolved, "count": len(objects), "objects": objects}

    if preview and objects:
        key = objects[0]["key"]
        try:
            raw = s3_client.get_object_bytes(key)
            text = raw.decode("utf-8", errors="replace")
            if key.endswith(".json"):
                parsed = json.loads(text)
                result["preview"] = {"key": key, "content": _truncate_json(parsed)}
            else:
                result["preview"] = {"key": key,
                                     "content": text[:_PREVIEW_MAX_CHARS]}
        except Exception as exc:  # noqa: BLE001
            result["preview"] = {"key": key, "error": str(exc)}

    return result


def _truncate_json(parsed):
    """Reduit un gros JSON pour l'apercu (ex. flux Velib de milliers de stations)."""
    if isinstance(parsed, dict) and "data" in parsed:
        data = parsed.get("data", {})
        if isinstance(data, dict) and "stations" in data:
            stations = data["stations"]
            preview = dict(parsed)
            preview["data"] = {
                "stations_count": len(stations),
                "stations_sample": stations[:3],
            }
            return preview
    return parsed