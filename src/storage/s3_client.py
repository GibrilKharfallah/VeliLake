"""Client S3 (LocalStack) pour la zone RAW.

Encapsule boto3. Le reste du code n'importe jamais boto3 directement : il passe
par ces fonctions. Cela centralise l'endpoint LocalStack et la gestion d'erreurs.

Convention de cles (voir README) :
    raw/file/bike_sharing/hour.csv
    raw/api/velib/YYYY-MM-DD/velib_snapshot_HHMMSS.json
    raw/api/weather/YYYY-MM-DD/weather_snapshot_HHMMSS.json
    raw/metadata/ingestion_runs.json
"""
from __future__ import annotations

import json
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from src.config import settings
from src.utils.logging import get_logger

logger = get_logger(__name__)


def get_s3_client():
    """Cree un client boto3 pointant vers LocalStack.

    Les credentials 'test'/'test' sont ceux attendus par LocalStack ; ils ne
    donnent acces a rien de reel, ce ne sont pas des secrets.
    """
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        region_name=settings.aws_default_region,
        config=Config(retries={"max_attempts": 3, "mode": "standard"}),
    )


def ensure_bucket(bucket: str | None = None) -> None:
    """Cree le bucket s'il n'existe pas (idempotent)."""
    bucket = bucket or settings.s3_bucket_raw
    s3 = get_s3_client()
    try:
        s3.head_bucket(Bucket=bucket)
        logger.debug("Bucket '%s' deja present", bucket)
    except ClientError:
        # eu-west-3 impose un LocationConstraint ; LocalStack le tolere.
        try:
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={
                    "LocationConstraint": settings.aws_default_region
                },
            )
        except ClientError:
            # Fallback pour region us-east-1 ou config non supportee
            s3.create_bucket(Bucket=bucket)
        logger.info("Bucket '%s' cree", bucket)


def put_bytes(key: str, data: bytes, content_type: str = "application/octet-stream",
                bucket: str | None = None) -> str:
    """Ecrit des octets bruts. Retourne la cle S3."""
    bucket = bucket or settings.s3_bucket_raw
    get_s3_client().put_object(Bucket=bucket, Key=key, Body=data,
                                ContentType=content_type)
    logger.info("PUT s3://%s/%s (%d octets)", bucket, key, len(data))
    return key


def put_json(key: str, obj: Any, bucket: str | None = None) -> str:
    """Serialise un objet Python en JSON et l'ecrit dans S3."""
    payload = json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8")
    return put_bytes(key, payload, content_type="application/json", bucket=bucket)


def put_text(key: str, text: str, content_type: str = "text/plain",
                bucket: str | None = None) -> str:
    return put_bytes(key, text.encode("utf-8"), content_type=content_type, bucket=bucket)


def get_object_bytes(key: str, bucket: str | None = None) -> bytes:
    """Lit un objet et retourne ses octets. Leve KeyError si absent."""
    bucket = bucket or settings.s3_bucket_raw
    try:
        resp = get_s3_client().get_object(Bucket=bucket, Key=key)
        return resp["Body"].read()
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
            raise KeyError(f"Objet introuvable : s3://{bucket}/{key}") from exc
        raise


def get_json(key: str, bucket: str | None = None) -> Any:
    return json.loads(get_object_bytes(key, bucket).decode("utf-8"))


def list_objects(prefix: str = "", limit: int = 100,
                    bucket: str | None = None) -> list[dict]:
    """Liste les objets sous un prefixe.

    Retourne une liste de dicts {key, size, last_modified}. Utilise la
    pagination boto3 pour ne pas s'arreter a 1000 objets, mais s'arrete des
    que `limit` est atteint.
    """
    bucket = bucket or settings.s3_bucket_raw
    paginator = get_s3_client().get_paginator("list_objects_v2")
    out: list[dict] = []
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                out.append({
                    "key": obj["Key"],
                    "size": obj["Size"],
                    "last_modified": obj["LastModified"].isoformat(),
                })
                if len(out) >= limit:
                    return out
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("NoSuchBucket", "404"):
            logger.warning("Bucket '%s' inexistant, liste vide", bucket)
            return []
        raise
    return out


def find_latest_key(prefix: str, bucket: str | None = None) -> str | None:
    """Retourne la cle de l'objet le plus recent sous un prefixe (ou None).

    Utilise par raw_to_staging pour ne transformer que le dernier snapshot
    ingere (Velib, meteo), plutot que de retraiter tout l'historique.
    """
    bucket = bucket or settings.s3_bucket_raw
    paginator = get_s3_client().get_paginator("list_objects_v2")
    latest = None
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if latest is None or obj["LastModified"] > latest["LastModified"]:
                latest = obj
    return latest["Key"] if latest else None


def count_objects(prefix: str = "", bucket: str | None = None) -> int:
    """Compte les objets sous un prefixe (pour /stats)."""
    bucket = bucket or settings.s3_bucket_raw
    paginator = get_s3_client().get_paginator("list_objects_v2")
    total = 0
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            total += page.get("KeyCount", 0)
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("NoSuchBucket", "404"):
            logger.warning("Bucket '%s' inexistant, count=0", bucket)
            return 0
        raise
    return total


def ping() -> bool:
    """Test de sante : LocalStack repond-il ? (pour /health)."""
    try:
        get_s3_client().list_buckets()
        return True
    except Exception as exc:  # noqa: BLE001
        logger.warning("S3 injoignable : %s", exc)
        return False
