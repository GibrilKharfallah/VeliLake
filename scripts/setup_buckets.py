"""Cree le(s) bucket(s) S3 necessaires dans LocalStack.

Usage :
    python scripts/setup_buckets.py
"""
from src.storage import s3_client
from src.utils.logging import get_logger

logger = get_logger("setup_buckets")


def main() -> None:
    s3_client.ensure_bucket()  # bucket raw (defini dans settings)
    s3_client.ensure_bucket("dvc-store")   # remote DVC (versioning du dataset UCI) 
    buckets = s3_client.get_s3_client().list_buckets().get("Buckets", [])
    logger.info("Buckets presents : %s", [b["Name"] for b in buckets])


if __name__ == "__main__":
    main()
