"""Initialise le schema MySQL (staging) et les index MongoDB (curated).

Usage :
    python scripts/init_mysql.py
"""
from src.storage import mongo_client, mysql_client
from src.utils.logging import get_logger

logger = get_logger("init_stores")


def main() -> None:
    mysql_client.init_schema()
    for table in sorted(mysql_client.ALLOWED_TABLES):
        logger.info("  %-24s : %d lignes", table, mysql_client.count_rows(table))

    mongo_client.ensure_indexes()
    logger.info("MongoDB '%s' : %d documents",
                "station_analytics", mongo_client.count_documents())


if __name__ == "__main__":
    main()
