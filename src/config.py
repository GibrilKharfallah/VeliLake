"""Configuration centrale du projet.

Toutes les valeurs proviennent de variables d'environnement (fichier .env en
local, variables du docker-compose en conteneur). On n'ecrit JAMAIS un secret
ou une URL en dur ailleurs dans le code : tout passe par cet objet `settings`.

Usage :
    from src.config import settings
    settings.mysql_host, settings.s3_bucket_raw, ...
"""
from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Parametres applicatifs charges depuis l'environnement / .env."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # ignore les variables d'env non declarees ici
    )

    # ---- S3 / LocalStack (RAW) --------------------------------------------
    s3_endpoint_url: str = "http://localhost:4566"
    aws_access_key_id: str = "test"
    aws_secret_access_key: str = "test"
    aws_default_region: str = "eu-west-3"
    s3_bucket_raw: str = "raw"

    # ---- MySQL (STAGING) ---------------------------------------------------
    mysql_host: str = "localhost"
    mysql_port: int = 3306
    mysql_user: str = "root"
    mysql_password: str = "root"
    mysql_database: str = "staging"

    # ---- MongoDB (CURATED) -------------------------------------------------
    mongo_uri: str = "mongodb://localhost:27017"
    mongo_database: str = "curated"
    mongo_collection: str = "station_analytics"

    # ---- Sources de donnees ------------------------------------------------
    velib_status_url: str = (
        "https://velib-metropole-opendata.smovengo.cloud/"
        "opendata/Velib_Metropole/station_status.json"
    )
    velib_information_url: str = (
        "https://velib-metropole-opendata.smovengo.cloud/"
        "opendata/Velib_Metropole/station_information.json"
    )
    weather_url: str = "https://api.open-meteo.com/v1/forecast"
    weather_lat: float = 48.8566
    weather_lon: float = 2.3522
    http_timeout: int = 15
    http_max_retries: int = 3

    # ---- Divers ------------------------------------------------------------
    pipeline_version: str = "1.0.0"
    log_level: str = "INFO"

    @property
    def mysql_dsn(self) -> dict:
        """Kwargs prets a passer a mysql.connector.connect()."""
        return {
            "host": self.mysql_host,
            "port": self.mysql_port,
            "user": self.mysql_user,
            "password": self.mysql_password,
            "database": self.mysql_database,
        }


@lru_cache
def get_settings() -> Settings:
    """Instance unique mise en cache (evite de relire .env a chaque import)."""
    return Settings()


# Objet importable directement : `from src.config import settings`
settings = get_settings()
