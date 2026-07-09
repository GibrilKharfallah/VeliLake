"""Client HTTP resilient pour les appels aux APIs externes (Velib, Open-Meteo).

Centralise le timeout et la strategie de retry (backoff exponentiel via
tenacity). Toute l'ingestion API passe par `get_json`, jamais par requests
directement : ainsi le comportement reseau est uniforme et configurable via
.env (HTTP_TIMEOUT, HTTP_MAX_RETRIES).
"""
from __future__ import annotations

from typing import Any

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.config import settings
from src.utils.logging import get_logger

logger = get_logger(__name__)


@retry(
    stop=stop_after_attempt(settings.http_max_retries),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type(requests.RequestException),
    reraise=True,  # apres N echecs, on relance la vraie exception
)
def get_json(url: str, params: dict | None = None,
             timeout: int | None = None) -> Any:
    """GET + parse JSON, avec retry automatique sur erreur reseau/HTTP.

    Leve requests.HTTPError sur un statut >= 400 (via raise_for_status), ce qui
    declenche le retry. Apres HTTP_MAX_RETRIES tentatives, l'exception remonte.
    """
    timeout = timeout or settings.http_timeout
    logger.debug("GET %s params=%s", url, params)
    resp = requests.get(url, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.json()