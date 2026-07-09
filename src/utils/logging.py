"""Configuration de logging unifiee pour tout le projet.

On expose une seule fonction `get_logger(name)`. Elle garantit un format
coherent (horodatage + niveau + module) et evite les handlers dupliques
quand un module est importe plusieurs fois.
"""
from __future__ import annotations

import logging
import sys

from src.config import settings

_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
_configured = False


def _configure_root() -> None:
    global _configured
    if _configured:
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(_FORMAT, datefmt="%Y-%m-%d %H:%M:%S"))
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(settings.log_level.upper())
    _configured = True


def get_logger(name: str) -> logging.Logger:
    """Retourne un logger nomme, configure une seule fois au niveau racine."""
    _configure_root()
    return logging.getLogger(name)
