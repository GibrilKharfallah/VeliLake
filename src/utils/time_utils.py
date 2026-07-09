"""Petits utilitaires de date/heure.

MySQL DATETIME est naif (sans fuseau). On travaille donc en UTC naif partout
pour eviter les incoherences. `datetime.utcnow()` etant deprecie en 3.12, on
passe par `now(timezone.utc)` puis on retire le tzinfo.
"""
from __future__ import annotations

from datetime import datetime, timezone


def utcnow() -> datetime:
    """Datetime UTC naif (compatible colonnes MySQL DATETIME)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def utc_iso() -> str:
    """Horodatage ISO 8601 en UTC (pour metadonnees JSON / Mongo)."""
    return utcnow().isoformat() + "Z"

def from_epoch(epoch) -> datetime:
    """Convertit un timestamp epoch (secondes) en datetime UTC naif.

    Utilise pour le champ `lastUpdatedOther` / `last_reported` des flux GBFS.
    """
    return datetime.fromtimestamp(int(epoch), tz=timezone.utc).replace(tzinfo=None)