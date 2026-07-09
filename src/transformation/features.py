"""Features metier calculees par station a partir d'une ligne de disponibilite.

Fonctions pures (aucun I/O) : faciles a tester unitairement et reutilisables
par le pipeline batch (staging_to_curated) comme par l'endpoint /ingest.

Regles de robustesse :
    - capacite nulle/0/absente -> ratios a None (indeterminable, pas 0 trompeur).
    - 0 velo dispo -> ebike_ratio = 0.0 (pas de division par zero).
"""
from __future__ import annotations

# Niveaux de tension (exposes pour filtrage API et tests)
TENSION_EMPTY = "empty_or_almost_empty"
TENSION_LOW = "low"
TENSION_NORMAL = "normal"
TENSION_FULL = "full_or_almost_full"
TENSION_CRITICAL = "critical"


def occupancy_rate(num_bikes: int, capacity) -> float | None:
    """Taux d'occupation = velos disponibles / capacite. None si capacite inconnue."""
    if not capacity or capacity <= 0:
        return None
    return round(num_bikes / capacity, 4)


def ebike_ratio(num_ebikes: int, num_bikes: int) -> float:
    """Part de velos electriques parmi les velos disponibles. 0.0 si aucun velo."""
    if not num_bikes or num_bikes <= 0:
        return 0.0
    return round(num_ebikes / num_bikes, 4)


def dock_ratio(num_docks: int, capacity) -> float | None:
    """Taux de bornes libres = bornes libres / capacite. None si capacite inconnue."""
    if not capacity or capacity <= 0:
        return None
    return round(num_docks / capacity, 4)


def is_critical(num_bikes: int, num_docks: int, is_renting, is_returning,
                is_installed) -> bool:
    """Station operationnellement bloquee.

    Regle metier documentee : une station installee est critique si elle est
    cense louer mais n'a aucun velo, OU cense recevoir mais n'a aucune borne
    libre. Dans les deux cas l'usager est bloque.
    """
    if not is_installed:
        return False
    no_bike_to_rent = bool(is_renting) and num_bikes == 0
    no_dock_to_return = bool(is_returning) and num_docks == 0
    return bool(no_bike_to_rent or no_dock_to_return)


def tension_level(occ_rate: float | None, critical: bool) -> str:
    """Niveau de tension a partir du taux d'occupation. `critical` a priorite."""
    if critical:
        return TENSION_CRITICAL
    if occ_rate is None:
        return TENSION_NORMAL  # capacite inconnue -> pas de tension calculable
    if occ_rate <= 0.10:
        return TENSION_EMPTY
    if occ_rate <= 0.30:
        return TENSION_LOW
    if occ_rate >= 0.85:
        return TENSION_FULL
    return TENSION_NORMAL