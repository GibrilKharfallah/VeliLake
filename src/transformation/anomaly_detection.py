"""Score d'anomalie sur un batch de stations.

Approche : IsolationForest (non supervise) sur les features numeriques de
l'ensemble des stations d'un meme snapshot. Une station est "anormale" si sa
combinaison (taux d'occupation, ratio elec, taux de bornes) s'ecarte du reste
du reseau a cet instant.

Robustesse :
  - batch trop petit (< 20) -> repli sur un z-score du taux d'occupation ;
  - valeurs manquantes (capacite inconnue) -> imputation par moyenne de colonne ;
  - scores normalises en [0, 1] (1 = plus anormal) pour un stockage lisible.
"""
from __future__ import annotations

import numpy as np

from src.utils.logging import get_logger

logger = get_logger(__name__)

_MIN_BATCH_FOR_IFOREST = 20


def _normalize(scores: np.ndarray) -> list[float]:
    lo, hi = float(np.min(scores)), float(np.max(scores))
    if hi - lo < 1e-9:
        return [0.0] * len(scores)
    return [round((float(s) - lo) / (hi - lo), 4) for s in scores]


def _zscore_fallback(occupancy: list) -> list[float]:
    arr = np.array([np.nan if v is None else float(v) for v in occupancy], dtype=float)
    mask = ~np.isnan(arr)
    if mask.sum() < 2:
        return [0.0] * len(arr)
    mean = arr[mask].mean()
    std = arr[mask].std() or 1.0
    z = np.abs((arr - mean) / std)
    z = np.nan_to_num(z, nan=0.0)
    return _normalize(z)


def compute_anomaly_scores(feature_rows: list[list]) -> list[float]:
    """Scores d'anomalie [0,1] pour un batch.

    feature_rows : liste de [occupancy_rate, ebike_ratio, dock_ratio] (None
    autorise). Retourne un score par ligne, dans le meme ordre.
    """
    n = len(feature_rows)
    if n == 0:
        return []

    X = np.array(
        [[np.nan if v is None else float(v) for v in row] for row in feature_rows],
        dtype=float,
    )

    # Imputation des NaN par la moyenne de colonne (colonne toute-NaN -> 0).
    col_means = np.nanmean(X, axis=0)
    col_means = np.where(np.isnan(col_means), 0.0, col_means)
    nan_idx = np.where(np.isnan(X))
    X[nan_idx] = np.take(col_means, nan_idx[1])

    if n < _MIN_BATCH_FOR_IFOREST:
        return _zscore_fallback([row[0] for row in feature_rows])

    try:
        from sklearn.ensemble import IsolationForest

        model = IsolationForest(
            n_estimators=100, contamination="auto", random_state=42
        )
        model.fit(X)
        raw = -model.score_samples(X)  # plus haut = plus anormal
        return _normalize(raw)
    except Exception as exc:  # noqa: BLE001
        logger.warning("IsolationForest indisponible (%s), repli z-score", exc)
        return _zscore_fallback([row[0] for row in feature_rows])