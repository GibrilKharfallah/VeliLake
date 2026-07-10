"""Tests des features metier et du scoring d'anomalie (fonctions pures)."""
from src.transformation import features as F
from src.transformation.anomaly_detection import compute_anomaly_scores


def test_occupancy_rate():
    assert F.occupancy_rate(12, 25) == 0.48
    assert F.occupancy_rate(5, 0) is None       # capacite 0 -> pas de division
    assert F.occupancy_rate(5, None) is None     # capacite absente


def test_ebike_ratio():
    assert F.ebike_ratio(5, 12) == round(5 / 12, 4)
    assert F.ebike_ratio(3, 0) == 0.0            # aucun velo -> 0, pas d'erreur


def test_dock_ratio():
    assert F.dock_ratio(8, 25) == 0.32
    assert F.dock_ratio(1, 0) is None


def test_is_critical():
    assert F.is_critical(0, 5, 1, 1, 1) is True   # loue mais 0 velo
    assert F.is_critical(5, 0, 1, 1, 1) is True   # recoit mais 0 borne
    assert F.is_critical(5, 5, 1, 1, 1) is False  # station saine
    assert F.is_critical(0, 0, 1, 1, 0) is False  # non installee -> jamais critique


def test_tension_level():
    assert F.tension_level(0.05, False) == F.TENSION_EMPTY
    assert F.tension_level(0.20, False) == F.TENSION_LOW
    assert F.tension_level(0.50, False) == F.TENSION_NORMAL
    assert F.tension_level(0.90, False) == F.TENSION_FULL
    assert F.tension_level(0.50, True) == F.TENSION_CRITICAL   # critical prioritaire
    assert F.tension_level(None, False) == F.TENSION_NORMAL


def test_anomaly_scores_range_and_outlier():
    rows = [[0.5, 0.4, 0.5] for _ in range(50)] + [[0.0, 0.0, 1.0]]
    scores = compute_anomaly_scores(rows)
    assert len(scores) == 51
    assert all(0.0 <= s <= 1.0 for s in scores)
    assert scores[-1] == max(scores)             # l'outlier a le score max


def test_anomaly_small_batch_fallback():
    scores = compute_anomaly_scores([[0.5, 0.4, 0.5], [0.9, 0.1, 0.1]])
    assert len(scores) == 2
    assert all(0.0 <= s <= 1.0 for s in scores)


def test_anomaly_empty():
    assert compute_anomaly_scores([]) == []