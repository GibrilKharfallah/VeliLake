"""Outils de chronometrage.

Utilises par les endpoints /ingest et /ingest_fast (mesure du temps total de
traitement) et par le script de benchmark. On mesure en `time.perf_counter`
(horloge monotone haute resolution), comme dans le TP2.
"""
from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass, field


@dataclass
class Timer:
    """Chronometre a usage unique.

    Exemple :
        t = Timer().start()
        ... traitement ...
        duration_ms = t.stop()
    """

    _start: float = field(default=0.0)
    _elapsed_ms: float = field(default=0.0)

    def start(self) -> "Timer":
        self._start = time.perf_counter()
        return self

    def stop(self) -> float:
        self._elapsed_ms = (time.perf_counter() - self._start) * 1000.0
        return self._elapsed_ms

    @property
    def elapsed_ms(self) -> float:
        return self._elapsed_ms


@contextmanager
def measure():
    """Context manager renvoyant un dict dont la cle 'ms' est remplie a la sortie.

    Exemple :
        with measure() as m:
            do_work()
        print(m["ms"])
    """
    result = {"ms": 0.0}
    start = time.perf_counter()
    try:
        yield result
    finally:
        result["ms"] = (time.perf_counter() - start) * 1000.0
