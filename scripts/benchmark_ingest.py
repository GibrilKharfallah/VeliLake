"""Benchmark reel /ingest vs /ingest_fast.

Genere des batchs synthetiques (1 et 100 stations), appelle chaque endpoint
plusieurs fois, moyenne les durees mesurees COTE SERVEUR (champ duration_ms de
la reponse = temps reel du pipeline), calcule le speedup, et ecrit :
  - reports/performance_results.json
  - reports/performance_report.md

Prerequis : l'API doit tourner (uvicorn src.api.main:app --port 8000) et les
services Docker doivent etre up.

Usage :
    python scripts/benchmark_ingest.py
    python scripts/benchmark_ingest.py --runs 10 --url http://localhost:8000
"""
from __future__ import annotations

import argparse
import json
import platform
import random
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

REPORTS_DIR = Path("reports")
BATCH_SIZES = [1, 100]


def make_station(i: int) -> dict:
    """Une station synthetique coherente (docks = capacite - velos)."""
    capacity = random.randint(10, 40)
    bikes = random.randint(0, capacity)
    ebikes = random.randint(0, bikes)
    return {
        "station_id": f"bench_{i}",
        "station_name": f"Benchmark Station {i}",
        "timestamp": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "num_bikes_available": bikes,
        "num_docks_available": capacity - bikes,
        "num_ebikes_available": ebikes,
        "capacity": capacity,
        "lat": 48.85 + random.uniform(-0.05, 0.05),
        "lon": 2.35 + random.uniform(-0.05, 0.05),
        "is_installed": 1,
        "is_renting": 1,
        "is_returning": 1,
    }


def make_payload(size: int) -> dict:
    return {"source": "benchmark", "data": [make_station(i) for i in range(size)]}


def call(url: str, endpoint: str, payload: dict) -> float:
    """Appelle un endpoint, retourne la duree serveur (ms). Leve si erreur."""
    resp = requests.post(f"{url}{endpoint}", json=payload, timeout=120)
    resp.raise_for_status()
    return float(resp.json()["duration_ms"])


def bench_endpoint(url: str, endpoint: str, size: int, runs: int) -> list[float]:
    """Warm-up (non compte) + `runs` mesures de la duree serveur."""
    payload = make_payload(size)
    call(url, endpoint, payload)  # warm-up : connexions, compilation, caches
    durations = []
    for _ in range(runs):
        durations.append(call(url, endpoint, make_payload(size)))
    return durations


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark /ingest vs /ingest_fast")
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--runs", type=int, default=5,
                        help="mesures par configuration (min 3)")
    args = parser.parse_args()
    runs = max(3, args.runs)

    # verifie que l'API repond
    try:
        health = requests.get(f"{args.url}/health", timeout=10).json()
        print(f"API OK, services: {health.get('services')}")
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"API injoignable sur {args.url} : {exc}")

    results: dict = {}
    for size in BATCH_SIZES:
        std = bench_endpoint(args.url, "/ingest", size, runs)
        fast = bench_endpoint(args.url, "/ingest_fast", size, runs)
        mean_std, mean_fast = statistics.mean(std), statistics.mean(fast)
        speedup = mean_std / mean_fast if mean_fast else float("inf")
        improvement = (mean_std - mean_fast) / mean_std * 100 if mean_std else 0.0
        results[f"batch_{size}"] = {
            "batch_size": size,
            "ingest": {"mean_ms": round(mean_std, 2),
                        "runs_ms": [round(x, 2) for x in std]},
            "ingest_fast": {"mean_ms": round(mean_fast, 2),
                            "runs_ms": [round(x, 2) for x in fast]},
            "speedup_x": round(speedup, 2),
            "improvement_pct": round(improvement, 1),
        }
        print(f"batch {size:>3} | ingest {mean_std:8.1f} ms | "
                f"ingest_fast {mean_fast:8.1f} ms | "
                f"speedup x{speedup:.2f} ({improvement:+.1f}%)")

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "machine": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "processor": platform.processor() or "unknown",
        },
        "methodology": (
            "Batchs synthetiques generes localement. Chaque configuration : "
            f"1 warm-up non compte + {runs} mesures. Metrique = duree serveur "
            "(duration_ms) = temps reel du pipeline raw->staging->curated, "
            "hors latence reseau client. Moyenne arithmetique."
        ),
        "runs_per_config": runs,
        "results": results,
    }

    REPORTS_DIR.mkdir(exist_ok=True)
    (REPORTS_DIR / "performance_results.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False)
    )
    _write_markdown(report)
    print(f"\nRapports ecrits dans {REPORTS_DIR}/")


def _write_markdown(report: dict) -> None:
    lines = [
        "# Rapport de performance — /ingest vs /ingest_fast",
        "",
        f"Genere le {report['generated_at']}",
        "",
        "## Machine",
        f"- Plateforme : `{report['machine']['platform']}`",
        f"- Python : {report['machine']['python']}",
        f"- Processeur : {report['machine']['processor']}",
        "",
        "## Methodologie",
        report["methodology"],
        "",
        "## Resultats",
        "",
        "| Batch | /ingest (ms) | /ingest_fast (ms) | Speedup | Amelioration |",
        "|------:|-------------:|------------------:|--------:|-------------:|",
    ]
    for key in sorted(report["results"], key=lambda k: report["results"][k]["batch_size"]):
        r = report["results"][key]
        lines.append(
            f"| {r['batch_size']} | {r['ingest']['mean_ms']} | "
            f"{r['ingest_fast']['mean_ms']} | x{r['speedup_x']} | "
            f"{r['improvement_pct']:+}% |"
        )
    lines += [
        "",
        "## Explication technique des optimisations",
        "",
        "`/ingest_fast` reduit les aller-retours reseau/disque par rapport a "
        "`/ingest` :",
        "",
        "- **S3 (raw)** : 1 objet pour tout le batch au lieu d'un objet par record.",
        "- **MySQL (staging)** : un seul `executemany` + un `commit`, au lieu d'un "
        "`INSERT` + `commit` par record.",
        "- **MongoDB (curated)** : un `bulk_write` au lieu d'un `replace_one` par record.",
        "- **Features** : calcul en lot + score d'anomalie IsolationForest sur "
        "l'ensemble du batch.",
        "",
        "Le gain croit avec la taille du batch : sur 1 record, les deux modes sont "
        "quasi equivalents (rien a grouper) ; sur 100, la reduction des aller-retours "
        "domine.",
    ]
    (REPORTS_DIR / "performance_report.md").write_text("\n".join(lines))


if __name__ == "__main__":
    main()