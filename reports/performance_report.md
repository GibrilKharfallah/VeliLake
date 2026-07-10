# Rapport de performance — /ingest vs /ingest_fast

Genere le 2026-07-10T17:24:53.882812+00:00

## Machine
- Plateforme : `Linux-6.18.33.2-microsoft-standard-WSL2-x86_64-with-glibc2.35`
- Python : 3.12.4
- Processeur : x86_64

## Methodologie
Batchs synthetiques generes localement. Chaque configuration : 1 warm-up non compte + 5 mesures. Metrique = duree serveur (duration_ms) = temps reel du pipeline raw->staging->curated, hors latence reseau client. Moyenne arithmetique.

## Resultats

| Batch | /ingest (ms) | /ingest_fast (ms) | Speedup | Amelioration |
|------:|-------------:|------------------:|--------:|-------------:|
| 1 | 420.67 | 239.06 | x1.76 | +43.2% |
| 100 | 4837.01 | 791.72 | x6.11 | +83.6% |

## Explication technique des optimisations

`/ingest_fast` reduit les aller-retours reseau/disque par rapport a `/ingest` :

- **S3 (raw)** : 1 objet pour tout le batch au lieu d'un objet par record.
- **MySQL (staging)** : un seul `executemany` + un `commit`, au lieu d'un `INSERT` + `commit` par record.
- **MongoDB (curated)** : un `bulk_write` au lieu d'un `replace_one` par record.
- **Features** : calcul en lot + score d'anomalie IsolationForest sur l'ensemble du batch.

Le gain croit avec la taille du batch : sur 1 record, les deux modes sont quasi equivalents (rien a grouper) ; sur 100, la reduction des aller-retours domine.