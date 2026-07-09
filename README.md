# VeliLake

**Data lake multi-zones pour l'analyse de disponibilité des vélos partagés (Vélib).**

Projet final — EFREI *Data Lakes & Data Integration* (2025-2026).

VeliLake ingère des données de mobilité urbaine autour du vélo partagé, les fait
transiter par trois zones (brute → intermédiaire → enrichie), et les expose via
une API REST. Trois sources sont combinées :

- **Vélib Métropole (API GBFS)** — disponibilité temps réel des ~1 450 stations parisiennes ;
- **UCI Bike Sharing Dataset (fichier CSV)** — historique horaire de location (Washington D.C., 2011-2012) ;
- **Open-Meteo (API)** — météo courante de Paris, pour enrichir les snapshots.

> Nom de package : `smart-mobility-data-lake`. Nom d'affichage : **VeliLake**.

---

## Architecture

| Zone | Rôle | Techno |
|------|------|--------|
| **Raw** | copie fidèle des données sources, sans transformation | **S3** (LocalStack) |
| **Staging** | données nettoyées, typées, requêtables en SQL | **MySQL** |
| **Curated** | documents enrichis (features métier, météo, anomalies) | **MongoDB** |
| **Orchestration** | planification des pipelines | **Airflow** *(à venir)* |
| **API Gateway** | exposition REST des trois zones | **FastAPI** |

Flux : `sources → raw (S3) → staging (MySQL) → curated (MongoDB) → API`.

La contrainte du sujet (zone raw en S3 ou Elasticsearch) est respectée avec S3/LocalStack.

---

## Prérequis

- **Docker** + **Docker Compose** (services de données)
- **[uv](https://docs.astral.sh/uv/)** (gestion d'environnement et de dépendances Python)
- **Python ≥ 3.10** (le projet est développé/testé en 3.12)

---

## Structure du projet (état actuel)

```bash
VeliLake/
├── docker-compose.yml          # LocalStack + MySQL + MongoDB (+ Airflow/API à venir)
├── pyproject.toml              # dépendances + packaging (uv)
├── uv.lock                     # versions verrouillées (reproductibilité)
├── .env.example                # gabarit de configuration (à copier en .env)
├── .gitignore
├── src/
│   ├── config.py               # config centrale (pydantic-settings) — source unique de vérité
│   ├── storage/
│   │   ├── s3_client.py         # zone raw : lecture/écriture/liste d'objets S3
│   │   ├── mysql_client.py      # zone staging : schéma, insert batch, lectures sécurisées
│   │   └── mongo_client.py      # zone curated : upsert idempotent, index, agrégations
│   ├── ingestion/
│   │   ├── run_tracker.py       # trace chaque run dans la table ingestion_runs
│   │   ├── ingest_file.py       # CSV UCI → S3 raw
│   │   ├── ingest_velib_api.py  # GBFS Vélib → S3 raw
│   │   └── ingest_weather_api.py# Open-Meteo → S3 raw
│   ├── transformation/
│   │   ├── raw_to_staging.py    # S3 → MySQL (nettoyage, jointure, typage)
│   │   ├── features.py          # features métier (fonctions pures)
│   │   ├── anomaly_detection.py # score d'anomalie (IsolationForest + fallback)
│   │   └── staging_to_curated.py# MySQL → MongoDB (enrichissement + upsert)
│   ├── api/
│   │   ├── main.py              # app FastAPI, montage des routers
│   │   ├── schemas.py           # modèles Pydantic
│   │   ├── routes_health.py     # GET /health
│   │   ├── routes_raw.py        # GET /raw
│   │   ├── routes_staging.py    # GET /staging
│   │   ├── routes_curated.py    # GET /curated
│   │   └── routes_stats.py      # GET /stats
│   └── utils/
│       ├── logging.py           # logger unifié
│       ├── timing.py            # chronométrage (Timer, measure)
│       ├── time_utils.py        # helpers datetime UTC
│       └── http.py              # client HTTP resilient (retry tenacity)
├── scripts/
│   ├── setup_buckets.py         # crée les buckets S3
│   ├── init_mysql.py            # crée le schéma MySQL + index MongoDB
│   └── run_ingestion_once.py    # lance les 3 ingestions vers raw
└── data/
    └── raw_files/               # hour.csv / day.csv (dataset UCI, non versionné en git)
```

---

## Installation & lancement pas à pas

### 1. Cloner le dépôt

```bash
git clone https://github.com/<votre-utilisateur>/VeliLake.git
cd VeliLake
```

### 2. Environnement Python (uv)

```bash
uv venv                    # crée l'environnement virtuel .venv
source .venv/bin/activate  # active l'environnement (prompt : (VeliLake))
uv sync                    # installe TOUTES les dépendances depuis uv.lock
```

- `uv venv` crée un environnement isolé dans `.venv/`.
- `uv sync` installe exactement les versions verrouillées dans `uv.lock` (reproductible à l'identique).
- Le projet est installé en mode éditable : le package `src` est importable partout
  (`from src.config import settings`). C'est pourquoi on lance toujours les scripts
  **depuis la racine** avec `python scripts/...`, jamais `python3` hors venv.

> Pour ajouter une dépendance plus tard : `uv add <paquet>` (met à jour `pyproject.toml`
> **et** `uv.lock`). Réserve `uv pip install` au dépannage ponctuel — il ne met pas à jour
> les fichiers du projet.

### 3. Configuration (`.env`)

```bash
cp .env.example .env
sed -i 's/localhost/127.0.0.1/g' .env   # force TCP (voir Dépannage : erreur 1698)
```

Le `.env.example` est déjà calé sur le `docker-compose.yml` (identifiants `root`/`root`,
base `staging`, bucket `raw`, credentials LocalStack `test`/`test`). Ces valeurs ne sont
pas des secrets : ce sont des identifiants locaux de conteneurs. `.env` est ignoré par git.

### 4. Lancer les services Docker

```bash
docker compose up -d       # démarre LocalStack (S3), MySQL, MongoDB en arrière-plan
docker ps                  # vérifie que les 3 conteneurs sont "healthy"
```

- `localstack` (port 4566) simule S3 en local.
- `mysql` (port 3306) — zone staging. **Peut prendre ~60 s** à s'initialiser.
- `mongodb` (port 27017) — zone curated.

Attends que `dl_mysql` soit `healthy` avant l'étape suivante.

### 5. Initialiser buckets et schéma

```bash
python scripts/setup_buckets.py   # crée le bucket S3 "raw" dans LocalStack
python scripts/init_mysql.py      # crée les 4 tables MySQL + les index MongoDB
```

- `setup_buckets.py` appelle `s3_client.ensure_bucket()` (idempotent).
- `init_mysql.py` crée `bike_history`, `velib_station_status`, `weather_snapshots`,
  `ingestion_runs` (via `mysql_client.init_schema()`) et les index MongoDB
  (`mongo_client.ensure_indexes()`, dont l'index unique `station_id + timestamp`).
  La sortie doit lister les 4 tables à **0 ligne**.

### 6. (Optionnel) Dataset UCI + DVC

Le pipeline fonctionne sans le CSV (les APIs suffisent), mais pour la source *fichier* :

```bash
cd data/raw_files
curl -L -o bike.zip "https://archive.ics.uci.edu/static/public/275/bike+sharing+dataset.zip"
unzip bike.zip && rm bike.zip     # produit hour.csv, day.csv, Readme.txt
cd ../..
```

**Versionnement du dataset avec DVC** (optionnel — le fichier est trop lourd pour git) :

```bash
uv tool install "dvc[s3]"                       # dvc comme outil CLI isolé (hors venv projet)
dvc init
dvc remote add -d localstack-s3 s3://dvc-store
dvc remote modify localstack-s3 endpointurl http://127.0.0.1:4566
dvc remote modify --local localstack-s3 access_key_id test
dvc remote modify --local localstack-s3 secret_access_key test
python scripts/setup_buckets.py                 # (crée aussi le bucket dvc-store si ajouté)
dvc add data/raw_files/hour.csv data/raw_files/day.csv
dvc push
git add data/raw_files/*.dvc .dvc/config .dvcignore
git commit -m "Track UCI bike-sharing dataset with DVC"
```

DVC versionne uniquement le **fichier statique** UCI. Les zones staging/curated étant des
bases de données (état non-fichier), elles ne sont pas gérées par DVC ; l'orchestration est
confiée à Airflow. C'est un choix assumé : un seul orchestrateur, DVC cantonné à sa force
(le versionnement de fichiers).

### 7. Ingestion → zone Raw

```bash
python scripts/run_ingestion_once.py            # lance les 3 sources
# options : --skip-file / --skip-velib / --skip-weather
```

Ce script appelle les trois modules d'ingestion. Chaque source est isolée (l'échec de l'une
n'arrête pas les autres) et écrit une ligne dans `ingestion_runs` :

- `ingest_velib_api.py` → `raw/api/velib/<date>/velib_status_*.json` + `velib_information_*.json`
- `ingest_weather_api.py` → `raw/api/weather/<date>/weather_snapshot_*.json`
- `ingest_file.py` → `raw/file/bike_sharing/hour.csv` (si le CSV est présent)

Vérifications :

```bash
# objets bruts dans S3
aws --endpoint-url=http://127.0.0.1:4566 s3 ls s3://raw/ --recursive

# traces d'ingestion
docker exec -it dl_mysql mysql -uroot -proot staging \
  -e "SELECT source, status, records_count, duration_ms FROM ingestion_runs ORDER BY id DESC LIMIT 5;"
```

### 8. Transformation Raw → Staging

```bash
python -m src.transformation.raw_to_staging
```

Lit le dernier snapshot brut depuis S3, **extrait le champ imbriqué
`num_bikes_available_types`** (nombre de vélos électriques), **joint** status × information
Vélib par `station_id`, nettoie/type, et insère par batch dans MySQL (`INSERT IGNORE` pour
l'idempotence). Charge aussi `hour.csv` dans `bike_history` (une seule fois).

Vérification :

```bash
docker exec -it dl_mysql mysql -uroot -proot staging -e "
SELECT 'velib' t, COUNT(*) n FROM velib_station_status
UNION ALL SELECT 'weather', COUNT(*) FROM weather_snapshots
UNION ALL SELECT 'bike', COUNT(*) FROM bike_history;"
```

### 9. Transformation Staging → Curated

```bash
python -m src.transformation.staging_to_curated
```

Calcule les **features métier** (taux d'occupation, ratio élec, taux de bornes, niveau de
tension, criticité), greffe le **contexte météo global**, score les **anomalies**
(IsolationForest sur tout le batch), et **upsert** les documents enrichis dans MongoDB.
Rejouable sans doublon (upsert sur `station_id + timestamp`).

Vérification :

```bash
docker exec -it dl_mongodb mongosh --quiet --eval '
  const c = db.getSiblingDB("curated").station_analytics;
  print("documents:", c.countDocuments());
  print("critiques:", c.countDocuments({"analytics.is_critical": true}));'
```

### 10. API Gateway

```bash
uvicorn src.api.main:app --reload --port 8000
```

Documentation interactive (Swagger) : **http://localhost:8000/docs**

Exemples :

```bash
curl -s localhost:8000/health | python -m json.tool
curl -s "localhost:8000/raw?source=velib&limit=5" | python -m json.tool
curl -s "localhost:8000/staging?table=velib_station_status&limit=3" | python -m json.tool
curl -s "localhost:8000/curated?is_critical=true&limit=3" | python -m json.tool
curl -s localhost:8000/stats | python -m json.tool
```

| Endpoint | Rôle |
|----------|------|
| `GET /health` | état de l'API + connectivité S3 / MySQL / MongoDB |
| `GET /raw` | objets de la zone brute (filtres `source`, `prefix`, `limit`, `preview`) |
| `GET /staging` | lignes MySQL (whitelist de tables, filtres `station_id`, `source`) |
| `GET /curated` | documents enrichis (filtres `station_id`, `tension_level`, `is_critical`) |
| `GET /stats` | volumes par zone + indicateurs Vélib (occupation, top vides/pleines, critiques) |

---

## Récapitulatif des commandes

```bash
# --- setup ---
uv venv && source .venv/bin/activate && uv sync
cp .env.example .env && sed -i 's/localhost/127.0.0.1/g' .env
docker compose up -d
python scripts/setup_buckets.py
python scripts/init_mysql.py

# --- pipeline complet (manuel) ---
python scripts/run_ingestion_once.py
python -m src.transformation.raw_to_staging
python -m src.transformation.staging_to_curated

# --- API ---
uvicorn src.api.main:app --reload --port 8000
```

---

## Dépannage

**`docker compose up` → "cannot connect to the Docker daemon"**
Le daemon Docker n'est pas lancé. Démarre Docker Desktop (macOS/Windows/WSL2), ou
`sudo systemctl start docker` (Linux). Vérifie avec `docker ps`.

**`ModuleNotFoundError: No module named 'src'`**
Le projet n'est pas installé en éditable ou le venv n'est pas actif.
`source .venv/bin/activate && uv sync`, puis lance depuis la racine avec `python scripts/...`.

**MySQL `Access denied for user 'root'@'localhost'` (erreur 1698 / 28000)**
Erreur `auth_socket` : la connexion tombe sur un MySQL **installé localement** (via le socket
Unix) au lieu du conteneur. Deux correctifs :

- mettre `MYSQL_HOST=127.0.0.1` dans `.env` (force TCP) ;
- si un MySQL système occupe déjà `127.0.0.1:3306` (`sudo ss -ltnp | grep 3306`), soit
  l'arrêter (`sudo systemctl stop mysql && sudo systemctl disable mysql`), soit remapper le
  conteneur sur un autre port dans `docker-compose.yml` (`"3307:3306"`) et poser
  `MYSQL_PORT=3307` dans `.env`.

**`ImportError: cannot import name 'DocumentModifiedShape' from 'botocore...'`**
Versions boto3/botocore désynchronisées.
`uv pip install --force-reinstall "boto3>=1.34" "botocore>=1.34"`, puis vérifie qu'elles
partagent la même série de version. Si un botocore de conda `base` interfère, `conda deactivate`.

---

## À venir (roadmap)

- Endpoints avancés `POST /ingest` et `POST /ingest_fast` (+ benchmark de performance).
- DAG Airflow `smart_mobility_data_lake` orchestrant l'ensemble du pipeline.
- Tests `pytest` et checklist de conformité aux exigences du sujet.
