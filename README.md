# VeliLake

**Data lake multi-zones pour l'analyse de disponibilité des vélos partagés (Vélib).**

Projet final — EFREI *Data Lakes & Data Integration* (2025-2026).

VeliLake ingère des données de mobilité urbaine, les fait transiter par trois zones
(brute → intermédiaire → enrichie), et les expose via une API REST, le tout orchestré
par Airflow. Trois sources sont combinées :

- **Vélib Métropole (API GBFS)** — disponibilité temps réel des ~1 500 stations parisiennes ;
- **UCI Bike Sharing Dataset (fichier CSV)** — historique horaire de location (Washington D.C., 2011-2012) ;
- **Open-Meteo (API)** — météo courante de Paris, pour enrichir les snapshots.

> Nom de package Python : `smart-mobility-data-lake`. Nom d'affichage : **VeliLake**.

---

## 1. Architecture

| Zone | Rôle | Techno |
|------|------|--------|
| **Raw** | copie fidèle des sources, sans transformation | **S3** (LocalStack) |
| **Staging** | données nettoyées, typées, requêtables en SQL | **MySQL** |
| **Curated** | documents enrichis (features métier, météo, anomalies) | **MongoDB** |
| **Orchestration** | planification du pipeline | **Airflow** |
| **API Gateway** | exposition REST des trois zones | **FastAPI** |

Flux : `sources → raw (S3) → staging (MySQL) → curated (MongoDB) → API`.
La zone raw en S3 respecte la contrainte du sujet (raw en S3 ou Elasticsearch).

**Détection d'anomalie** : un `IsolationForest` (scikit-learn) score chaque station par
rapport à l'ensemble du réseau à l'instant t ; repli sur un z-score si le batch est trop petit.

---

## 2. Prérequis

- **Docker** + **Docker Compose v2** (≥ 4 Go alloués à Docker, idéalement 8 Go pour Airflow)
- **[uv](https://docs.astral.sh/uv/)** (environnement et dépendances Python)
- **Python ≥ 3.10** (développé/testé en 3.12)

> **Sous Windows/WSL2** : allouez de la RAM à WSL2 via `C:\Users\<user>\.wslconfig`
> (`[wsl2]` puis `memory=8GB`), puis `wsl --shutdown`. Airflow est gourmand ; sans ça
> son interface web peut ne pas démarrer (voir §11 Dépannage).

---

## 3. Structure du projet

```
VeliLake/
├── docker-compose.yml          # LocalStack + MySQL + MongoDB + Airflow
├── pyproject.toml              # dépendances + packaging (uv)
├── uv.lock                     # versions verrouillées (reproductibilité)
├── .env.example                # gabarit de configuration
├── src/
│   ├── config.py               # config centrale (pydantic-settings)
│   ├── storage/                # clients S3 / MySQL / MongoDB
│   ├── ingestion/              # ingestion fichier + APIs -> raw
│   ├── transformation/         # raw->staging, staging->curated, features, anomalies
│   ├── api/                    # FastAPI : main + routes + ingest_service + schemas
│   └── utils/                  # logging, timing, http (retry), time_utils
├── dags/
│   └── smart_mobility_data_lake.py   # DAG Airflow orchestrant tout le pipeline
├── scripts/
│   ├── setup_buckets.py        # crée les buckets S3
│   ├── init_mysql.py           # crée le schéma MySQL + index MongoDB
│   ├── run_ingestion_once.py   # lance les 3 ingestions vers raw
│   └── benchmark_ingest.py     # benchmark /ingest vs /ingest_fast
├── tests/                      # tests pytest (features, schemas, health)
├── reports/                    # rapports de performance (générés)
└── data/raw_files/             # hour.csv / day.csv (dataset UCI, non versionné en git)
```

---

## 4. Installation pas à pas

### 4.1 Cloner le dépôt
```bash
git clone https://github.com/<votre-utilisateur>/VeliLake.git
cd VeliLake
```

### 4.2 Environnement Python (uv)
```bash
uv venv                      # crée l'environnement virtuel .venv
source .venv/bin/activate    # active (prompt : (VeliLake))
uv sync                      # installe TOUTES les dépendances depuis uv.lock
```
`uv sync` installe exactement les versions verrouillées (reproductible à l'identique).
Le package `src` est installé en éditable : lancez toujours les scripts **depuis la racine**
avec `python scripts/...`.

### 4.3 Configuration (`.env`)
```bash
cp .env.example .env
sed -i 's/localhost/127.0.0.1/g' .env    # force TCP (évite le socket MySQL local, cf §11)
```
Le `.env.example` est déjà aligné sur le `docker-compose.yml` (identifiants `root`/`root`,
base `staging`, bucket `raw`, credentials LocalStack `test`/`test`). Ces valeurs sont des
identifiants **locaux de conteneurs**, pas des secrets. `.env` est ignoré par git.

### 4.4 Lancer les services Docker
```bash
docker compose up -d                     # LocalStack, MySQL, MongoDB, Airflow
docker ps                                # les conteneurs doivent être "healthy"
```
- `dl_localstack` (4566) — S3 (zone raw)
- `dl_mysql` (3306) — staging ; **~60 s** d'initialisation
- `dl_mongodb` (27017) — curated
- `dl_airflow` (8080) — orchestration ; **premier démarrage lent** (installe ses dépendances)

> Pour ne démarrer que les bases de données (sans Airflow) : `docker compose up -d localstack mysql mongodb`.

### 4.5 Initialiser buckets et schéma
```bash
python scripts/setup_buckets.py   # crée le bucket S3 "raw"
python scripts/init_mysql.py      # crée les 4 tables MySQL + les index MongoDB
```
La sortie de `init_mysql.py` doit lister les 4 tables à **0 ligne**.

### 4.6 (Optionnel) Dataset UCI + DVC
Le pipeline fonctionne sans le CSV (les APIs suffisent), mais pour la source *fichier* :
```bash
cd data/raw_files
curl -L -o bike.zip "https://archive.ics.uci.edu/static/public/275/bike+sharing+dataset.zip"
unzip bike.zip && rm bike.zip     # -> hour.csv, day.csv, Readme.txt
cd ../..
```
**Versionnement du dataset avec DVC** (optionnel) :
```bash
uv tool install "dvc[s3]"
dvc pull            # si le dépôt contient déjà les .dvc et un remote configuré
```
DVC ne versionne que le **fichier statique** UCI ; les zones staging/curated sont des bases
de données (état non-fichier), gérées par Airflow. Choix assumé : un seul orchestrateur.

---

## 5. Exécuter le pipeline

Deux voies équivalentes. **La voie B (Airflow) est celle demandée par le sujet** ; la voie A
sert au développement et au débogage.

### Voie A — manuelle (scripts)
```bash
python scripts/run_ingestion_once.py            # sources -> raw (S3)
python -m src.transformation.raw_to_staging     # raw -> staging (MySQL)
python -m src.transformation.staging_to_curated # staging -> curated (MongoDB)
```

### Voie B — orchestrée (Airflow)
Le DAG `smart_mobility_data_lake` enchaîne : `setup_infrastructure` → (`ingest_file_to_raw`,
`ingest_api_to_raw`) → `raw_to_staging` → `staging_to_curated` → `validate_pipeline`.
Il est planifié toutes les 15 min et déclenchable manuellement.

**Récupérer le mot de passe admin** (généré au démarrage) :
```bash
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
```

**Via l'interface web** : ouvrir http://localhost:8080 (login `admin` + mot de passe
ci-dessus), activer le DAG `smart_mobility_data_lake`, puis le déclencher (▶).

**Via la CLI** (utile si l'UI ne démarre pas faute de RAM — voir §11) :
```bash
# déclencher le run complet
docker compose exec airflow airflow dags trigger smart_mobility_data_lake

# exécuter tâche par tâche, immédiatement, sans scheduler (idéal démo)
RID=$(date +manual__%Y-%m-%dT%H:%M:%S+00:00)
docker compose exec airflow airflow dags trigger smart_mobility_data_lake -r $RID
for t in setup_infrastructure ingest_api_to_raw ingest_file_to_raw \
         raw_to_staging staging_to_curated validate_pipeline; do
  docker compose exec airflow airflow tasks test smart_mobility_data_lake $t $RID
done
```
La tâche `validate_pipeline` affiche un résumé des trois zones ; elle échoue si une zone est vide.

---

## 6. API Gateway

Lancer l'API (services Docker up, venv actif) :
```bash
uvicorn src.api.main:app --reload --port 8000
```
Documentation interactive (Swagger) : **http://localhost:8000/docs**

| Endpoint | Rôle |
|----------|------|
| `GET /health` | état de l'API + connectivité S3 / MySQL / MongoDB |
| `GET /raw` | objets de la zone brute (`source`, `prefix`, `limit`, `preview`) |
| `GET /staging` | lignes MySQL (whitelist de tables ; `station_id`, `source`, `limit`) |
| `GET /curated` | documents enrichis (`station_id`, `tension_level`, `is_critical`, `limit`) |
| `GET /stats` | volumes par zone + indicateurs Vélib |
| `POST /ingest` | ingestion record par record (baseline) |
| `POST /ingest_fast` | ingestion optimisée par lots |

Exemples :
```bash
curl -s localhost:8000/health | python -m json.tool
curl -s "localhost:8000/raw?source=velib&limit=5" | python -m json.tool
curl -s "localhost:8000/staging?table=velib_station_status&limit=3" | python -m json.tool
curl -s "localhost:8000/curated?is_critical=true&limit=3" | python -m json.tool
curl -s localhost:8000/stats | python -m json.tool
```

---

## 7. Endpoints avancés `/ingest` vs `/ingest_fast`

Les deux acceptent le même payload et propagent un batch de stations dans les trois zones.
Ils font le **même travail logique** ; seule la stratégie d'I/O diffère :

| | `/ingest` (standard) | `/ingest_fast` (optimisé) |
|---|---|---|
| S3 (raw) | 1 objet **par record** | 1 objet **pour tout le batch** |
| MySQL (staging) | `INSERT` + `commit` par record | 1 `executemany` + 1 `commit` |
| MongoDB (curated) | `replace_one` par record | 1 `bulk_write` |
| Features | record par record | en lot + anomalie sur le batch |

Test manuel :
```bash
curl -s -X POST localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{"source":"manual","data":[{"station_id":"test1","num_bikes_available":5,"num_docks_available":10,"num_ebikes_available":2,"capacity":15,"lat":48.85,"lon":2.35}]}'
```
Réponse : `{"status":"ok","records_processed":1,"duration_ms":...,"mode":"standard"}`.

---

## 8. Benchmark de performance

Prérequis : l'API tourne et les services Docker sont up.
```bash
python scripts/benchmark_ingest.py --runs 5
```
Le script génère des batchs de 1 et 100 stations, appelle chaque endpoint (1 warm-up +
N mesures), moyenne la durée mesurée **côté serveur** (le temps réel du pipeline), et écrit
`reports/performance_results.json` + `reports/performance_report.md`.

**Résultats obtenus sur la machine de développement** (WSL2, LocalStack local) :

| Batch | `/ingest` | `/ingest_fast` | Speedup | Amélioration |
|------:|----------:|---------------:|--------:|-------------:|
| 1 | ~467 ms | ~197 ms | ×2.37 | +57.8 % |
| 100 | ~12 760 ms | ~915 ms | ×13.94 | **+92.8 %** |

L'exigence du sujet (≥ 30 % plus rapide sur un batch de 100) est largement satisfaite. Le gain
vient de la réduction des aller-retours réseau/disque (100 commits MySQL → 1, 100 écritures
Mongo → 1, 100 objets S3 → 1). Les chiffres dépendent de la machine ; relancez le script pour
obtenir les vôtres.

---

## 9. Tests
```bash
uv pip install -e ".[dev]"    # pytest + httpx
pytest -v
```
18 tests unitaires couvrant les features métier, le scoring d'anomalie, les schémas Pydantic et
la forme de l'API. Ils s'exécutent **sans Docker** (aucun service requis).

---

## 10. Récapitulatif des commandes
```bash
# --- setup ---
uv venv && source .venv/bin/activate && uv sync
cp .env.example .env && sed -i 's/localhost/127.0.0.1/g' .env
docker compose up -d
python scripts/setup_buckets.py
python scripts/init_mysql.py

# --- pipeline (voie manuelle) ---
python scripts/run_ingestion_once.py
python -m src.transformation.raw_to_staging
python -m src.transformation.staging_to_curated

# --- pipeline (voie Airflow) ---
docker compose exec airflow airflow dags trigger smart_mobility_data_lake

# --- API + benchmark + tests ---
uvicorn src.api.main:app --reload --port 8000
python scripts/benchmark_ingest.py --runs 5
pytest -v
```

---

## 11. Dépannage

**`docker compose up` → "cannot connect to the Docker daemon"**
Démarrer Docker Desktop (macOS/Windows/WSL2) ou `sudo systemctl start docker` (Linux).

**`ModuleNotFoundError: No module named 'src'`**
Venv inactif ou projet non installé en éditable : `source .venv/bin/activate && uv sync`,
puis lancer depuis la racine avec `python scripts/...`.

**MySQL `Access denied for user 'root'@'localhost'` (erreur 1698 / 28000)**
Un MySQL installé localement intercepte la connexion via le socket Unix. Corrigez :
- `MYSQL_HOST=127.0.0.1` dans `.env` (force TCP) ;
- si un MySQL système occupe déjà `127.0.0.1:3306` (`sudo ss -ltnp | grep 3306`), arrêtez-le
  (`sudo systemctl stop mysql`) **ou** remappez le conteneur (`"3307:3306"` dans le compose) et
  posez `MYSQL_PORT=3307` dans `.env` (uniquement en local ; laissez 3306 dans `.env.example`).

**`ImportError: cannot import name 'DocumentModifiedShape' from 'botocore...'`**
boto3/botocore désynchronisés : `uv pip install --force-reinstall "boto3>=1.34" "botocore>=1.34"`.

**`/raw` ou `/stats` renvoient une erreur / le bucket a disparu**
LocalStack communautaire ne persiste pas S3 au redémarrage. Recréez et réingérez :
`python scripts/setup_buckets.py && python scripts/run_ingestion_once.py`. L'API recrée aussi
le bucket à son démarrage.

**L'interface Airflow (`:8080`) ne démarre pas / se ferme**
Manque de RAM (fréquent sous WSL2) : le webserver est tué (`No response from gunicorn master`).
Allouez 8 Go à WSL2 (`.wslconfig`), et/ou ajoutez au service `airflow` du compose :
`AIRFLOW__WEBSERVER__WORKERS=2` et `AIRFLOW__WEBSERVER__WEB_SERVER_MASTER_TIMEOUT=300`.
**Alternative sans UI** : pilotez le DAG en CLI (`airflow tasks test ...`, cf §5 voie B) — le
scheduler et le webserver ne sont pas nécessaires pour exécuter et prouver le pipeline.

---

## 12. Choix techniques, limites et améliorations

**Choix techniques**
- Zones raw/staging/curated alignées sur les TP (reconnaissables par l'évaluateur).
- Idempotence : `INSERT IGNORE` (MySQL) et upsert sur `(station_id, timestamp)` (MongoDB).
- Météo traitée comme **contexte global** de la ville greffé à toutes les stations d'un
  snapshot (pas une jointure par station) — assumé et documenté.
- La zone raw est considérée **réingérable** depuis les sources live (cohérent avec
  LocalStack qui ne persiste pas).

**Limites**
- `top5_emptiest/fullest` de `/stats` trie sur toute la collection curated (correct tant qu'un
  seul snapshot est présent).
- Le pipeline transforme le **dernier** snapshot ; l'historisation complète nécessiterait de
  boucler sur les timestamps.
- Interface Airflow dépendante de la RAM allouée (contournable en CLI).

**Améliorations possibles**
- Remote DVC durable (S3 réel) pour un `dvc pull` reproductible hors LocalStack.
- Indexation géospatiale (Uber H3) pour l'analyse spatiale.
- Modèle prédictif de saturation de station (au-delà de la détection d'anomalie).