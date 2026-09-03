# Vélib Data Platform

Pipeline Data Engineering permettant d'ingérer, stocker, nettoyer et analyser des données Vélib avec **Kafka, MinIO, Spark, Airflow, DuckDB et MongoDB**.

## Architecture

```text
                         ┌──────────────┐
                         │  Vélib API   │
                         └──────┬───────┘
                                │
                                ▼
                         ┌──────────────┐
                         │   Producer   │
                         └──────┬───────┘
                                │
                                ▼
                         ┌──────────────┐
                         │  Redpanda    │
                         │   (Kafka)    │
                         └──────┬───────┘
                                │
                                ▼
                         ┌──────────────┐
                         │   Consumer   │
                         └──────┬───────┘
                                │
                                ▼
                         ┌──────────────┐
                         │    MinIO     │
                         │  Raw / S3    │
                         └──────┬───────┘
                                │
                                │
                 ┌──────────────▼──────────────┐
                 │           Airflow           │
                 │        Orchestration        │
                 └──────────────┬──────────────┘
                                │
                         spark-submit
                                │
                                ▼
                    ┌─────────────────────┐
                    │    Spark Cluster    │
                    │                     │
                    │  Master → Worker    │
                    └──────────┬──────────┘
                               │
                               ▼
                         ┌──────────────┐
                         │    MinIO     │
                         │   Curated    │
                         └──────┬───────┘
                                │
                                ▼
                         ┌──────────────┐
                         │   MongoDB    │
                         │  Analytics   │
                         └──────────────┘
````

## Technologies

* **Python 3.12**
* **Apache Spark 3.5.1**
* **Apache Airflow 3.3.0**
* **Redpanda / Kafka**
* **MinIO / S3**
* **DuckDB**
* **MongoDB**
* **Docker / Docker Compose**

## Structure du projet

```text
datalake-pyspark/
├── dags/                    # DAGs Airflow
│   └── velib_pipeline.py
│
├── src/
│   ├── config/              # Configuration
│   ├── ingestion/           # Ingestion des données
│   ├── clean/               # Nettoyage Spark
│   ├── analysis/            # Analyses Spark
│   ├── load/                # Chargement vers MongoDB
│   ├── kafka/               # Producer / Consumer Kafka
│   └── driver/              # Connecteurs
│
├── docker/
│   ├── app/
│   │   └── Dockerfile
│   ├── spark/
│   │   └── Dockerfile
│   ├── airflow/
│   │   └── Dockerfile
│   └── docker-compose.airflow.yml
│
├── tests/
├── .env.docker
├── pyproject.toml
└── README.md
```

## Configuration

Créer le fichier `.env.docker` à la racine du projet en se basant sur le .env.example

Exemple :

```env
MINIO_USER=minioadmin
MINIO_PASSWORD=minioadmin

MINIO_ENDPOINT=minio:9000

MONGO_USER=admin
MONGO_PASSWORD=admin

KAFKA_BOOTSTRAP_SERVERS=redpanda:9092
KAFKA_TOPIC=velib-status
```

Il faut également changer les droits d'accès au dossier **data/analysis** afin que docker compose puisse écrire dedans (export des analyses):

```bash
sudo chown -R 50000:0 data/analysis
sudo chmod -R 775 data/analysis
```

## Lancer le projet

Depuis la racine :

```bash
sudo docker compose \
  --env-file .env.docker \
  -f docker/docker-compose.airflow.yml up -d
```

## Airflow

Interface :

```text
http://localhost:8085
```

Récupérer le mot de passe:

```bash
```bash
sudo docker compose -f docker/docker-compose.airflow.yml logs airflow | grep -i -E "username|password"
```
```

Le DAG `velib_pipeline` orchestre :

```text
ingestion
    ↓
Spark cleaning
    ↓
Spark analysis
```

Airflow ne démarre pas les infrastructures : Docker Compose s'en charge.

## Spark

Spark est composé de :

```text
Spark Master
    │
    └── Spark Worker
```

Interface Master :

```text
http://localhost:8080
```

Interface Worker :

```text
http://localhost:8081
```

Spark utilise :

```text
Spark 3.5.1
Python 3.12
```

Les variables suivantes garantissent la même version Python sur le driver et les workers :

```env
PYSPARK_PYTHON=/usr/local/bin/python3
PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3
```

## MinIO

MinIO joue le rôle de Data Lake compatible S3.

Console :

```text
http://localhost:9003
```

API S3 :

```text
http://localhost:9002
```

Organisation des données :

```text
s3://data/
├── raw/
│   └── public/
│       ├── historique/
│       └── stations/
│
└── curated/
    └── public/
```

Les données sont stockées principalement au format **Parquet**.

Spark utilise **S3A** pour communiquer avec MinIO.

## Kafka / Redpanda

Redpanda remplace Kafka dans l'environnement Docker.

Flux temps réel :

```text
Vélib API
    ↓
Producer
    ↓
Redpanda
    ↓
Consumer
    ↓
MinIO
```


Depuis la machine hôte :

```text
localhost:19092
```

## Logs

Voir les logs d'un service :

```bash
sudo docker compose \
  -f docker/docker-compose.airflow.yml \
  logs -f airflow
```

## Pipeline global

Le projet combine deux flux.

### Batch

```text
Sources historiques
       ↓
   Ingestion
       ↓
      MinIO
       ↓
     Airflow
       ↓
      Spark
       ↓
 Nettoyage / Analyse
       ↓
 MinIO Curated
```

### Streaming

```text
Vélib API
    ↓
 Producer
    ↓
 Redpanda
    ↓
 Consumer
    ↓
 MinIO Raw
```

Docker Compose gère l'infrastructure, tandis qu'Airflow orchestre les traitements Data Engineering.

Accéder aux data:

Il faut ouvrir un login shell en root pour voir les data:

```bash
sudo -i
```