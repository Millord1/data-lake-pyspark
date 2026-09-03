import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

from src.config.database import KAFKA_TOPIC, StorageConfig

project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))


load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")

print("========= Consumer: " + KAFKA_BOOTSTRAP_SERVERS)


MINIO_ACCESS_KEY = os.environ.get("MINIO_USER")
MINIO_SECRET_KEY = os.environ.get("MINIO_PASSWORD")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")


def flush_batch_to_minio(records: list[dict]):
    if not records:
        return

    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(
            f"""
            CREATE OR REPLACE SECRET minio (
                TYPE s3,
                PROVIDER config,
                KEY_ID '{MINIO_ACCESS_KEY}',
                SECRET '{MINIO_SECRET_KEY}',
                ENDPOINT '{MINIO_ENDPOINT}',
                REGION 'us-east-1',
                URL_STYLE 'path',
                USE_SSL false
            );
            """
        )

        timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

        base_path = StorageConfig.get_bucket_path(raw=True, dataset_name="historique")
        target_path = f"{base_path}/velib_realtime_{timestamp_str}.parquet"

        con.execute(
            f"""
            COPY (
                SELECT
                    record.ts_utc,
                    record.tbin_utc,
                    record.station_id,
                    record.bikes,
                    record.capacity,
                    record.mechanical,
                    record.ebike
                FROM (
                    SELECT unnest(
                        from_json(
                            ?,
                            '[{{
                                "ts_utc": "VARCHAR",
                                "tbin_utc": "VARCHAR",
                                "station_id": "VARCHAR",
                                "bikes": "BIGINT",
                                "capacity": "BIGINT",
                                "mechanical": "BIGINT",
                                "ebike": "BIGINT"
                            }}]'
                        )
                    ) AS record
                )
            )
            TO '{target_path}'
            (
                FORMAT PARQUET,
                COMPRESSION ZSTD
            );
            """,
            [json.dumps(records)],
        )

        print(
            f"Micro-batch de {len(records)} lignes écrit avec succès dans {target_path}"
        )

    finally:
        con.close()


def run_consumer(batch_size: int = 1000, timeout_seconds: float = 10.0):
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "velib-s3-ingestion-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # Commit manuel post-écriture MinIO
    }

    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])

    print("Consumer Kafka démarré. Écoute du topic 'velib-status'...")

    buffer = []
    last_flush_time = datetime.now(timezone.utc)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"Erreur Kafka : {msg.error()}")
                else:
                    payload = json.loads(msg.value().decode("utf-8"))
                    buffer.append(payload)

            elapsed = (datetime.now(timezone.utc) - last_flush_time).total_seconds()
            if len(buffer) >= batch_size or (buffer and elapsed >= timeout_seconds):
                flush_batch_to_minio(buffer)
                consumer.commit(asynchronous=False)
                buffer.clear()
                last_flush_time = datetime.now(timezone.utc)

    except KeyboardInterrupt:
        print("Arrêt du consumer...")
    finally:
        if buffer:
            flush_batch_to_minio(buffer)
            consumer.commit(asynchronous=False)
        consumer.close()


if __name__ == "__main__":
    run_consumer()
