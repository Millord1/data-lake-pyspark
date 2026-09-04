import json
import os
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

from src.config.database import KAFKA_TOPIC, StorageConfig
from src.driver.duckdb_driver import DuckDBConnector

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")


MINIO_ACCESS_KEY = os.environ.get("MINIO_USER")
MINIO_SECRET_KEY = os.environ.get("MINIO_PASSWORD")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")


def flush_batch_to_minio(records: list[dict]) -> None:
    """Send data to minIO

    Args:
        records (list[dict]): All the data to save
    """

    if not records:
        return

    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    base_path = StorageConfig.get_bucket_path(
        raw=True,
        dataset_name="historique",
    )
    target_path = f"{base_path}/velib_realtime_{timestamp_str}.parquet"

    with DuckDBConnector() as db:
        db.execute(
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
        f"Micro-batch de {len(records)} lignes écrit avec succès " f"dans {target_path}"
    )


def run_consumer(batch_size: int = 1000, timeout_seconds: float = 10.0):
    """Start Kafka consumer

    Args:
        batch_size (int, optional): Batch size to create. Defaults to 1000.
        timeout_seconds (float, optional): Timeout setup. Defaults to 10.0.
    """
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "velib-s3-ingestion-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
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
