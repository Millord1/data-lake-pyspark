import json
import os
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

from src.config.database import KAFKA_TOPIC, SourcesUrls

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = KAFKA_TOPIC


def delivery_report(err, msg):
    if err is not None:
        print(f"Échec de l'envoi du message : {err}")
    else:
        print(f"Message envoyé à {msg.topic()} [{msg.partition()}]")


def fetch_and_publish_velib_data():
    producer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "velib-producer",
    }
    producer = Producer(producer_config)

    print("Interrogation de l'API Vélib")

    offset = 0
    limit = 100
    total_records = 0

    while True:
        url = SourcesUrls.velib_api + f"limit={limit}&offset={offset}"
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Erreur API ({response.status_code}): {response.text}")
            break

        data = response.json()
        results = data.get("results", [])
        if not results:
            break

        now_utc = datetime.now(timezone.utc)
        ts_utc_str = now_utc.strftime("%Y-%m-%d %H:%M:%S")

        minute_bin = (now_utc.minute // 5) * 5
        tbin_utc_str = now_utc.replace(
            minute=minute_bin, second=0, microsecond=0
        ).strftime("%Y-%m-%d %H:%M:%S")

        for record in results:
            station_event = {
                "ts_utc": ts_utc_str,
                "tbin_utc": tbin_utc_str,
                "station_id": str(record.get("stationcode")),
                "bikes": int(record.get("numbikesavailable", 0)),
                "capacity": int(record.get("capacity", 0)),
                "mechanical": int(record.get("mechanical", 0))
                if "mechanical" in record
                else 0,
                "ebike": int(record.get("ebike", 0)) if "ebike" in record else 0,
            }

            producer.produce(
                topic=TOPIC_NAME,
                key=str(station_event["station_id"]),
                value=json.dumps(station_event),
                callback=delivery_report,
            )
            total_records += 1

        offset += limit
        producer.poll(0)

    producer.flush()
    print(f"Total publié : {total_records} enregistrements.")


if __name__ == "__main__":
    POLL_INTERVAL_SECONDS = 900
    while True:
        try:
            fetch_and_publish_velib_data()
        except Exception as e:
            print(f"Erreur lors du cycle de production : {e}")
        time.sleep(POLL_INTERVAL_SECONDS)
