"""WeatherAPI -> Kafka producer.

Polls WeatherAPI's current-conditions endpoint and publishes one JSON message per
metric (long format) to a Kafka topic. Every message carries all fields the Spark
processor requires: timestamp, value, metric_type, sensor_id, location, unit,
country, condition, source.
"""
import json
import os
import sys
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

# Locally, values from .env win over stale shell variables. In Docker there is no
# .env file inside the image, so the values injected by Compose are used.
load_dotenv(override=True)

API_KEY = os.getenv("WEATHER_API_KEY", "").strip()
LOCATION = os.getenv("WEATHER_LOCATION", "Manila")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "streaming-data")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))
KAFKA_CONNECT_RETRIES = int(os.getenv("KAFKA_CONNECT_RETRIES", "30"))

WEATHER_API_URL = "https://api.weatherapi.com/v1/current.json"
PLACEHOLDER_KEYS = {"", "your_weatherapi_key_here", "changeme"}

# metric_type -> (WeatherAPI field, unit)
METRICS = {
    "temperature": ("temp_c", "°C"),
    "humidity": ("humidity", "%"),
    "wind_speed": ("wind_kph", "kph"),
    "rainfall": ("precip_mm", "mm"),
}


def json_serializer(value):
    return json.dumps(value).encode("utf-8")


def build_records(payload, timestamp=None):
    """Convert a WeatherAPI /current.json payload into long-format Kafka records."""
    current = payload["current"]
    location = payload["location"]
    timestamp = timestamp or datetime.now(timezone.utc).isoformat()

    records = []
    for metric_type, (api_field, unit) in METRICS.items():
        raw_value = current.get(api_field)
        if raw_value is None:
            continue  # skip metrics the API did not return instead of sending bad data
        records.append(
            {
                "timestamp": timestamp,
                "value": float(raw_value),
                "metric_type": metric_type,
                "sensor_id": f"weatherapi_{metric_type}",
                "location": location["name"],
                "unit": unit,
                "country": location.get("country", "Unknown"),
                "condition": current.get("condition", {}).get("text", "Unknown"),
                "source": "WeatherAPI",
            }
        )
    return records


def fetch_weather():
    response = requests.get(
        WEATHER_API_URL,
        params={"key": API_KEY, "q": LOCATION, "aqi": "no"},
        timeout=20,
    )
    response.raise_for_status()
    return build_records(response.json())


def create_producer():
    """Create a KafkaProducer, retrying while the broker is still starting."""
    for attempt in range(1, KAFKA_CONNECT_RETRIES + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=json_serializer,
                key_serializer=lambda key: key.encode("utf-8"),
                acks="all",
                retries=5,
            )
        except NoBrokersAvailable:
            print(
                f"Kafka not reachable at {BOOTSTRAP_SERVERS} "
                f"(attempt {attempt}/{KAFKA_CONNECT_RETRIES}); retrying in 5s...",
                flush=True,
            )
            time.sleep(5)
    raise RuntimeError(f"Could not connect to Kafka at {BOOTSTRAP_SERVERS}")


def main():
    if API_KEY.lower() in PLACEHOLDER_KEYS:
        sys.exit(
            "WEATHER_API_KEY is missing or still a placeholder. "
            "Set a real key in your .env file (see .env.example)."
        )

    producer = create_producer()
    print(f"Publishing WeatherAPI data for {LOCATION}", flush=True)
    print(f"Kafka broker: {BOOTSTRAP_SERVERS}  topic: {TOPIC}  every {POLL_SECONDS}s", flush=True)

    try:
        while True:
            try:
                records = fetch_weather()
                for record in records:
                    producer.send(TOPIC, key=record["location"], value=record)
                producer.flush(timeout=30)
                summary = ", ".join(f"{r['metric_type']}={r['value']}{r['unit']}" for r in records)
                print(f"Published {len(records)} records for {LOCATION}: {summary}", flush=True)
            except (requests.RequestException, KeyError, ValueError, KafkaError) as error:
                # Keep running: a transient API or broker problem should not kill the loop.
                print(f"Error: {type(error).__name__}: {error}", flush=True)
            time.sleep(POLL_SECONDS)
    except KeyboardInterrupt:
        print("Stopping producer...", flush=True)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
