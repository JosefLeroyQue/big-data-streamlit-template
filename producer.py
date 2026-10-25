import json
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

API_KEY = os.getenv("WEATHER_API_KEY")
LOCATION = os.getenv("WEATHER_LOCATION", "Manila")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "streaming-data")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "15"))


def fetch_weather():
    if not API_KEY:
        raise RuntimeError("WEATHER_API_KEY is missing.")

    response = requests.get(
        "https://api.weatherapi.com/v1/current.json",
        params={
            "key": API_KEY,
            "q": LOCATION,
            "aqi": "no"
        },
        timeout=20
    )

    response.raise_for_status()
    data = response.json()

    current = data["current"]
    location = data["location"]

    timestamp = datetime.now(timezone.utc).isoformat()

    measurements = [
        {
            "timestamp": timestamp,
            "value": float(current["temp_c"]),
            "metric_type": "temperature",
            "sensor_id": "weatherapi_temperature",
            "unit": "°C"
        },
        {
            "timestamp": timestamp,
            "value": float(current["humidity"]),
            "metric_type": "humidity",
            "sensor_id": "weatherapi_humidity",
            "unit": "%"
        },
        {
            "timestamp": timestamp,
            "value": float(current["wind_kph"]),
            "metric_type": "wind_speed",
            "sensor_id": "weatherapi_wind_speed",
            "unit": "kph"
        }
    ]

    for measurement in measurements:
        measurement["location"] = location["name"]
        measurement["country"] = location["country"]
        measurement["condition"] = current["condition"]["text"]
        measurement["source"] = "WeatherAPI"

    return measurements


def main():
    if not API_KEY:
        raise RuntimeError("WEATHER_API_KEY is missing. Check your .env file.")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        retries=5
    )

    print(f"Publishing WeatherAPI data for {LOCATION}")
    print(f"Kafka broker: {BOOTSTRAP_SERVERS}")
    print(f"Kafka topic: {TOPIC}")

    try:
        while True:
            try:
                weather_records = fetch_weather()

                for record in weather_records:
                    producer.send(TOPIC, record).get(timeout=30)
                    print(json.dumps(record, indent=2))

                producer.flush()
                print("Weather data successfully published.\n")

            except Exception as error:
                print(f"Error: {error}")

            time.sleep(POLL_SECONDS)

    finally:
        producer.close()


if __name__ == "__main__":
    main()
