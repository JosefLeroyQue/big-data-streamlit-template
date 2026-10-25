"""Producer record contract + Spark processor validation/parsing tests."""
import json
import sys
from pathlib import Path

import mongomock
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "producer"))
sys.path.insert(0, str(ROOT / "processor"))

import processor  # noqa: E402
import producer  # noqa: E402

API_PAYLOAD = {
    "location": {"name": "Manila", "country": "Philippines"},
    "current": {"temp_c": 29.4, "humidity": 74, "wind_kph": 11.2, "precip_mm": 0.3,
                "condition": {"text": "Partly cloudy"}},
}


def test_producer_records_have_every_required_field():
    records = producer.build_records(API_PAYLOAD)
    assert {r["metric_type"] for r in records} == {"temperature", "humidity", "wind_speed", "rainfall"}
    needed = set(processor.REQUIRED_FIELDS) | set(processor.OPTIONAL_FIELDS)
    for record in records:
        assert needed <= set(record), needed - set(record)
        assert all(record[f] not in (None, "") for f in needed)
        assert isinstance(record["value"], float)
        json.dumps(record)  # serializable


def test_producer_skips_metrics_the_api_did_not_return():
    payload = json.loads(json.dumps(API_PAYLOAD))
    del payload["current"]["precip_mm"]
    assert {r["metric_type"] for r in producer.build_records(payload)} == {"temperature", "humidity", "wind_speed"}


def test_producer_topic_default_matches_processor():
    assert producer.TOPIC == processor.KAFKA_TOPIC == "streaming-data"


def test_build_documents_accepts_producer_records():
    docs, skipped = processor.build_documents(producer.build_records(API_PAYLOAD))
    assert skipped == 0 and len(docs) == 4
    assert docs[0]["timestamp"].tzinfo is not None


@pytest.mark.parametrize("bad", [
    {},                                                       # failed JSON parse -> all None
    {"timestamp": "not-a-date", "value": 1.0, "metric_type": "temperature",
     "sensor_id": "s", "location": "Manila", "unit": "C"},
    {"timestamp": "2026-10-25T02:00:00Z", "value": None, "metric_type": "temperature",
     "sensor_id": "s", "location": "Manila", "unit": "C"},
    {"timestamp": "2026-10-25T02:00:00Z", "value": float("nan"), "metric_type": "temperature",
     "sensor_id": "s", "location": "Manila", "unit": "C"},
    {"timestamp": "2026-10-25T02:00:00Z", "value": 1.0, "metric_type": "temperature",
     "sensor_id": None, "location": "Manila", "unit": "C"},      # the original producer's temperature bug
    {"timestamp": "2026-10-25T02:00:00Z", "value": 1.0, "metric_type": "  ",
     "sensor_id": "s", "location": "Manila", "unit": "C"},
])
def test_build_documents_rejects_invalid(bad):
    docs, skipped = processor.build_documents([bad])
    assert docs == [] and skipped == 1


def test_write_is_idempotent():
    collection = mongomock.MongoClient()["weather_db"]["weather_data"]
    docs, _ = processor.build_documents(producer.build_records(API_PAYLOAD))
    processor.write_documents(collection, docs)
    docs_again, _ = processor.build_documents(producer.build_records(API_PAYLOAD, timestamp=docs[0]["timestamp"].isoformat()))
    processor.write_documents(collection, docs_again)  # replayed micro-batch
    assert collection.count_documents({}) == 4


def test_odd_fractional_seconds_parse():
    for ts in ("2026-10-25T02:00:00.5+00:00", "2026-10-25T02:00:00.123456789Z", "2026-10-25T02:00:00.12+00:00"):
        assert processor.parse_timestamp(ts).hour == 2


def test_naive_and_z_timestamps_become_utc():
    for ts in ("2026-10-25T02:00:00", "2026-10-25T02:00:00Z", "2026-10-25T10:00:00+08:00"):
        assert processor.parse_timestamp(ts).isoformat() == "2026-10-25T02:00:00+00:00"


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    session = (SparkSession.builder.master("local[1]").appName("test")
               .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_spark_parse_and_foreach_batch(spark, monkeypatch):
    """Runs the real from_json schema + save_batch_to_mongodb on valid and broken JSON."""
    from pyspark.sql.functions import col, from_json

    good = [json.dumps(r) for r in producer.build_records(API_PAYLOAD)]
    broken = ["this is not json", '{"value": "abc"}', json.dumps({**json.loads(good[0]), "sensor_id": None}), "{}"]
    df = spark.createDataFrame([(m,) for m in good + broken], ["json_value"])
    parsed = df.select(from_json(col("json_value"), processor.weather_schema).alias("d")).select("d.*")

    client = mongomock.MongoClient()
    monkeypatch.setattr(processor, "MongoClient", lambda *a, **k: client)
    processor.save_batch_to_mongodb(parsed, 0)
    stored = list(client[processor.MONGODB_DATABASE][processor.MONGODB_COLLECTION].find({}, {"_id": 0}))
    assert len(stored) == 4
    assert {d["metric_type"] for d in stored} == {"temperature", "humidity", "wind_speed", "rainfall"}
    assert (processor.MONGODB_DATABASE, processor.MONGODB_COLLECTION) == ("weather_db", "weather_data")
