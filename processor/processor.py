"""Spark Structured Streaming processor: Kafka -> validation -> MongoDB.

Reads long-format weather messages (one JSON object per metric) from Kafka,
validates them, and upserts them into MongoDB. The dashboard reads the same
collection and pivots metric_type/value into temperature, humidity, etc.
"""
import math
import os
import re
from datetime import datetime, timezone

from pymongo import ASCENDING, MongoClient, ReplaceOne
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "streaming-data")
STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://mongodb:27017")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "weather_db")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "weather_data")

CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/tmp/spark-weather-checkpoint")

weather_schema = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("metric_type", StringType(), True),
        StructField("sensor_id", StringType(), True),
        StructField("location", StringType(), True),
        StructField("unit", StringType(), True),
        StructField("country", StringType(), True),
        StructField("condition", StringType(), True),
        StructField("source", StringType(), True),
    ]
)

# Fields that must be present for a record to be stored.
REQUIRED_FIELDS = ("timestamp", "value", "metric_type", "sensor_id", "location", "unit")
OPTIONAL_FIELDS = ("country", "condition", "source")
# Fields that identify one reading; used for idempotent upserts and the unique index.
KEY_FIELDS = ("timestamp", "metric_type", "location", "sensor_id")


def parse_timestamp(text):
    """Parse an ISO-8601 string into a timezone-aware UTC datetime."""
    text = text.strip().replace("Z", "+00:00")
    # Python 3.8 (Spark image) only accepts 3 or 6 fractional digits; normalise to 6.
    text = re.sub(r"\.(\d+)", lambda m: "." + m.group(1)[:6].ljust(6, "0"), text, count=1)
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def build_documents(rows):
    """Validate row dicts and return (documents, skipped_count).

    A row is skipped when a required field is missing/blank, the value is not a
    finite number, or the timestamp cannot be parsed. Rows that fail JSON parsing
    in Spark arrive here as all-None rows and are skipped the same way.
    """
    documents, skipped = [], 0
    for row in rows:
        try:
            for name in REQUIRED_FIELDS:
                field = row.get(name)
                if field is None or (isinstance(field, str) and not field.strip()):
                    raise ValueError(f"missing {name}")
            value = float(row["value"])
            if not math.isfinite(value):
                raise ValueError("non-finite value")
            document = {name: row[name].strip() for name in REQUIRED_FIELDS if name not in ("timestamp", "value")}
            document["timestamp"] = parse_timestamp(row["timestamp"])
            document["value"] = value
            for name in OPTIONAL_FIELDS:
                document[name] = row.get(name)
            document["ingested_at"] = datetime.now(timezone.utc)
            documents.append(document)
        except (TypeError, ValueError, AttributeError):
            skipped += 1
    return documents, skipped


def ensure_indexes(collection):
    collection.create_index([("timestamp", -1)], name="timestamp_desc")
    try:
        collection.create_index(
            [(name, ASCENDING) for name in KEY_FIELDS], unique=True, name="unique_reading"
        )
    except Exception as error:  # e.g. pre-existing duplicates from an older run
        print(f"Warning: could not create unique index: {error}", flush=True)


def write_documents(collection, documents):
    """Upsert documents so that a replayed micro-batch never creates duplicates."""
    operations = [
        ReplaceOne({name: doc[name] for name in KEY_FIELDS}, doc, upsert=True)
        for doc in documents
    ]
    return collection.bulk_write(operations, ordered=False)


def save_batch_to_mongodb(batch_df, batch_id):
    rows = [row.asDict() for row in batch_df.collect()]
    if not rows:
        print(f"Batch {batch_id}: empty batch", flush=True)
        return

    documents, skipped = build_documents(rows)
    if skipped:
        print(f"Batch {batch_id}: skipped {skipped} invalid record(s)", flush=True)
    if not documents:
        print(f"Batch {batch_id}: no valid records", flush=True)
        return

    client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
    try:
        collection = client[MONGODB_DATABASE][MONGODB_COLLECTION]
        ensure_indexes(collection)
        result = write_documents(collection, documents)
        print(
            f"Batch {batch_id}: stored {len(documents)} record(s) "
            f"(inserted={result.upserted_count}, matched={result.matched_count}) "
            f"in {MONGODB_DATABASE}.{MONGODB_COLLECTION}",
            flush=True,
        )
    finally:
        client.close()
    # A MongoDB error propagates, fails the micro-batch and Spark retries it from the
    # checkpoint instead of silently losing data.


def main():
    spark = SparkSession.builder.appName("WeatherStreamingProcessor").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_stream = (
        kafka_stream.selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), weather_schema).alias("data"))
        .select("data.*")
    )

    query = (
        parsed_stream.writeStream.foreachBatch(save_batch_to_mongodb)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="5 seconds")
        .start()
    )

    print(f"Listening to Kafka topic '{KAFKA_TOPIC}' at {KAFKA_BOOTSTRAP_SERVERS}", flush=True)
    print(f"Writing to MongoDB {MONGODB_DATABASE}.{MONGODB_COLLECTION} at {MONGODB_URI}", flush=True)

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping Spark streaming processor...", flush=True)
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
