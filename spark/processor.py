import os
from datetime import datetime, timezone

from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "streaming-data")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "streaming_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "sensor_readings")

CHECKPOINT_LOCATION = os.getenv(
    "CHECKPOINT_LOCATION", "/tmp/spark-sensor-checkpoint"
)

weather_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("metric_type", StringType(), True),
    StructField("sensor_id", StringType(), True),
    StructField("location", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("country", StringType(), True),
    StructField("condition", StringType(), True),
    StructField("source", StringType(), True),
])


def save_batch_to_mongodb(batch_df, batch_id):
    required_fields = [
        "timestamp",
        "value",
        "metric_type",
        "sensor_id",
        "location",
        "unit",
    ]

    valid_df = batch_df.filter(
        col("timestamp").isNotNull()
        & col("value").isNotNull()
        & col("metric_type").isNotNull()
        & col("sensor_id").isNotNull()
        & col("location").isNotNull()
        & col("unit").isNotNull()
    )

    records = valid_df.select(
        "timestamp",
        "value",
        "metric_type",
        "sensor_id",
        "location",
        "unit",
        "country",
        "condition",
        "source",
    ).collect()

    if not records:
        print(f"Batch {batch_id}: no valid records")
        return

    documents = []

    for row in records:
        item = row.asDict()

        try:
            timestamp_value = item["timestamp"]

            timestamp_text = timestamp_value.replace("Z", "+00:00")
            parsed_timestamp = datetime.fromisoformat(timestamp_text)

            if parsed_timestamp.tzinfo is None:
                parsed_timestamp = parsed_timestamp.replace(
                    tzinfo=timezone.utc
                )

            item["timestamp"] = parsed_timestamp
            item["value"] = float(item["value"])
            documents.append(item)

        except (TypeError, ValueError, AttributeError):
            print(
                f"Batch {batch_id}: skipped record with invalid timestamp"
            )

    if not documents:
        print(f"Batch {batch_id}: no records passed timestamp validation")
        return

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
    )

    try:
        collection = client[MONGO_DATABASE][MONGO_COLLECTION]
        collection.insert_many(documents)
        print(
            f"Batch {batch_id}: saved {len(documents)} weather records "
            f"to MongoDB"
        )
    finally:
        client.close()


def main():
    spark = (
        SparkSession.builder
        .appName("WeatherStreamingProcessor")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_stream = (
        kafka_stream
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(
            from_json(col("json_value"), weather_schema).alias("data")
        )
        .select("data.*")
    )

    query = (
        parsed_stream.writeStream
        .foreachBatch(save_batch_to_mongodb)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    print(f"Listening to Kafka topic: {KAFKA_TOPIC}")
    print(f"Kafka broker: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"MongoDB database: {MONGO_DATABASE}")
    print(f"MongoDB collection: {MONGO_COLLECTION}")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping Spark streaming processor...")
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
