# The -python3 variant ships Python + PySpark; the plain tag does not.
# Its Python is old (3.8), so pymongo is pinned to the last release that supports it.
FROM apache/spark:3.5.7-python3

USER root

# Writable dirs for the Ivy cache (Kafka connector jars) and the streaming checkpoint.
RUN mkdir -p /tmp/spark-ivy /tmp/spark-weather-checkpoint && \
    chmod -R 777 /tmp/spark-ivy /tmp/spark-weather-checkpoint && \
    python3 -m pip install --no-cache-dir pymongo==4.10.1

WORKDIR /app
COPY processor.py /app/processor.py

USER 185

# Kafka connector jars are downloaded from Maven Central on first start (needs internet)
# and cached in /tmp/spark-ivy (a named volume in compose.yaml).
CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[2]", \
     "--conf", "spark.jars.ivy=/tmp/spark-ivy", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7", \
     "/app/processor.py"]
