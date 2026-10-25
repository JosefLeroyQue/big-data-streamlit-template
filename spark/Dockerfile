FROM apache/spark:3.5.7

USER root

RUN mkdir -p /tmp/spark-ivy /tmp/spark-work && \
    chmod -R 777 /tmp/spark-ivy /tmp/spark-work

RUN pip install --no-cache-dir pymongo

WORKDIR /app

COPY processor.py /app/processor.py

USER 185

CMD ["/opt/spark/bin/spark-submit", \
     "--conf", "spark.jars.ivy=/tmp/spark-ivy", \
     "--packages", \
     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7", \
     "/app/processor.py"]
