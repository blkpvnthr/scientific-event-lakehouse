# Airflow image with the Spark/Delta runtime the pipeline tasks need.
FROM apache/airflow:2.10.5-python3.11

USER root
# Java 17 for PySpark.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PYTHONPATH=/opt/airflow

USER airflow
COPY requirements-spark.txt requirements-aws.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements-spark.txt -r /tmp/requirements-aws.txt
