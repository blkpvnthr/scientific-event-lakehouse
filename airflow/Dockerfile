# Airflow image with the Spark/Delta runtime the pipeline tasks need.
FROM apache/airflow:2.10.5-python3.11

USER root
# Java 17 for PySpark.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    # Architecture-independent JAVA_HOME (works on arm64 and amd64).
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java

ENV JAVA_HOME=/opt/java
ENV PYTHONPATH=/opt/airflow

USER airflow
COPY requirements-spark.txt requirements-aws.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements-spark.txt -r /tmp/requirements-aws.txt
