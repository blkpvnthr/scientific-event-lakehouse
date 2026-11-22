# Pipeline runtime image: Python 3.11 + Java 17 + Spark/Delta/AWS dependencies.
# Used to run the medallion CLIs (bronze/silver/delta/gold) in a container.
FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV SPARK_LOCAL_IP=127.0.0.1
ENV PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt requirements-spark.txt requirements-aws.txt ./
RUN pip install --no-cache-dir \
    -r requirements.txt -r requirements-spark.txt -r requirements-aws.txt

COPY src ./src
COPY sql ./sql

# Default: show the Bronze CLI help.
ENTRYPOINT ["python", "-m"]
CMD ["src.bronze.build_bronze", "--help"]
