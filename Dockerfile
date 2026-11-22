# Pipeline runtime image: Python 3.11 + Java 17 + Spark/Delta/AWS dependencies.
# Used to run the medallion CLIs (bronze/silver/delta/gold) in a container.
# Pinned to Debian 12 (bookworm): it ships openjdk-17 (trixie/slim dropped it).
FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    # Architecture-independent JAVA_HOME (works on arm64 and amd64).
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java

ENV JAVA_HOME=/opt/java
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
