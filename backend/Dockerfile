# Cross-platform image: works on Mac (arm64/amd64), Linux, and Windows via Docker Desktop.
# GPU is detected at runtime by PyTorch; for NVIDIA GPU use docker-compose.gpu.yaml override.
FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    default-jre-headless \
 && rm -rf /var/lib/apt/lists/* \
 && java_bin=$(readlink -f /usr/bin/java) \
 && ln -sf "$(dirname "$(dirname "$java_bin")")" /opt/java-home

# Stable JAVA_HOME symlink works on both amd64 and arm64
ENV JAVA_HOME=/opt/java-home

RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Install CPU-only PyTorch first (works on all platforms; no CUDA required).
# The CUDA build is only needed when running with an NVIDIA GPU — see docker-compose.gpu.yaml.
RUN pip install --no-cache-dir \
    torch==2.5.1 \
    --index-url https://download.pytorch.org/whl/cpu

COPY backend/requirements.txt /tmp/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r /tmp/requirements.txt

# Pre-download Spark packages so harness spark-submit needs no network at runtime
RUN echo "" > /tmp/dummy.py && \
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
        --conf spark.jars.ivy=/opt/spark-jars \
        /tmp/dummy.py || true

COPY backend/ ./backend/

EXPOSE 8000

CMD ["uvicorn", "backend.app:app", "--host", "0.0.0.0", "--port", "8000"]
