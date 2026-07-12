# syntax=docker/dockerfile:1
# ---------------------------------------------------------------------------
# Stage 1: Python dependency builder
# ---------------------------------------------------------------------------
FROM python:3.11-slim-bookworm AS builder

WORKDIR /build

# Install build tools needed by some wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --prefix=/install --no-cache-dir -r requirements.txt

# ---------------------------------------------------------------------------
# Stage 2: Java runtime for PySpark
# ---------------------------------------------------------------------------
FROM python:3.11-slim-bookworm AS runtime

# Java 17 (headless) for PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Copy installed Python packages from builder
COPY --from=builder /install /usr/local

WORKDIR /app

# Copy project source
COPY . .

# MinIO / S3 connection — overridden at runtime via env vars or docker-compose
ENV MINIO_ENDPOINT=http://minio:9000
ENV MINIO_ROOT_USER=minioadmin
ENV MINIO_ROOT_PASSWORD=minioadmin
ENV MINIO_BUCKET_RAW=raw-data
ENV MINIO_BUCKET_OUTPUT=processed-data

# Pyspark needs JAVA_HOME exposed; also set PYTHONPATH so modules resolve
ENV PYTHONPATH=/app
ENV PYSPARK_PYTHON=python3

ENTRYPOINT ["python"]
CMD ["-m", "benchmark.runner", "--engines", "spark,dask,ray"]
