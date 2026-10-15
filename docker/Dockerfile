FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PYTHONPATH=/app/src
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

COPY requirements.txt requirements-spark.txt ./
RUN pip install --no-cache-dir -r requirements-spark.txt

COPY pyproject.toml README.md LICENSE ./
COPY src ./src
COPY dags ./dags
COPY sql ./sql
COPY config ./config
COPY scripts ./scripts

RUN pip install --no-cache-dir -e .

RUN mkdir -p /app/data/raw /app/data/warehouse /app/data/checkpoints

CMD ["python", "-m", "pipeline.producer.simulator", "--count", "50", "--rate", "25", "--bootstrap", "file"]
