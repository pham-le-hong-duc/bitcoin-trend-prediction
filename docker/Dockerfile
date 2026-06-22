FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY docker/requirements.txt .

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip && \
    pip install -r requirements.txt && \
    python -m nltk.downloader stopwords

COPY src/ ./src/
COPY dags/ ./dags/

# Keep one canonical copy of the code under /app while exposing the
# Airflow-friendly layout expected by existing compose files.
RUN mkdir -p /opt/airflow && \
    ln -s /app/src /opt/airflow/src && \
    ln -s /app/dags /opt/airflow/dags

CMD ["python", "-u"]
