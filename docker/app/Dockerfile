FROM python:3.12-slim-bookworm

WORKDIR /app

ENV PYTHONPATH=/app
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps && \
    rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./

RUN mkdir -p src/app && touch src/app/__init__.py

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -e .

COPY . .

RUN pip install --no-deps -e .
