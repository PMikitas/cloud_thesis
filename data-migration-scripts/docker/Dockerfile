FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    SUPERCRONIC_VERSION=v0.2.29 \
    PYTHONPATH=/app/src

WORKDIR /app

# Build deps for cryptography / pymysql + curl for fetching supercronic.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential curl ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

# supercronic = drop-in cron that inherits env vars and logs to stdout.
RUN curl -fsSLo /usr/local/bin/supercronic \
        "https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/supercronic-linux-amd64" \
    && chmod +x /usr/local/bin/supercronic

# Install deps from pyproject.toml. Using pip (PEP 621 via poetry-core build
# backend) avoids needing to keep poetry.lock in sync at image build time.
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .

COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Default: cron mode. Override CMD to run scheduler.py instead (see compose).
ENTRYPOINT ["/entrypoint.sh"]
CMD ["cron"]
