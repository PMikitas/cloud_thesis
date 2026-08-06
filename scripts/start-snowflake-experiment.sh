#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

GATLING_ACTIVITY_USERS="${GATLING_ACTIVITY_USERS:-1200}"
GATLING_SESSION_DURATION_SECONDS="${GATLING_SESSION_DURATION_SECONDS:-21600}"
GATLING_RAMP_SECONDS="${GATLING_RAMP_SECONDS:-300}"
SHOPIZER_NETWORK_NAME="${SHOPIZER_NETWORK_NAME:-cloud_thesis_default}"

./scripts/preflight-snowflake-experiment.sh

mkdir -p logs

docker rm -f gatling-experiment metrics-migrations metrics-immutable snowflake-experiment-watchdog 2>/dev/null || true

SHOPIZER_NETWORK_NAME="$SHOPIZER_NETWORK_NAME" \
docker compose -f data-migration-scripts/docker-compose.yml up -d --force-recreate migration-cron

SHOPIZER_NETWORK_NAME="$SHOPIZER_NETWORK_NAME" \
docker compose --env-file metrics-scheduler/.env.migrations \
  -f metrics-scheduler/docker-compose.yml run -d \
  --name metrics-migrations \
  metrics-cron cron

SHOPIZER_NETWORK_NAME="$SHOPIZER_NETWORK_NAME" \
docker compose --env-file metrics-scheduler/.env.immutable_ops \
  -f metrics-scheduler/docker-compose.yml run -d \
  --name metrics-immutable \
  metrics-cron cron

GATLING_ACTIVITY_USERS="$GATLING_ACTIVITY_USERS" \
GATLING_SESSION_DURATION_SECONDS="$GATLING_SESSION_DURATION_SECONDS" \
GATLING_RAMP_SECONDS="$GATLING_RAMP_SECONDS" \
docker compose --profile loadtest run -d --name gatling-experiment --no-deps gatling

nohup ./scripts/watch-snowflake-experiment.sh > logs/snowflake-experiment-watchdog.log 2>&1 &
echo "$!" > logs/snowflake-experiment-watchdog.pid

echo "Experiment started."
echo "Gatling: docker logs -f gatling-experiment"
echo "Watchdog: tail -f logs/snowflake-experiment-watchdog.log"
