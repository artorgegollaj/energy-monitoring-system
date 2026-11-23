#!/usr/bin/env bash
# Checks that the local development environment is ready.
# Usage: ./scripts/check_env.sh   (run from anywhere; requires a running `docker compose up -d`)

set -u

# Prevent Git Bash (Windows) from rewriting container paths such as /opt/kafka/bin
export MSYS_NO_PATHCONV=1

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR" || exit 1

if [ ! -f .env ]; then
  echo "FAIL  .env not found (run: cp .env.example .env)"
  exit 1
fi

# Load variables from .env (simple KEY=VALUE lines)
set -a
# shellcheck disable=SC1091
. ./.env
set +a

FAILURES=0

ok()   { printf 'OK    %s\n' "$1"; }
fail() { printf 'FAIL  %s\n' "$1"; FAILURES=$((FAILURES + 1)); }

# 1. Health status of the three main services
for svc in postgres kafka redis; do
  cid="$(docker compose ps -q "$svc" 2>/dev/null)"
  if [ -z "$cid" ]; then
    fail "$svc: container is not running"
    continue
  fi
  status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}' "$cid" 2>/dev/null)"
  if [ "$status" = "healthy" ]; then
    ok "$svc: healthy"
  else
    fail "$svc: status=$status"
  fi
done

# 2. Kafka topics
topics="$(docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list 2>/dev/null | tr -d '\r')"
for t in meter-readings meter-readings-invalid; do
  if printf '%s\n' "$topics" | grep -qx "$t"; then
    ok "kafka topic: $t"
  else
    fail "kafka topic missing: $t"
  fi
done

partitions="$(docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic meter-readings 2>/dev/null \
  | tr -d '\r' | grep -o 'PartitionCount: *[0-9]*' | grep -o '[0-9]*$')"
if [ "${partitions:-0}" = "3" ]; then
  ok "kafka topic meter-readings: 3 partitions"
else
  fail "kafka topic meter-readings: expected 3 partitions, got '${partitions:-none}'"
fi

# 3. PostgreSQL: SELECT 1
pg_result="$(docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc 'SELECT 1' 2>/dev/null | tr -d '\r[:space:]')"
if [ "$pg_result" = "1" ]; then
  ok "postgres: SELECT 1"
else
  fail "postgres: SELECT 1 returned '${pg_result}'"
fi

# 4. Redis: PING (password passed via env var, not on the command line)
redis_result="$(docker compose exec -T -e REDISCLI_AUTH="$REDIS_PASSWORD" redis redis-cli ping 2>/dev/null | tr -d '\r[:space:]')"
if [ "$redis_result" = "PONG" ]; then
  ok "redis: PING -> PONG"
else
  fail "redis: PING returned '${redis_result}'"
fi

echo "----------------------------------------"
if [ "$FAILURES" -eq 0 ]; then
  echo "All checks passed."
  exit 0
else
  echo "$FAILURES check(s) failed."
  exit 1
fi
