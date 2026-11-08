#!/usr/bin/env bash
# Brings the whole local stack up (or down) with a single command.
#
#   scripts/stack.sh [up]   build the service jars and start everything, waiting until healthy
#   scripts/stack.sh down   stop the containers, keeping Kafka and ClickHouse data
#   scripts/stack.sh reset  stop the containers and delete all data
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$REPO_ROOT/infra/compose.yaml"

usage() {
  echo "usage: $(basename "$0") [up|down|reset]" >&2
}

require_docker() {
  if ! command -v docker > /dev/null 2>&1 || ! docker compose version > /dev/null 2>&1; then
    echo "docker with the Compose v2 plugin is required" >&2
    exit 1
  fi
}

compose() {
  docker compose -f "$COMPOSE_FILE" "$@"
}

up() {
  # Always rebuild jars and images so a code change is never silently ignored.
  (cd "$REPO_ROOT" && ./gradlew bootJar)
  compose up -d --build --wait
  cat <<EOF

Stack is up:
  click service     POST http://localhost:8080/click
  Kafka             localhost:9092 (topic click-events)
  ClickHouse HTTP   http://localhost:8123 (user ad_clicks, password ad_clicks, database ad_clicks)
EOF
}

command="${1:-up}"
case "$command" in
  up | down | reset) ;;
  *) usage; exit 2 ;;
esac

require_docker
case "$command" in
  up) up ;;
  down) compose down ;;
  reset) compose down -v ;;
esac
