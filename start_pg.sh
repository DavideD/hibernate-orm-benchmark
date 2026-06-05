#!/bin/bash

# Start a perf-tuned PostgreSQL container for benchmarking.
# Based on https://github.com/quarkusio/spring-quarkus-perf-comparison/blob/main/scripts/infra.sh

CONTAINER_NAME="postgres-bench"
PG_USER="hibernate_orm_test"
PG_PASS="hibernate_orm_test"
PG_DB="hibernate_orm_test"
CPUS=3
USE_HOST_NETWORKING=false

usage() {
  echo "Usage: $0 [-n] [start|stop]"
  echo
  echo "  -n      Use host networking instead of port mapping (Linux only)"
  echo "  start   Start the PostgreSQL container (default)"
  echo "  stop    Stop and remove the container"
}

while getopts "nh" opt; do
  case $opt in
    n) USE_HOST_NETWORKING=true ;;
    h) usage; exit 0 ;;
    *) usage; exit 1 ;;
  esac
done
shift $((OPTIND - 1))

ACTION=${1:-start}

if [[ "$ACTION" == "stop" ]]; then
  podman stop "$CONTAINER_NAME" 2>/dev/null
  echo "Stopped $CONTAINER_NAME"
  exit 0
fi

if [ "${USE_HOST_NETWORKING}" = "true" ]; then
  networking_flags="--network host"
else
  networking_flags="-p 5432:5432"
fi

podman stop "$CONTAINER_NAME" 2>/dev/null

podman run --rm -d \
  --name "$CONTAINER_NAME" \
  ${networking_flags} \
  --cpus "$CPUS" \
  -e POSTGRES_USER="$PG_USER" \
  -e POSTGRES_PASSWORD="$PG_PASS" \
  -e POSTGRES_DB="$PG_DB" \
  docker.io/library/postgres:18 \
  -c fsync=off \
  -c synchronous_commit=off \
  -c autovacuum=off \
  -c full_page_writes=off \
  -c wal_level=minimal \
  -c archive_mode=off \
  -c max_wal_senders=0 \
  -c max_wal_size=4GB \
  -c track_counts=off \
  -c checkpoint_timeout=1h \
  -c work_mem=32MB \
  -c maintenance_work_mem=256MB

echo "Waiting for PostgreSQL to be ready..."
until podman exec "$CONTAINER_NAME" pg_isready -U "$PG_USER" -q 2>/dev/null; do
  sleep 0.5
done
echo "PostgreSQL is ready on localhost:5432 (cpus=$CPUS, networking=${networking_flags})"
