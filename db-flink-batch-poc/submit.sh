#!/usr/bin/env bash
# Submits the fat JAR to the running Flink cluster.
# Requires: docker compose up (postgres + flink-jobmanager + flink-taskmanager) already running.
# Requires: ./build.sh has been run first.
#
# Usage:
#   ./submit.sh --from 2024-01-01 --to 2024-01-31
set -euo pipefail
cd "$(dirname "$0")"

FROM=""
TO=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --from) FROM="$2"; shift 2 ;;
    --to)   TO="$2";   shift 2 ;;
    *) echo "Unknown argument: $1" >&2; echo "Usage: ./submit.sh --from yyyy-MM-dd --to yyyy-MM-dd" >&2; exit 1 ;;
  esac
done

if [[ -z "$FROM" || -z "$TO" ]]; then
  echo "Usage: ./submit.sh --from yyyy-MM-dd --to yyyy-MM-dd" >&2
  exit 1
fi

JAR="$(pwd)/flink-job/target/flink-job-1.0.jar"
if [[ ! -f "$JAR" ]]; then
  echo "JAR not found: $JAR" >&2
  echo "Run ./build.sh first." >&2
  exit 1
fi

METRICS_DIR="$(pwd)/flink-job/target"
METRICS_FILE="$METRICS_DIR/job-metrics.json"
rm -f "$METRICS_FILE"

echo "Submitting batch job  from=$FROM  to=$TO"
docker run --rm \
  --network db-flink-batch-poc_default \
  -v "$JAR":/job.jar \
  -v "$METRICS_DIR":/metrics \
  -e METRICS_FILE=/metrics/job-metrics.json \
  -e JUICEFS_BUCKET=sales-csv \
  -e SOURCE_DB_URL=jdbc:postgresql://postgres-source:5432/salesdb \
  -e SOURCE_DB_USER=poc \
  -e SOURCE_DB_PASS=poc123 \
  -e SINK_DB_URL=jdbc:postgresql://postgres-sink:5432/salesdb \
  -e SINK_DB_USER=poc \
  -e SINK_DB_PASS=poc123 \
  flink:2.0-java17 \
  flink run -m flink-jobmanager:8081 -c com.poc.BatchJob /job.jar \
    --from "$FROM" --to "$TO"

RUN_ID="$(date +%s)000"
echo "Recording lineage (run=$RUN_ID) ..."
docker run --rm \
  --network db-flink-batch-poc_default \
  -v "$JAR":/job.jar \
  -v "$METRICS_FILE":/metrics/job-metrics.json:ro \
  -e ATLAS_URL=http://atlas:21000 \
  -e ATLAS_USER=admin \
  -e ATLAS_PASS=admin \
  flink:2.0-java17 \
  java -cp /job.jar com.poc.LineageReporter \
    --from "$FROM" --to "$TO" --run-id "$RUN_ID" --metrics-file /metrics/job-metrics.json

echo "Done."
