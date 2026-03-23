#!/bin/sh
set -e

echo "Formatting JuiceFS volume (idempotent)..."
juicefs format \
  --storage none \
  "${JFS_METASTORE_URL}" \
  "${JFS_BUCKET}" 2>&1 || true

echo "Starting JuiceFS S3 gateway on 0.0.0.0:9000..."
exec juicefs gateway \
  "${JFS_METASTORE_URL}" \
  "0.0.0.0:9000" \
  --access-key "${JFS_ACCESS_KEY}" \
  --secret-key "${JFS_SECRET_KEY}"
