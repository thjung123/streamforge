#!/usr/bin/env bash

set -e

JOB_ID=$1
SAVEPOINT_DIR=${2:-"/tmp/savepoints"}

if [ -z "$JOB_ID" ]; then
  echo "Usage: $0 <job-id> [savepoint-dir]"
  exit 1
fi

echo "[SAVEPOINT] Triggering savepoint for job: $JOB_ID"
flink savepoint "$JOB_ID" "$SAVEPOINT_DIR"

echo "[SAVEPOINT] Savepoint created successfully in $SAVEPOINT_DIR"
