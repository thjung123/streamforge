#!/usr/bin/env bash
# Restore a Flink job from a given savepoint

set -e

JAR_PATH=$1
SAVEPOINT_PATH=$2
JOB_CLASS=$3

if [ -z "$JAR_PATH" ] || [ -z "$SAVEPOINT_PATH" ]; then
  echo "Usage: $0 <job-jar-path> <savepoint-path> [main-class]"
  exit 1
fi

echo "[RESTORE] Starting Flink job from savepoint..."
if [ -z "$JOB_CLASS" ]; then
  flink run -s "$SAVEPOINT_PATH" "$JAR_PATH"
else
  flink run -s "$SAVEPOINT_PATH" -c "$JOB_CLASS" "$JAR_PATH"
fi

echo "[RESTORE] Job restored successfully from $SAVEPOINT_PATH"
