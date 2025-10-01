#!/usr/bin/env bash
# Replay DLQ messages back into the source topic for reprocessing

set -e

DLQ_TOPIC=${1:-"cdc-dlq"}
SOURCE_TOPIC=${2:-"cdc-topic"}
BOOTSTRAP=${3:-"localhost:9092"}

echo "[DLQ] Replaying messages from $DLQ_TOPIC -> $SOURCE_TOPIC"

kafka-console-consumer --bootstrap-server "$BOOTSTRAP" \
  --topic "$DLQ_TOPIC" --from-beginning \
  | kafka-console-producer --bootstrap-server "$BOOTSTRAP" \
  --topic "$SOURCE_TOPIC"

echo "[DLQ] Replay complete."
