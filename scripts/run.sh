#!/usr/bin/env bash
# Usage: docker compose up -d && ./scripts/run.sh [JobName]   (no arg lists jobs)
set -euo pipefail
./gradlew -q jar
exec java -cp build/libs/streamforge.jar com.streamforge.core.launcher.Launcher "$@"
