#!/usr/bin/env bash

set -euo pipefail

export JAVA_OPTS="${JAVA_OPTS} -XX:MaxRAMPercentage=${JAVA_OPT_MAX_RAM_PERCENTAGE}"

exec "$@"
