#!/usr/bin/env bash

set -euo pipefail

export SCALARDB_SERVER_OPTS="${SCALARDB_SERVER_OPTS} -XX:MaxRAMPercentage=${JAVA_OPT_MAX_RAM_PERCENTAGE}"

exec "$@"
