#!/usr/bin/env bash

set -euo pipefail

export SCALARDB_GRAPHQL_SERVER_OPTS="${SCALARDB_GRAPHQL_SERVER_OPTS} -XX:MaxRAMPercentage=${JAVA_OPT_MAX_RAM_PERCENTAGE}"

exec "$@"
