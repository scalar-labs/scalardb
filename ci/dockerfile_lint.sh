#!/usr/bin/env bash

set -e -o pipefail; [[ -n "$DEBUG" ]] && set -x

HADOLINT_VERSION="v2.12.0"
HADOLINT="${HADOLINT:-docker run --rm -i -v "$(pwd):/mnt" -w "/mnt" "hadolint/hadolint:${HADOLINT_VERSION}" hadolint}"

exec ${HADOLINT} ./Dockerfile
