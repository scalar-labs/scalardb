#!/usr/bin/env bash

set -e -o pipefail; [[ -n "$DEBUG" ]] && set -x

SCRIPT_ROOT="$(cd "$(dirname "$0")"; pwd)"

if [[ -z "$HADOLINT" ]]; then
  # The hadolint image is pinned in ci/hadolint/Dockerfile so that Dependabot can
  # keep it up to date. That Dockerfile is never built; it is only a version pin.
  HADOLINT_IMAGE=$(awk '$1 == "FROM" { print $2; exit }' "${SCRIPT_ROOT}/hadolint/Dockerfile")
  if [[ -z "$HADOLINT_IMAGE" ]]; then
    echo "Failed to read the hadolint image from ci/hadolint/Dockerfile" >&2
    exit 1
  fi
  HADOLINT="docker run --rm -i -v $(pwd):/mnt -w /mnt ${HADOLINT_IMAGE} hadolint"
fi

exec ${HADOLINT} ./Dockerfile
