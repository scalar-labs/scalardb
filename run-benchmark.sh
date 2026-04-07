#!/usr/bin/env bash
set -euo pipefail

TARGETS=(
  CBOR
  Fory
  Ion
  JSON
  Smile
  # CBOR+GZIP
  # Fory+GZIP
  # Ion+GZIP
  # JSON+GZIP
  # Smile+GZIP
  # CBOR+ZSTD
  # Fory+ZSTD
  # Ion+ZSTD
  # JSON+ZSTD
  # Smile+ZSTD
)

PASSED=()
FAILED=()

for target in "${TARGETS[@]}"; do
  echo "========================================"
  echo " Running: $target"
  echo "========================================"
  if ./gradlew :core:benchmarkTest -Dbenchmark.targets="$target" --no-daemon 2>&1; then
    PASSED+=("$target")
  else
    FAILED+=("$target")
    echo "*** $target FAILED ***"
  fi
  echo
done

echo "========================================"
echo " Summary"
echo "========================================"
echo "Passed (${#PASSED[@]}/${#TARGETS[@]}): ${PASSED[*]}"
if [ ${#FAILED[@]} -gt 0 ]; then
  echo "Failed (${#FAILED[@]}/${#TARGETS[@]}): ${FAILED[*]}"
  exit 1
fi
