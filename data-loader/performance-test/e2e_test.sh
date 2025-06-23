#!/bin/bash

set -e

# === FUNCTIONS ===

info()    { echo -e "\033[1;34m▶ $1\033[0m"; }
success() { echo -e "\033[1;32m✅ $1\033[0m"; }
warn()    { echo -e "\033[1;33m⚠️ $1\033[0m"; }
error()   { echo -e "\033[1;31m❌ $1\033[0m"; }

# Timestamp and elapsed time functions
timestamp() {
  date +"%Y-%m-%d %H:%M:%S"
}

start_timer() {
  TIMER_START=$(date +%s)
  echo -e "\033[1;36m[$(timestamp)] Starting: $1\033[0m"
}

end_timer() {
  local end_time=$(date +%s)
  local elapsed=$((end_time - TIMER_START))
  local hours=$((elapsed / 3600))
  local minutes=$(( (elapsed % 3600) / 60 ))
  local seconds=$((elapsed % 60))

  if [[ $hours -gt 0 ]]; then
    echo -e "\033[1;36m[$(timestamp)] Completed: $1 (Elapsed time: ${hours}h ${minutes}m ${seconds}s)\033[0m"
  elif [[ $minutes -gt 0 ]]; then
    echo -e "\033[1;36m[$(timestamp)] Completed: $1 (Elapsed time: ${minutes}m ${seconds}s)\033[0m"
  else
    echo -e "\033[1;36m[$(timestamp)] Completed: $1 (Elapsed time: ${seconds}s)\033[0m"
  fi
}

cleanup() {
  info "Cleaning up database container..."
  docker kill "$DATABASE_CONTAINER" &>/dev/null || true
  docker rm -v "$DATABASE_CONTAINER" &>/dev/null || true
  success "Removed database container"
}

setup_database() {
  info "Starting database Docker container..."
  start_timer "Setting up database and load schema"
  pushd "$DATABASE_ROOT_PATH" >/dev/null
  bash "$DATABASE_SETUP_SCRIPT"
  popd >/dev/null
  end_timer "Setting up database and load schema"
  success "Database container is running"
}

ensure_docker_network() {
  if ! docker network inspect $DOCKER_NETWORK >/dev/null 2>&1; then
    info "Creating Docker network '$DOCKER_NETWORK'"
    docker network create $DOCKER_NETWORK
  fi
}

run_import_container() {
  local mem=$1
  local cpu=$2
  local container_name="import-mem${mem}-cpu${cpu}"

  info "Running Import Docker container: $container_name with --memory=$mem --cpus=$cpu"
  start_timer "Import container $container_name"

  docker run --rm \
    --name "$container_name" \
    --network $DOCKER_NETWORK \
    --memory="$mem" \
    --cpus="$cpu" \
    -v "$PWD/$INPUT_SOURCE_FILE":"$CONTAINER_INPUT_FILE" \
    -v "$PWD/$PROPERTIES_PATH":"$CONTAINER_PROPERTIES_PATH" \
    -v "$PWD/$LOG_DIR_HOST":"$CONTAINER_LOG_DIR" \
    "$IMAGE_NAME" \
    import --config "$CONTAINER_PROPERTIES_PATH" \
           --file "$CONTAINER_INPUT_FILE" \
           --namespace test \
           --table all_columns \
           $IMPORT_ARGS \
           --log-dir "$CONTAINER_LOG_DIR_PATH"

  end_timer "Import container $container_name"
  success "Finished Import: $container_name"
  echo "----------------------------------------"
}

run_export_container() {
  local mem=$1
  local cpu=$2
  local container_name="export-mem${mem}-cpu${cpu}"

  mkdir -p dumps

  info "Running Export Docker container: $container_name with --memory=$mem --cpus=$cpu"
  start_timer "Export container $container_name"

  docker run --rm \
    --name "$container_name" \
    --network $DOCKER_NETWORK \
    --memory="$mem" \
    --cpus="$cpu" \
    -v "$PWD/$PROPERTIES_PATH":"$CONTAINER_PROPERTIES_PATH" \
    -v "$PWD/$OUTPUT_DIR_HOST":"$CONTAINER_OUTPUT_DIR" \
    -v "$PWD/dumps":/tmp \
    --entrypoint java \
    "$IMAGE_NAME" \
    -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/heapdump.hprof -XX:+UseContainerSupport -XX:MaxRAMPercentage=80.0 -jar /app.jar \
    export --config "$CONTAINER_PROPERTIES_PATH" \
           --namespace test \
           --table all_columns \
           --output-dir "$CONTAINER_OUTPUT_DIR_PATH" \
           $EXPORT_ARGS

  end_timer "Export container $container_name"
  success "Finished Export: $container_name"
  echo "----------------------------------------"
}

run_import_jar() {
  info "Running Import using JAR file"
  start_timer "Import using JAR"

  java -jar "$JAR_PATH" \
    import --config "$PROPERTIES_PATH" \
           --file "$INPUT_SOURCE_FILE" \
           --namespace test \
           --table all_columns \
           $IMPORT_ARGS \
           --log-dir "$LOG_DIR_HOST"

  end_timer "Import using JAR"
  success "Finished Import using JAR"
  echo "----------------------------------------"
}

run_export_jar() {
  info "Running Export using JAR file"
  start_timer "Export using JAR"

  java -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/heapdump.hprof -XX:MaxRAMPercentage=80.0 -jar "$JAR_PATH" \
    export --config "$PROPERTIES_PATH" \
           --namespace test \
           --table all_columns \
           --output-dir "$OUTPUT_DIR_HOST" \
           $EXPORT_ARGS

  end_timer "Export using JAR"
  success "Finished Export using JAR"
  echo "----------------------------------------"
}

# === PREREQUISITE CHECK ===

command -v docker >/dev/null 2>&1 || { error "Docker is not installed or not in PATH"; exit 1; }
# Java is checked when --use-jar is specified in the argument parsing section

# === CONFIGURATION VARIABLES ===

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE_BASE="ghcr.io/scalar-labs/scalardb-data-loader-cli"
IMAGE_TAG="4.0.0-SNAPSHOT"
IMAGE_NAME="${IMAGE_BASE}:${IMAGE_TAG}"
DATABASE_DIR="database"
DATABASE_ROOT_PATH="$SCRIPT_DIR/$DATABASE_DIR"
DATABASE_SETUP_SCRIPT="$DATABASE_ROOT_PATH/db_setup.sh"
DATABASE_CONTAINER="postgres-db-1"
SCHEMA_PATH="$DATABASE_ROOT_PATH/schema.json"
PROPERTIES_PATH="$DATABASE_DIR/scalardb.properties"
PYTHON_SCRIPT_PATH="scripts/import-data-generator.py"
DATA_SIZE=""
NUM_ROWS=""
PYTHON_ARGUMENTS=""
INPUT_SOURCE_FILE="./generated-imports.csv"
CONTAINER_INPUT_FILE="/app/generated-imports.csv"
CONTAINER_PROPERTIES_PATH="/app/scalardb.properties"
LOG_DIR_HOST="import-logs"
CONTAINER_LOG_DIR="/app/logs"
CONTAINER_LOG_DIR_PATH="/app/logs/"
OUTPUT_DIR_HOST="export-output"
CONTAINER_OUTPUT_DIR="/app/export-output"
CONTAINER_OUTPUT_DIR_PATH="/app/export-output/"
DOCKER_NETWORK="my-network"
JAR_PATH="./scalardb-data-loader-cli-4.0.0-SNAPSHOT.jar"

# Feature flags
SKIP_DATA_GEN=false
DISABLE_IMPORT=false
DISABLE_EXPORT=false
NO_CLEAN_DATA=false
USE_JAR=false

MEMORY_CONFIGS=("2g")
CPU_CONFIGS=("2")
IMPORT_ARGS="--format csv --import-mode insert --mode transaction --transaction-size 10 --data-chunk-size 500 --max-threads 8"
EXPORT_ARGS="--format csv --max-threads 8 --data-chunk-size 500"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --memory=*)
      IFS=',' read -r -a MEMORY_CONFIGS <<< "${1#*=}"
      shift
      ;;
    --cpu=*)
      IFS=',' read -r -a CPU_CONFIGS <<< "${1#*=}"
      shift
      ;;
    --data-size=*)
      if [[ -n "$NUM_ROWS" ]]; then
        error "Cannot specify both --data-size and --num-rows"
        echo "Usage: $0 [--memory=mem1,mem2,...] [--cpu=cpu1,cpu2,...] [--data-size=size] [--num-rows=number] [--image-tag=tag] [--import-args=args] [--export-args=args] [--network=network-name] [--skip-data-gen] [--disable-import] [--disable-export] [--no-clean-data] [--database-dir=path] [--use-jar] [--jar-path=path]"
        echo "Note: Either --data-size or --num-rows must be provided, but not both."
        exit 1
      fi
      DATA_SIZE=${1#*=}
      PYTHON_ARGUMENTS="-s $DATA_SIZE -o /app/generated-imports.csv /app/$DATABASE_DIR/schema.json test.all_columns"
      shift
      ;;
    --num-rows=*)
      if [[ -n "$DATA_SIZE" ]]; then
        error "Cannot specify both --data-size and --num-rows"
        echo "Usage: $0 [--memory=mem1,mem2,...] [--cpu=cpu1,cpu2,...] [--data-size=size] [--num-rows=number] [--image-tag=tag] [--import-args=args] [--export-args=args] [--network=network-name] [--skip-data-gen] [--disable-import] [--disable-export] [--no-clean-data] [--database-dir=path] [--use-jar] [--jar-path=path]"
        echo "Note: Either --data-size or --num-rows must be provided, but not both."
        exit 1
      fi
      NUM_ROWS=${1#*=}
      PYTHON_ARGUMENTS="-n $NUM_ROWS -o /app/generated-imports.csv /app/$DATABASE_DIR/schema.json test.all_columns"
      shift
      ;;
    --image-tag=*)
      IMAGE_TAG=${1#*=}
      IMAGE_NAME="${IMAGE_BASE}:${IMAGE_TAG}"
      shift
      ;;
    --import-args=*)
      IMPORT_ARGS=${1#*=}
      shift
      ;;
    --export-args=*)
      EXPORT_ARGS=${1#*=}
      shift
      ;;
    --network=*)
      DOCKER_NETWORK=${1#*=}
      shift
      ;;
    --skip-data-gen)
      SKIP_DATA_GEN=true
      shift
      ;;
    --disable-import)
      DISABLE_IMPORT=true
      shift
      ;;
    --disable-export)
      DISABLE_EXPORT=true
      shift
      ;;
    --no-clean-data)
      NO_CLEAN_DATA=true
      shift
      ;;
    --database-dir=*)
      DATABASE_DIR=${1#*=}
      DATABASE_ROOT_PATH="$SCRIPT_DIR/$DATABASE_DIR"
      DATABASE_SETUP_SCRIPT="$DATABASE_ROOT_PATH/db_setup.sh"
      SCHEMA_PATH="$DATABASE_ROOT_PATH/schema.json"
      PROPERTIES_PATH="$DATABASE_DIR/scalardb.properties"
      # Update Python arguments based on whether data-size or num-rows is specified
      if [[ -n "$DATA_SIZE" ]]; then
        PYTHON_ARGUMENTS="-s $DATA_SIZE -o /app/generated-imports.csv /app/$DATABASE_DIR/schema.json test.all_columns"
      elif [[ -n "$NUM_ROWS" ]]; then
        PYTHON_ARGUMENTS="-n $NUM_ROWS -o /app/generated-imports.csv /app/$DATABASE_DIR/schema.json test.all_columns"
      fi
      shift
      ;;
    --use-jar)
      USE_JAR=true
      command -v java >/dev/null 2>&1 || { error "Java is not installed or not in PATH"; exit 1; }
      shift
      ;;
    --jar-path=*)
      JAR_PATH=${1#*=}
      shift
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--memory=mem1,mem2,...] [--cpu=cpu1,cpu2,...] [--data-size=size] [--num-rows=number] [--image-tag=tag] [--import-args=args] [--export-args=args] [--network=network-name] [--skip-data-gen] [--disable-import] [--disable-export] [--no-clean-data] [--database-dir=path] [--use-jar] [--jar-path=path]"
      echo "Note: Either --data-size or --num-rows must be provided, but not both."
      echo "Example: $0 --memory=1g,2g,4g --cpu=1,2,4 --data-size=2MB --image-tag=4.0.0-SNAPSHOT --import-args=\"--format csv --import-mode insert --mode transaction --transaction-size 10 --data-chunk-size 50 --max-threads 16\" --export-args=\"--format csv --max-threads 8 --data-chunk-size 10\" --network=my-custom-network"
      echo "Example with num-rows: $0 --memory=1g,2g,4g --cpu=1,2,4 --num-rows=1000 --image-tag=4.0.0-SNAPSHOT"
      echo "Example with JAR: $0 --use-jar --jar-path=./scalardb-data-loader-cli.jar --import-args=\"--format csv --import-mode insert --mode transaction --transaction-size 10 --data-chunk-size 50 --max-threads 16\" --export-args=\"--format csv --max-threads 8 --data-chunk-size 10\""
      exit 1
      ;;
  esac
done

trap cleanup EXIT

# Set default if neither is provided
if [[ -z "$DATA_SIZE" && -z "$NUM_ROWS" ]]; then
  DATA_SIZE="1MB"
  PYTHON_ARGUMENTS="-s $DATA_SIZE -o /app/generated-imports.csv /app/$DATABASE_DIR/schema.json test.all_columns"
  info "No data size or row count specified, using default: $DATA_SIZE"
fi

# === SCRIPT START ===
start_timer "End-to-end test script"

# === SETUP DIRECTORIES ===
start_timer "Setup directories"
mkdir -p "$LOG_DIR_HOST"
chmod 777 "$LOG_DIR_HOST"

mkdir -p "$OUTPUT_DIR_HOST"
chmod 777 "$OUTPUT_DIR_HOST"
end_timer "Setup directories"

# === GENERATE IMPORT DATA ===
if [[ "$SKIP_DATA_GEN" != true ]]; then
  if [[ ! -f "$PYTHON_SCRIPT_PATH" ]]; then
    error "Python script not found at $PYTHON_SCRIPT_PATH"
    exit 1
  fi

  info "Running Python script to generate input data using Docker..."
  start_timer "Generate import data"
  docker run --rm \
    -v "$PWD":/app \
    python:alpine \
    python3 /app/"$PYTHON_SCRIPT_PATH" $PYTHON_ARGUMENTS
  end_timer "Generate import data"
  success "Input file generated."
else
  info "Skipping data generation as requested"
fi

# === ENSURE DOCKER NETWORK ===
start_timer "Ensure Docker network"
ensure_docker_network
end_timer "Ensure Docker network"

# === SETTING UP DATABASE AND LOAD SCHEMA ===
setup_database

# === RUN IMPORT AND EXPORT TESTS ===
info "Running tests..."
start_timer "All tests"

if [[ "$USE_JAR" == true ]]; then
  # Check if JAR file exists
  if [[ ! -f "$JAR_PATH" ]]; then
    error "JAR file not found at $JAR_PATH"
    exit 1
  fi

  info "Using JAR file for tests: $JAR_PATH"

  # Run import and export using JAR
  if [[ "$DISABLE_IMPORT" != true ]]; then
    run_import_jar
  else
    info "Skipping import test as requested"
  fi

  if [[ "$DISABLE_EXPORT" != true ]]; then
    run_export_jar
  else
    info "Skipping export test as requested"
  fi
else
  # Run tests using Docker containers with different memory and CPU configurations
  first_iteration=true
  for mem in "${MEMORY_CONFIGS[@]}"; do
    for cpu in "${CPU_CONFIGS[@]}"; do
      # Clean up and restart the database for each test iteration, except the first one
      if [ "$first_iteration" = true ]; then
        first_iteration=false
      else
        cleanup
        setup_database
      fi

      if [[ "$DISABLE_IMPORT" != true ]]; then
        run_import_container "$mem" "$cpu"
      else
        info "Skipping import test as requested"
      fi

      if [[ "$DISABLE_EXPORT" != true ]]; then
        run_export_container "$mem" "$cpu"
      else
        info "Skipping export test as requested"
      fi
    done
  done
fi
end_timer "All tests"

# === CLEANUP ===
if [[ "$NO_CLEAN_DATA" != true ]]; then
  info "Cleaning up generated files..."
  start_timer "Cleanup generated files"
  rm -rf "$LOG_DIR_HOST" "$INPUT_SOURCE_FILE" "$OUTPUT_DIR_HOST"
  end_timer "Cleanup generated files"
else
  info "Skipping cleanup of generated files as requested"
fi

end_timer "End-to-end test script"
success "End-to-end test completed successfully 🎉"
