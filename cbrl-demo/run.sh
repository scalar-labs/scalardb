#!/usr/bin/env bash
#
# CBRL end-to-end demo orchestrator — runs ENTIRELY through docker-compose.
#
# Proves the CBRL restore story against the LOCAL CBRL-enabled ScalarDB build: run a
# conserved-total cross-storage balance-transfer workload against a multi-storage
# ScalarDB (user data split across MySQL + PostgreSQL, coordinator in PostgreSQL), open
# a backup window so in-window commits are redo-logged, take non-snapshot-consistent
# physical backups of both databases WHILE writes are in flight, restore them into
# separate copy servers, run CbrlRestore over the coordinator redo to make the copy
# transactionally consistent, then verify the grand total balance is conserved on the
# RESTORED copy.
#
# Everything is one isolated compose project (name: cbrl-demo): a dedicated network and
# named volumes, nothing published to the host. The app steps are one-shot compose jobs.
#
# Usage:
#   bash run.sh              # build image + run the whole pipeline
#   bash run.sh <step>       # single step: build|up|schema|populate|open|workload|restore-load|
#                            #              check-raw|expire|cbrl-restore|check|down
#   bash run.sh resume       # workload + backup + restore + check on an already-seeded primary
#
# The demo is a correctness test, not just a smoke test: after loading the mid-flight physical
# backups into the copy servers (restore-load), the `check-raw` negative control asserts the raw
# copy is BROKEN (drifted total and/or in-flight records) — proving CBRL is actually needed — then
# `cbrl-restore` replays the coordinator redo and the final `check` asserts the copy is now complete,
# all-COMMITTED, and conserved. The demo thus shows: raw copy = BROKEN -> CBRL replay -> FIXED.
#
# Requires: Docker Desktop running. Run unsandboxed (docker needs host access).

set -euo pipefail

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${DEMO_DIR}/.." && pwd)"
COMPOSE_FILE="${DEMO_DIR}/docker-compose.yml"
COMPOSE=(docker compose -p cbrl-demo -f "${COMPOSE_FILE}")

# The single sanctioned host build step: the local schema-loader shadow (fat) jar,
# which bundles core + CBRL + JDBC drivers + the SchemaLoader entrypoint.
SHADOW_JAR="${REPO_ROOT}/schema-loader/build/libs/scalardb-schema-loader-4.0.0-SNAPSHOT.jar"
JAVA_HOME_GRADLE="${JAVA_HOME_GRADLE:-/Users/mitsunorikomatsu/Library/Java/JavaVirtualMachines/temurin-17.0.4.1/Contents/Home}"

# Properties baked into the app image (service hostnames on the compose network).
PROPS=/app/scalardb-multi-storage.properties
COPY_PROPS=/app/scalardb-multi-storage-copy.properties
SCHEMA=/app/schema.json

# Workload parameters.
NUM_ACCOUNTS="${NUM_ACCOUNTS:-100}"
INITIAL_BALANCE="${INITIAL_BALANCE:-10000}"
RUN_SECONDS="${RUN_SECONDS:-30}"
THREADS="${THREADS:-4}"
BACKUP_LABEL="${BACKUP_LABEL:-demo-window}"

# Timing.
CACHE_WAIT_SECONDS="${CACHE_WAIT_SECONDS:-8}"    # let BackupModeDaemon observe the window
WARMUP_SECONDS="${WARMUP_SECONDS:-8}"            # commit some transfers before dumping
BACKUP_GAP_SECONDS="${BACKUP_GAP_SECONDS:-3}"    # gap between the MySQL and PG dumps: guarantees
                                                 # cross-storage commits land on the PG side but not
                                                 # the frozen MySQL side, so the raw copy is broken
EXPIRY_WAIT_SECONDS="${EXPIRY_WAIT_SECONDS:-20}" # orphaned PREPARED records self-abort past the tx lifetime

# MySQL databases to dump: the user namespace + the ScalarDB metadata namespace.
MYSQL_DATABASES="${MYSQL_DATABASES:-transfer scalardb}"

log() { printf '\n=== %s ===\n' "$*"; }

run_app() { "${COMPOSE[@]}" run --rm app "$@"; }

# --- build: (re)build the local fat jar, stage it, build the app image. --------------
step_build() {
  log "Build: local CBRL fat jar + app image"
  JAVA_HOME="${JAVA_HOME_GRADLE}" "${REPO_ROOT}/gradlew" -p "${REPO_ROOT}" \
    :schema-loader:shadowJar --console=plain -q
  cp "${SHADOW_JAR}" "${DEMO_DIR}/app/scalardb-app.jar"
  "${COMPOSE[@]}" build app
}

# --- up: start the four DB containers (primaries + copies), wait healthy. -------------
step_up() {
  log "Up: start MySQL + PostgreSQL primaries and copies (isolated network, no host ports)"
  "${COMPOSE[@]}" up -d --wait mysql postgres mysql-copy postgres-copy
  "${COMPOSE[@]}" ps
}

# --- schema: create service tables + coordinator tables (incl. CBRL coordinator.backup). --
step_schema() {
  log "Schema: create service + coordinator tables on the primaries (local schema-loader)"
  run_app com.scalar.db.schemaloader.SchemaLoader --config "${PROPS}" -f "${SCHEMA}" --coordinator
}

# --- populate: seed both namespaces (window CLOSED -> pre-window base, carried by the copy). --
step_populate() {
  log "Populate: seed ${NUM_ACCOUNTS} accounts/namespace at balance ${INITIAL_BALANCE}"
  run_app CbrlDemoDriver populate "${PROPS}" "${NUM_ACCOUNTS}" "${INITIAL_BALANCE}"
}

# --- open: open the backup window (writes coordinator.backup BACKING_UP + enables redo). --
step_open() {
  log "Open: open backup window '${BACKUP_LABEL}' (enableRedoLogging)"
  run_app CbrlDemoDriver open "${PROPS}" "${BACKUP_LABEL}"
  log "Cache-wait ${CACHE_WAIT_SECONDS}s (let BackupModeDaemon observe the window)"
  sleep "${CACHE_WAIT_SECONDS}"
}

# --- backup: physical dumps of both PRIMARIES into the shared volume, taken live. -----
# User table (MySQL) first, coordinator (PG, dumped with transfer_pg) LAST, so the
# coordinator backup is a superset of everything the user dumps reflect.
step_backup() {
  log "Backup: mysqldump + pg_dump of the primaries into /backups (non-snapshot-consistent, live)"
  "${COMPOSE[@]}" exec -T mysql \
    sh -c "mysqldump --single-transaction -uroot -pmysql --databases ${MYSQL_DATABASES} > /backups/mysql.sql"
  echo "  wrote /backups/mysql.sql (MySQL frozen at this instant)"
  # Hold before dumping the coordinator/PG side: cross-storage transfers keep committing, so the PG
  # dump reflects commits the frozen MySQL dump does not. This guarantees the raw copy is broken.
  echo "  gap ${BACKUP_GAP_SECONDS}s before the coordinator/PG dump (transfers keep committing)"
  sleep "${BACKUP_GAP_SECONDS}"
  "${COMPOSE[@]}" exec -T postgres \
    sh -c "pg_dump -U postgres -d postgres > /backups/pg.sql"
  echo "  wrote /backups/pg.sql (coordinator + PG, taken later than MySQL)"
}

# --- workload: run the cross-storage transfer under the open window WHILE backups are taken. --
step_workload() {
  log "Workload: cross-storage transfer for ${RUN_SECONDS}s; back up mid-flight"
  local log_file="${DEMO_DIR}/transfer.out"
  run_app CbrlDemoDriver transfer "${PROPS}" "${BACKUP_LABEL}" \
      "${NUM_ACCOUNTS}" "${RUN_SECONDS}" "${THREADS}" > "${log_file}" 2>&1 &
  local tpid=$!
  echo "  transfer job PID ${tpid} (logging to ${log_file})"
  sleep "${WARMUP_SECONDS}"
  step_backup
  wait "${tpid}"
  echo "--- transfer output ---"
  cat "${log_file}"
}

# --- restore-load: load the dumps into the COPY servers (physical restore). -----------
step_restore_load() {
  log "Restore-load: load dumps into the copy servers"
  "${COMPOSE[@]}" exec -T mysql-copy \
    sh -c "mysql -uroot -pmysql < /backups/mysql.sql"
  echo "  loaded MySQL copy"
  "${COMPOSE[@]}" exec -T postgres-copy \
    sh -c "psql -v ON_ERROR_STOP=0 -U postgres -d postgres -f /backups/pg.sql > /dev/null"
  echo "  loaded PostgreSQL copy"
}

# --- check-raw: negative control — assert the raw copy is BROKEN before CBRL repairs it. ----
step_check_raw() {
  log "Check-raw (negative control): assert the RAW restored copy is INCONSISTENT before CBRL"
  run_app CbrlDemoDriver check-raw "${COPY_PROPS}" "${NUM_ACCOUNTS}" "${INITIAL_BALANCE}"
}

# --- expire: wait past the tx lifetime so orphaned PREPARED records can self-abort. ---
step_expire() {
  log "Expire-wait ${EXPIRY_WAIT_SECONDS}s (orphaned in-flight records self-abort on recovery)"
  sleep "${EXPIRY_WAIT_SECONDS}"
}

# --- cbrl-restore: replay redo + recover in-flight records on the copy. ---------------
step_cbrl_restore() {
  log "CbrlRestore: replay redo + recover in-flight records on the copy, label '${BACKUP_LABEL}'"
  run_app CbrlDemoDriver restore "${COPY_PROPS}" "${BACKUP_LABEL}"
}

# --- check: verify the RESTORED copy is complete, all-committed, and conserved. --------
step_check() {
  log "Check (final oracle): copy is complete + all-COMMITTED + conserved on the restored copy"
  run_app CbrlDemoDriver check "${COPY_PROPS}" "${NUM_ACCOUNTS}" "${INITIAL_BALANCE}"
}

step_down() {
  log "Down: remove containers, networks, and volumes"
  "${COMPOSE[@]}" down -v --remove-orphans
  # One-shot `docker compose run` jobs can leave the project's (unused) default network
  # behind; remove it best-effort so teardown leaves no trace.
  docker network rm cbrl-demo_default >/dev/null 2>&1 || true
}

run_all() {
  step_build
  step_up
  step_schema
  step_populate
  step_open
  step_workload
  step_restore_load
  step_check_raw
  step_expire
  step_cbrl_restore
  step_check
  log "Demo complete"
}

run_resume() {
  step_open
  step_workload
  step_restore_load
  step_check_raw
  step_expire
  step_cbrl_restore
  step_check
  log "Resume complete"
}

main() {
  if [ "$#" -eq 0 ]; then run_all; return; fi
  case "$1" in
    resume) run_resume ;;
    build) step_build ;;
    up) step_up ;;
    schema) step_schema ;;
    populate) step_populate ;;
    open) step_open ;;
    backup) step_backup ;;
    workload) step_workload ;;
    restore-load) step_restore_load ;;
    check-raw) step_check_raw ;;
    expire) step_expire ;;
    cbrl-restore) step_cbrl_restore ;;
    check) step_check ;;
    down) step_down ;;
    *) echo "Unknown step: $1" ; exit 2 ;;
  esac
}

main "$@"
