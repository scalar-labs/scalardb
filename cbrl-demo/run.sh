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
#   bash run.sh                       # run both variants: WITHOUT CBRL (expect broken), then WITH CBRL (expect fixed)
#   bash run.sh run-all-without-cbrl  # full pipeline, stopping before the redo; validate expects a STILL-broken copy
#   bash run.sh run-all-with-cbrl     # full pipeline including the redo replay; validate expects a fixed copy
#   bash run.sh <step>                # single step: build|up|schema|populate|cbrl-open|workload|backup|
#                                     #              restore-load|expire|cbrl-restore|validate-broken|validate-consistent|down
#   bash run.sh resume-without-cbrl   # from the window phase on an already-seeded primary, without the redo
#   bash run.sh resume-with-cbrl      # from the window phase on an already-seeded primary, with the redo
#
# The demo is a correctness test, not just a smoke test, and it isolates what the CBRL redo actually
# buys. BOTH variants validate the copy the same way: a Consensus Commit cross-partition ScanAll that
# applies lazy recovery to every in-flight record it reads. `run-all-without-cbrl` stops after the
# physical restore, so that ScanAll observes the copy after recovery ALONE and must find it STILL
# broken, because recovery cannot reconstruct a committed write that never reached the frozen-earlier
# storage. `run-all-with-cbrl` first replays the coordinator redo, and the same ScanAll must then find
# the copy complete and conserved. The demo thus shows: recovery alone = BROKEN -> CBRL redo -> FIXED.
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
WARMUP_SECONDS="${WARMUP_SECONDS:-8}"            # the app commits this long BEFORE the window opens
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

# --- cbrl-open: open the backup window (writes coordinator.backup BACKING_UP + enables redo). --
step_cbrl_open() {
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

# --- workload: the cross-storage transfer. A standalone step; the orchestrator runs it in parallel
# with open + backup (below). The app just commits and never opens the window itself. --
step_workload() {
  log "Workload: cross-storage transfer for ${RUN_SECONDS}s"
  run_app CbrlDemoDriver transfer "${PROPS}" "${BACKUP_LABEL}" \
      "${NUM_ACCOUNTS}" "${RUN_SECONDS}" "${THREADS}" > "${DEMO_DIR}/transfer.out" 2>&1
}

# --- window phase: run the workload in parallel, opening the window mid-run and backing up live. --
# Models real usage: the app is already committing before the window opens and never pauses for the
# backup (RPO > 0). Pre-open commits are the pre-window base carried only by the physical copy;
# commits after the window is opened (and the daemon observes it) log full redo.
run_window_phase() {
  step_workload &
  local workload_pid=$!
  log "Pre-window ${WARMUP_SECONDS}s: the app commits with no window open (base carried by the copy)"
  sleep "${WARMUP_SECONDS}"
  step_cbrl_open  # open the window WHILE the workload keeps committing, in parallel
  step_backup  # take the physical backups mid-flight, without pausing the workload
  wait "${workload_pid}"
  echo "--- transfer output ---"
  cat "${DEMO_DIR}/transfer.out"
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

# --- validate-broken: without-CBRL oracle. A Consensus Commit ScanAll must find the copy STILL
# broken after recovery alone, proving the CBRL redo is what repairs the torn committed writes. ----
step_validate_broken() {
  log "Validate (without CBRL): Consensus Commit ScanAll — expect the copy STILL broken after recovery"
  run_app CbrlDemoDriver validate "${COPY_PROPS}" "${NUM_ACCOUNTS}" "${INITIAL_BALANCE}" broken
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

# --- validate-consistent: with-CBRL oracle. A Consensus Commit ScanAll must find the copy complete
# and conserved after the redo replay. --------
step_validate_consistent() {
  log "Validate (with CBRL): Consensus Commit ScanAll — expect the copy complete + conserved"
  run_app CbrlDemoDriver validate "${COPY_PROPS}" "${NUM_ACCOUNTS}" "${INITIAL_BALANCE}" consistent
}

step_down() {
  log "Down: remove containers, networks, and volumes"
  "${COMPOSE[@]}" down -v --remove-orphans
  # One-shot `docker compose run` jobs can leave the project's (unused) default network
  # behind; remove it best-effort so teardown leaves no trace.
  docker network rm cbrl-demo_default >/dev/null 2>&1 || true
}

# --- run-all-without-cbrl: full pipeline that STOPS before the CBRL redo. The Consensus Commit
# ScanAll oracle must still find the copy broken — physical restore + recovery alone is not enough. --
run_all_without_cbrl() {
  step_down
  step_build
  step_up
  step_schema
  step_populate
  run_window_phase
  step_restore_load
  step_expire
  step_validate_broken
  log "Without-CBRL run complete: recovery alone left the copy broken."
}

# --- run-all-with-cbrl: the full pipeline INCLUDING the CBRL redo replay. The same Consensus Commit
# ScanAll oracle must now find the copy complete and conserved. --
run_all_with_cbrl() {
  step_down
  step_build
  step_up
  step_schema
  step_populate
  run_window_phase
  step_restore_load
  step_expire
  step_cbrl_restore
  step_validate_consistent
  log "With-CBRL run complete: the redo replay repaired the copy."
}

# --- default: run both, so one command tells the whole story. Each run resets the volumes first, so
# the with-CBRL run repairs its own independent, freshly torn copy. --
run_both() {
  log "PHASE 1/2 — WITHOUT CBRL (expect BROKEN)"
  run_all_without_cbrl
  log "PHASE 2/2 — WITH CBRL (expect FIXED)"
  run_all_with_cbrl
  log "Demo complete"
}

# --- resume variants: reuse an already-seeded primary (skip build/up/schema/populate). --
resume_without_cbrl() {
  run_window_phase
  step_restore_load
  step_expire
  step_validate_broken
  log "Without-CBRL resume complete."
}

resume_with_cbrl() {
  run_window_phase
  step_restore_load
  step_expire
  step_cbrl_restore
  step_validate_consistent
  log "With-CBRL resume complete."
}

main() {
  if [ "$#" -eq 0 ]; then run_both; return; fi
  case "$1" in
    run-all-without-cbrl) run_all_without_cbrl ;;
    run-all-with-cbrl) run_all_with_cbrl ;;
    resume-without-cbrl) resume_without_cbrl ;;
    resume-with-cbrl) resume_with_cbrl ;;
    build) step_build ;;
    up) step_up ;;
    schema) step_schema ;;
    populate) step_populate ;;
    cbrl-open) step_cbrl_open ;;
    backup) step_backup ;;
    workload) step_workload ;;
    restore-load) step_restore_load ;;
    expire) step_expire ;;
    cbrl-restore) step_cbrl_restore ;;
    validate-broken) step_validate_broken ;;
    validate-consistent) step_validate_consistent ;;
    down) step_down ;;
    *) echo "Unknown step: $1" ; exit 2 ;;
  esac
}

main "$@"
