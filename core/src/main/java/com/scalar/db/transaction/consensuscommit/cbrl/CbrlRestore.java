package com.scalar.db.transaction.consensuscommit.cbrl;

import com.google.common.util.concurrent.Uninterruptibles;
import com.google.protobuf.ByteString;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteBuilder;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.GetBuilder;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TransactionState;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.transaction.consensuscommit.Attribute;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import com.scalar.db.transaction.consensuscommit.CoordinatorGroupCommitter.CoordinatorGroupCommitKeyManipulator;
import com.scalar.db.transaction.consensuscommit.proto.v1.Entry;
import com.scalar.db.transaction.consensuscommit.proto.v1.EntryGroup;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Orchestrates the whole CBRL restore against a coordinator backup and a non-snapshot-consistent
 * copy already loaded into the restore tables. {@link #restore(Map)} does the three steps end to
 * end:
 *
 * <ol>
 *   <li><b>C4 recovery:</b> reload the coordinator from the backup (the consistency-point decision
 *       set) and resolve the copy's in-flight (PREPARED/DELETED) records against it via ScalarDB's
 *       lazy recovery, never against the live coordinator.
 *   <li><b>Chain replay (§5):</b> explode the backup's committed redo into {@link RedoOperation}s
 *       and replay them forward from each record's copied version via {@link RecordApplier}.
 *   <li><b>Write-back:</b> persist the reconstructed per-record state into the restore tables.
 * </ol>
 *
 * <p>Callers do the backup-taking (live user-table copy + coordinator snapshot) and then call only
 * {@link #restore(Map)}; the orchestrator owns recovery, replay, and persistence.
 */
public final class CbrlRestore {
  private static final int MAX_RETRIES = 100;
  private static final Duration RECOVERY_POLL = Duration.ofMillis(50);
  private static final long RECOVERY_TIMEOUT_MILLIS = 15_000;

  private final DistributedStorage storage;
  private final DistributedTransactionManager manager;
  private final Coordinator coordinator;
  private final DistributedTransactionAdmin admin;
  private final CoordinatorGroupCommitKeyManipulator keyManipulator;
  private final String restoreNamespace;
  // Restore table -> its user (non-key, non-tx-metadata) column names, read as the replay base.
  private final Map<String, List<String>> userColumnsByTable;
  private final int replayBuckets;
  private final int replayWorkers;

  public CbrlRestore(
      DistributedStorage storage,
      DistributedTransactionManager manager,
      Coordinator coordinator,
      DistributedTransactionAdmin admin,
      CoordinatorGroupCommitKeyManipulator keyManipulator,
      String restoreNamespace,
      Map<String, List<String>> userColumnsByTable,
      int replayBuckets,
      int replayWorkers) {
    this.storage = storage;
    this.manager = manager;
    this.coordinator = coordinator;
    this.admin = admin;
    this.keyManipulator = keyManipulator;
    this.restoreNamespace = restoreNamespace;
    this.userColumnsByTable = userColumnsByTable;
    this.replayBuckets = replayBuckets;
    this.replayWorkers = replayWorkers;
  }

  /** Recovers the copy, replays the committed redo, and writes the result back to the tables. */
  public void restore(Map<String, CoordinatorBackupRow> coordinatorBackup) throws Exception {
    recoverCopyAgainstBackupCoordinator(coordinatorBackup);

    List<RedoOperation> redoOps = new ArrayList<>();
    for (CoordinatorBackupRow row : coordinatorBackup.values()) {
      for (EntryGroup group : row.writeSet.getEntryGroupsList()) {
        // The writing transaction's full id is what records store and what other ops' prev_tx_id
        // chains to. For a normal group commit the row is keyed by the parent id and each child's
        // EntryGroup carries its child_id, so the full id = parent + child; otherwise the row's key
        // already is the full id.
        String txId =
            group.getChildId().isEmpty()
                ? row.txId
                : keyManipulator.fullKey(row.txId, group.getChildId());
        for (Entry entry : group.getEntriesList()) {
          if (!entry.hasTxVersion()) {
            // Key-only write set from outside the backup window (logging was off): not redo. The
            // copy carries that state instead.
            continue;
          }
          redoOps.add(new RedoOperation(txId, entry));
        }
      }
    }

    List<List<RedoOperation>> buckets = new RecordShuffler().shuffle(redoOps, replayBuckets);
    Map<RecordKey, RecordState> finalStates =
        new RecordApplier(this::readCopyState).apply(buckets, replayWorkers);

    writeBack(finalStates);
  }

  /**
   * C4: reload the coordinator from the backup, then drive ScalarDB's lazy recovery so the copy's
   * in-flight records resolve to a clean committed-or-absent base against that backup.
   */
  private void recoverCopyAgainstBackupCoordinator(Map<String, CoordinatorBackupRow> backup)
      throws Exception {
    reloadCoordinatorFromBackup(backup);
    awaitCopyRecovered();
  }

  /**
   * Truncates the coordinator and reloads it from the backup: every backed-up transaction as
   * COMMITTED, and every transaction still PREPARED/DELETED in the copy but absent from the backup
   * as ABORTED (in flight at copy time, never reached the consistency point, so it is discarded).
   */
  private void reloadCoordinatorFromBackup(Map<String, CoordinatorBackupRow> backup)
      throws Exception {
    admin.truncateCoordinatorTables();
    for (CoordinatorBackupRow row : backup.values()) {
      // For a normal group-commit parent row, preserve its child ids so recovery can resolve a
      // child by its full id; the write set is not needed in the reloaded coordinator (the redo is
      // replayed from the in-memory backup). Non-group rows reload with their write set.
      Coordinator.State state =
          row.childIds.isEmpty()
              ? new Coordinator.State(row.txId, row.writeSet, TransactionState.COMMITTED)
              : new Coordinator.State(row.txId, row.childIds, TransactionState.COMMITTED);
      coordinator.putState(state);
    }
    for (String txId : inDoubtCopyTxIds(backup.keySet())) {
      coordinator.putState(new Coordinator.State(txId, TransactionState.ABORTED));
    }
  }

  /**
   * Transaction ids of copy records still in an in-flight state (PREPARED/DELETED) whose
   * transaction is not in the backup — in flight at copy time and never committed by the
   * consistency point.
   */
  private Set<String> inDoubtCopyTxIds(Set<String> committedInBackup) throws Exception {
    Set<String> inDoubt = new HashSet<>();
    for (String table : userColumnsByTable.keySet()) {
      try (Scanner scanner = storage.scan(scanAll(table))) {
        for (Result result : scanner.all()) {
          if (result.isNull(Attribute.ID)
              || result.isNull(Attribute.STATE)
              || result.getInt(Attribute.STATE) == TransactionState.COMMITTED.get()) {
            continue;
          }
          String txId = result.getText(Attribute.ID);
          if (!committedInBackup.contains(txId)) {
            inDoubt.add(txId);
          }
        }
      }
    }
    return inDoubt;
  }

  /**
   * Drives ScalarDB's recovery by reading each in-flight copy record transactionally, then waits
   * until no copy record is left in a PREPARED/DELETED state — so the raw-storage replay base reads
   * resolved values. Recovery is asynchronous and conditional, so it is re-triggered until storage
   * settles.
   */
  private void awaitCopyRecovered() throws Exception {
    long deadlineMillis = System.currentTimeMillis() + RECOVERY_TIMEOUT_MILLIS;
    while (true) {
      List<Get> inFlight = inFlightCopyRecordGets();
      if (inFlight.isEmpty()) {
        return;
      }
      for (Get get : inFlight) {
        readTransactionallyToTriggerRecovery(get);
      }
      if (System.currentTimeMillis() > deadlineMillis) {
        throw new IllegalStateException("Copy did not become recovery-quiescent within 15s");
      }
      Uninterruptibles.sleepUninterruptibly(RECOVERY_POLL);
    }
  }

  /** Transactional Gets for every copy record currently in a non-COMMITTED (in-flight) state. */
  private List<Get> inFlightCopyRecordGets() throws Exception {
    List<Get> gets = new ArrayList<>();
    for (String table : userColumnsByTable.keySet()) {
      try (Scanner scanner = storage.scan(scanAll(table))) {
        for (Result result : scanner.all()) {
          if (!result.isNull(Attribute.STATE)
              && result.getInt(Attribute.STATE) != TransactionState.COMMITTED.get()) {
            GetBuilder.BuildableGet builder =
                Get.newBuilder()
                    .namespace(restoreNamespace)
                    .table(table)
                    .partitionKey(result.getPartitionKey().orElseThrow(IllegalStateException::new));
            result.getClusteringKey().ifPresent(builder::clusteringKey);
            gets.add(builder.build());
          }
        }
      }
    }
    return gets;
  }

  private void readTransactionallyToTriggerRecovery(Get get) {
    RuntimeException last = null;
    for (int retry = 0; retry < MAX_RETRIES; retry++) {
      DistributedTransaction tx = null;
      try {
        tx = manager.begin();
        tx.get(get);
        tx.commit();
        return;
      } catch (Exception e) {
        last = e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
        abortQuietly(tx);
      }
    }
    throw last;
  }

  /** Reads the (recovered) copy record as the replay base: its committed tx id + user columns. */
  private RecordState readCopyState(RecordKey key) {
    try {
      Optional<Result> result = storage.get(rawGet(key));
      if (!result.isPresent() || result.get().isNull(Attribute.ID)) {
        return RecordState.absent();
      }
      Result record = result.get();
      String currentTxId = record.getText(Attribute.ID);
      Map<String, com.scalar.db.transaction.consensuscommit.proto.v1.Column> columns =
          new LinkedHashMap<>();
      for (String name : userColumnsByTable.get(key.table())) {
        Column<?> column = record.getColumns().get(name);
        if (column != null) {
          columns.put(name, ioColumnToProto(column));
        }
      }
      return RecordState.of(currentTxId, false, columns, Collections.emptySet());
    } catch (Exception e) {
      throw new RuntimeException("Failed to read copy base for " + key, e);
    }
  }

  /** Persists every reconstructed record state into the restore tables, in one transaction. */
  private void writeBack(Map<RecordKey, RecordState> finalStates) {
    RuntimeException last = null;
    for (int retry = 0; retry < MAX_RETRIES; retry++) {
      DistributedTransaction tx = null;
      try {
        tx = manager.begin();
        for (Map.Entry<RecordKey, RecordState> entry : finalStates.entrySet()) {
          applyToRestore(tx, entry.getKey(), entry.getValue());
        }
        tx.commit();
        return;
      } catch (Exception e) {
        last = e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
        abortQuietly(tx);
      }
    }
    throw last;
  }

  /** Applies one replayed record state to the restore table within {@code tx} (read-then-write). */
  private void applyToRestore(DistributedTransaction tx, RecordKey key, RecordState state)
      throws Exception {
    Key partitionKey = toIoKey(key.partitionKey());
    Key clusteringKey = key.clusteringKey() == null ? null : toIoKey(key.clusteringKey());

    tx.get(rawGet(key)); // read first so the put/delete is an update, not insert-mode

    if (state.present()) {
      PutBuilder.Buildable put =
          Put.newBuilder()
              .namespace(restoreNamespace)
              .table(key.table())
              .partitionKey(partitionKey);
      if (clusteringKey != null) {
        put.clusteringKey(clusteringKey);
      }
      for (com.scalar.db.transaction.consensuscommit.proto.v1.Column column :
          state.columns().values()) {
        put.value(toIoColumn(column));
      }
      tx.put(put.build());
    } else {
      DeleteBuilder.Buildable delete =
          Delete.newBuilder()
              .namespace(restoreNamespace)
              .table(key.table())
              .partitionKey(partitionKey);
      if (clusteringKey != null) {
        delete.clusteringKey(clusteringKey);
      }
      tx.delete(delete.build());
    }
  }

  private Get rawGet(RecordKey key) {
    GetBuilder.BuildableGet builder =
        Get.newBuilder()
            .namespace(restoreNamespace)
            .table(key.table())
            .partitionKey(toIoKey(key.partitionKey()));
    if (key.clusteringKey() != null) {
      builder.clusteringKey(toIoKey(key.clusteringKey()));
    }
    return builder.build();
  }

  private Scan scanAll(String table) {
    return Scan.newBuilder().namespace(restoreNamespace).table(table).all().build();
  }

  private static void abortQuietly(@Nullable DistributedTransaction tx) {
    if (tx != null) {
      try {
        tx.abort();
      } catch (Exception ignored) {
        // Best effort.
      }
    }
  }

  private static com.scalar.db.transaction.consensuscommit.proto.v1.Column ioColumnToProto(
      Column<?> column) {
    String name = column.getName();
    com.scalar.db.transaction.consensuscommit.proto.v1.Column.Builder builder =
        com.scalar.db.transaction.consensuscommit.proto.v1.Column.newBuilder().setName(name);
    if (column instanceof IntColumn) {
      com.scalar.db.transaction.consensuscommit.proto.v1.Column.IntValue.Builder value =
          com.scalar.db.transaction.consensuscommit.proto.v1.Column.IntValue.newBuilder();
      if (!column.hasNullValue()) {
        value.setValue(((IntColumn) column).getIntValue());
      }
      return builder.setIntValue(value).build();
    }
    if (column instanceof BigIntColumn) {
      com.scalar.db.transaction.consensuscommit.proto.v1.Column.BigIntValue.Builder value =
          com.scalar.db.transaction.consensuscommit.proto.v1.Column.BigIntValue.newBuilder();
      if (!column.hasNullValue()) {
        value.setValue(((BigIntColumn) column).getBigIntValue());
      }
      return builder.setBigintValue(value).build();
    }
    if (column instanceof BooleanColumn) {
      com.scalar.db.transaction.consensuscommit.proto.v1.Column.BooleanValue.Builder value =
          com.scalar.db.transaction.consensuscommit.proto.v1.Column.BooleanValue.newBuilder();
      if (!column.hasNullValue()) {
        value.setValue(((BooleanColumn) column).getBooleanValue());
      }
      return builder.setBooleanValue(value).build();
    }
    if (column instanceof TextColumn) {
      com.scalar.db.transaction.consensuscommit.proto.v1.Column.TextValue.Builder value =
          com.scalar.db.transaction.consensuscommit.proto.v1.Column.TextValue.newBuilder();
      if (!column.hasNullValue()) {
        value.setValue(((TextColumn) column).getTextValue());
      }
      return builder.setTextValue(value).build();
    }
    if (column instanceof BlobColumn) {
      com.scalar.db.transaction.consensuscommit.proto.v1.Column.BlobValue.Builder value =
          com.scalar.db.transaction.consensuscommit.proto.v1.Column.BlobValue.newBuilder();
      if (!column.hasNullValue()) {
        value.setValue(ByteString.copyFrom(((BlobColumn) column).getBlobValueAsBytes()));
      }
      return builder.setBlobValue(value).build();
    }
    throw new IllegalStateException("Unsupported column type for " + name + ": " + column);
  }

  private static Key toIoKey(com.scalar.db.transaction.consensuscommit.proto.v1.Key protoKey) {
    Key.Builder builder = Key.newBuilder();
    for (com.scalar.db.transaction.consensuscommit.proto.v1.Column column :
        protoKey.getColumnsList()) {
      builder.add(toIoColumn(column));
    }
    return builder.build();
  }

  private static Column<?> toIoColumn(
      com.scalar.db.transaction.consensuscommit.proto.v1.Column column) {
    String name = column.getName();
    if (column.hasIntValue()) {
      return column.getIntValue().hasValue()
          ? IntColumn.of(name, column.getIntValue().getValue())
          : IntColumn.ofNull(name);
    }
    if (column.hasBigintValue()) {
      return column.getBigintValue().hasValue()
          ? BigIntColumn.of(name, column.getBigintValue().getValue())
          : BigIntColumn.ofNull(name);
    }
    if (column.hasBooleanValue()) {
      return column.getBooleanValue().hasValue()
          ? BooleanColumn.of(name, column.getBooleanValue().getValue())
          : BooleanColumn.ofNull(name);
    }
    if (column.hasTextValue()) {
      return column.getTextValue().hasValue()
          ? TextColumn.of(name, column.getTextValue().getValue())
          : TextColumn.ofNull(name);
    }
    if (column.hasBlobValue()) {
      return column.getBlobValue().hasValue()
          ? BlobColumn.of(name, column.getBlobValue().getValue().toByteArray())
          : BlobColumn.ofNull(name);
    }
    throw new IllegalStateException("Unsupported column value in redo entry: " + column);
  }
}
