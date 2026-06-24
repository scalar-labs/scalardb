package com.scalar.db.transaction.consensuscommit.cbrl;

import com.google.protobuf.ByteString;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteBuilder;
import com.scalar.db.api.DistributedStorage;
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
import com.scalar.db.transaction.consensuscommit.CoordinatorGroupCommitter.CoordinatorGroupCommitKeyManipulator;
import com.scalar.db.transaction.consensuscommit.proto.v1.Entry;
import com.scalar.db.transaction.consensuscommit.proto.v1.EntryGroup;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Orchestrates the whole CBRL restore against a non-snapshot-consistent copy and the redo captured
 * in the coordinator backup. It uses <b>only the Storage API</b> for applying records and core's
 * record-level recovery for resolving in-flight ones — like SSR, it never runs a transaction, so a
 * restore creates <b>no coordinator rows</b> for its own work. {@link #restore(Map)} does three
 * steps:
 *
 * <ol>
 *   <li><b>C4 recovery:</b> resolve the copy's in-flight ({@code PREPARED}/{@code DELETED}) records
 *       against the already-restored coordinator via {@link
 *       DistributedTransactionManager#recoverRecord} — core rolls each forward/back with the
 *       Storage API. The coordinator must already hold a terminal state for every writer (it is
 *       restored from its own backup, like any table), so recovery only rolls records and writes
 *       nothing to the coordinator.
 *   <li><b>Chain replay (§5):</b> explode the backup's committed redo into {@link RedoOperation}s
 *       and replay them forward from each record's copied version via {@link RecordApplier}.
 *   <li><b>Write-back:</b> persist each reconstructed record as a COMMITTED Consensus Commit record
 *       with a raw {@code storage.put} (deletes via {@code storage.delete}) — no transaction.
 * </ol>
 *
 * <p>Callers do the backup-taking and ensure the coordinator and user tables are restored (in a
 * test, arranged) before calling only {@link #restore(Map)}.
 */
public final class CbrlRestore {
  private final DistributedStorage storage;
  private final DistributedTransactionManager manager;
  private final CoordinatorGroupCommitKeyManipulator keyManipulator;
  private final String restoreNamespace;
  // Restore table -> its user (non-key, non-tx-metadata) column names, read as the replay base.
  private final Map<String, List<String>> userColumnsByTable;
  private final int replayBuckets;
  private final int replayWorkers;

  public CbrlRestore(
      DistributedStorage storage,
      DistributedTransactionManager manager,
      CoordinatorGroupCommitKeyManipulator keyManipulator,
      String restoreNamespace,
      Map<String, List<String>> userColumnsByTable,
      int replayBuckets,
      int replayWorkers) {
    this.storage = storage;
    this.manager = manager;
    this.keyManipulator = keyManipulator;
    this.restoreNamespace = restoreNamespace;
    this.userColumnsByTable = userColumnsByTable;
    this.replayBuckets = replayBuckets;
    this.replayWorkers = replayWorkers;
  }

  /**
   * Recovers the copy, replays the committed redo, and writes the result back via the Storage API.
   */
  public void restore(Map<String, CoordinatorBackupRow> coordinatorBackup) throws Exception {
    recoverCopy();

    // Original commit time (the coordinator's tx_created_at) per transaction id, used to stamp the
    // restored records so they keep their commit timestamp instead of the restore-time clock.
    Map<String, Long> committedAtByTxId = new HashMap<>();
    List<RedoOperation> redoOps = new ArrayList<>();
    for (CoordinatorBackupRow row : coordinatorBackup.values()) {
      committedAtByTxId.put(row.txId, row.createdAt);
      for (EntryGroup group : row.writeSet.getEntryGroupsList()) {
        // The writing transaction's full id is what records store and what other ops' prev_tx_id
        // chains to. For a normal group commit the row is keyed by the parent id and each child's
        // EntryGroup carries its child_id, so the full id = parent + child; otherwise the row's key
        // already is the full id.
        String txId =
            group.getChildId().isEmpty()
                ? row.txId
                : keyManipulator.fullKey(row.txId, group.getChildId());
        committedAtByTxId.put(txId, row.createdAt);
        for (Entry entry : group.getEntriesList()) {
          if (!entry.hasTxVersion()) {
            // Key-only write set from outside the backup window (logging was off): not redo. The
            // copy carries that state instead.
            continue;
          }
          redoOps.add(new RedoOperation(txId, entry, row.createdAt));
        }
      }
    }

    List<List<RedoOperation>> buckets = new RecordShuffler().shuffle(redoOps, replayBuckets);
    Map<RecordKey, RecordState> finalStates =
        new RecordApplier(key -> readCopyState(key, committedAtByTxId))
            .apply(buckets, replayWorkers);

    writeBack(finalStates);
  }

  /**
   * C4: resolves every in-flight ({@code PREPARED}/{@code DELETED}) copy record against the
   * already-restored coordinator via core's record-level recovery, so the replay base reads clean
   * committed-or-absent values. With a terminal coordinator state present for each writer (the
   * caller restored the coordinator from its backup), {@code recoverRecord} only rolls the record —
   * it writes nothing to the coordinator, and never uses a transaction.
   */
  private void recoverCopy() throws Exception {
    for (String table : userColumnsByTable.keySet()) {
      List<Key[]> inDoubt = new ArrayList<>();
      try (Scanner scanner = storage.scan(scanAll(table))) {
        for (Result result : scanner.all()) {
          if (!result.isNull(Attribute.STATE)
              && result.getInt(Attribute.STATE) != TransactionState.COMMITTED.get()) {
            Key partitionKey = result.getPartitionKey().orElseThrow(IllegalStateException::new);
            Key clusteringKey = result.getClusteringKey().orElse(null);
            inDoubt.add(new Key[] {partitionKey, clusteringKey});
          }
        }
      }
      for (Key[] keys : inDoubt) {
        boolean resolved = manager.recoverRecord(restoreNamespace, table, keys[0], keys[1]);
        if (!resolved) {
          throw new IllegalStateException(
              "Copy record not recoverable — its writer has no coordinator state and has not"
                  + " expired; the coordinator backup must be restored before restore(). Table: "
                  + table
                  + "; Partition Key: "
                  + keys[0]);
        }
      }
    }
  }

  /**
   * Reads the (recovered) copy record as the replay base: committed tx id, version, user columns.
   */
  private RecordState readCopyState(RecordKey key, Map<String, Long> committedAtByTxId) {
    try {
      Optional<Result> result = storage.get(rawGet(key));
      if (!result.isPresent() || result.get().isNull(Attribute.ID)) {
        return RecordState.absent();
      }
      Result record = result.get();
      String currentTxId = record.getText(Attribute.ID);
      int version = record.isNull(Attribute.VERSION) ? 0 : record.getInt(Attribute.VERSION);
      // Prefer the writer's coordinator commit time: recovery (rollforward) may have re-stamped the
      // record's tx_committed_at to recovery-time. Fall back to the record's column for txs not in
      // the backup (e.g. pre-window writers, whose copied record still carries the original time).
      long committedAt =
          committedAtByTxId.getOrDefault(
              currentTxId,
              record.isNull(Attribute.COMMITTED_AT)
                  ? 0L
                  : record.getBigInt(Attribute.COMMITTED_AT));
      Map<String, com.scalar.db.transaction.consensuscommit.proto.v1.Column> columns =
          new LinkedHashMap<>();
      for (String name : userColumnsByTable.get(key.table())) {
        Column<?> column = record.getColumns().get(name);
        if (column != null) {
          columns.put(name, ioColumnToProto(column));
        }
      }
      return RecordState.of(
          currentTxId, false, columns, Collections.emptySet(), version, committedAt);
    } catch (Exception e) {
      throw new RuntimeException("Failed to read copy base for " + key, e);
    }
  }

  /**
   * Persists each reconstructed record into the restore tables with the Storage API — SSR-style,
   * one record at a time, no transaction: present records as COMMITTED Consensus Commit records
   * ({@code storage.put}), absent records removed ({@code storage.delete}). A crash mid-write-back
   * leaves a partial set; a re-run re-derives the same final states and re-stamps them
   * idempotently.
   */
  private void writeBack(Map<RecordKey, RecordState> finalStates) throws Exception {
    for (Map.Entry<RecordKey, RecordState> entry : finalStates.entrySet()) {
      applyToRestore(entry.getKey(), entry.getValue());
    }
  }

  private void applyToRestore(RecordKey key, RecordState state) throws Exception {
    if (state.present()) {
      storage.put(buildCommittedPut(key, state));
    } else {
      storage.delete(buildDelete(key));
    }
  }

  /**
   * A COMMITTED Consensus Commit record for a raw {@code storage.put}, mirroring {@code
   * CommitMutationComposer}'s committed image: final user columns plus {@code tx_id}, {@code
   * tx_state = COMMITTED}, {@code tx_version}, and commit/prepare timestamps. The {@code before_*}
   * images are already cleared on the recovered base, and {@code storage.put} only overwrites the
   * columns set here, so they stay cleared.
   */
  private Put buildCommittedPut(RecordKey key, RecordState state) {
    PutBuilder.Buildable put =
        Put.newBuilder()
            .namespace(restoreNamespace)
            .table(key.table())
            .partitionKey(toIoKey(key.partitionKey()));
    if (key.clusteringKey() != null) {
      put.clusteringKey(toIoKey(key.clusteringKey()));
    }
    for (com.scalar.db.transaction.consensuscommit.proto.v1.Column column :
        state.columns().values()) {
      put.value(toIoColumn(column));
    }
    put.textValue(Attribute.ID, state.currentTxId());
    put.intValue(Attribute.STATE, TransactionState.COMMITTED.get());
    put.intValue(Attribute.VERSION, state.version());
    // Keep the original commit time (the writing transaction's coordinator tx_created_at), not the
    // restore-time clock. tx_prepared_at is set to the same value — it is not load-bearing for a
    // committed record, and the redo carries no separate prepare time.
    put.bigIntValue(Attribute.PREPARED_AT, state.committedAt());
    put.bigIntValue(Attribute.COMMITTED_AT, state.committedAt());
    return put.build();
  }

  private Delete buildDelete(RecordKey key) {
    DeleteBuilder.Buildable delete =
        Delete.newBuilder()
            .namespace(restoreNamespace)
            .table(key.table())
            .partitionKey(toIoKey(key.partitionKey()));
    if (key.clusteringKey() != null) {
      delete.clusteringKey(toIoKey(key.clusteringKey()));
    }
    return delete.build();
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
