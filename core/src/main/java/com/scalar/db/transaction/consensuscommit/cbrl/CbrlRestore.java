package com.scalar.db.transaction.consensuscommit.cbrl;

import com.google.protobuf.ByteString;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteBuilder;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.GetBuilder;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.service.TransactionFactory;
import com.scalar.db.transaction.consensuscommit.Attribute;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import com.scalar.db.transaction.consensuscommit.CoordinatorGroupCommitter.CoordinatorGroupCommitKeyManipulator;
import com.scalar.db.transaction.consensuscommit.TransactionTableMetadata;
import com.scalar.db.transaction.consensuscommit.TransactionTableMetadataManager;
import com.scalar.db.transaction.consensuscommit.proto.v1.Entry;
import com.scalar.db.transaction.consensuscommit.proto.v1.EntryGroup;
import com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet;
import com.scalar.db.util.TimeRelatedColumnEncodingUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 * Orchestrates the whole CBRL restore against a non-snapshot-consistent copy and the redo captured
 * in the coordinator backup. It uses <b>only the Storage API</b> for applying records and core's
 * record-level recovery for resolving in-flight ones — like SSR, it never runs a transaction, so a
 * restore creates <b>no coordinator rows</b> for its own work. Every record is routed by the
 * namespace and table carried in its redo {@link Entry}, so one restore spans every namespace the
 * backup window touched (the coordinator namespace alone comes from configuration). {@link
 * #restore()} reads the redo from the restored coordinator table, then for each record key — on the
 * worker that owns it — does three steps:
 *
 * <ol>
 *   <li><b>C4 recovery:</b> resolve the copy's in-flight ({@code PREPARED}/{@code DELETED}) record
 *       against the already-restored coordinator via {@link
 *       DistributedTransactionManager#recoverRecord} — core rolls it forward/back with the Storage
 *       API. The coordinator must already hold a terminal state for every writer (it is restored
 *       from its own backup, like any table), so recovery only rolls the record and writes nothing
 *       to the coordinator. {@code recoverRecord} is a no-op for an already-committed or absent
 *       record, so it is called per redo key without a prior scan.
 *   <li><b>Chain replay (§5):</b> replay the key's committed redo forward from its copied version
 *       via {@link RecordApplier}.
 *   <li><b>Write-back:</b> persist the reconstructed record as a COMMITTED Consensus Commit record
 *       with a raw {@code storage.put} (deletes via {@code storage.delete}) — no transaction.
 * </ol>
 *
 * <p>Callers do the backup-taking and ensure the coordinator and user tables are restored (in a
 * test, arranged) before calling only {@link #restore()}.
 */
public final class CbrlRestore implements AutoCloseable {
  private final DistributedStorage storage;
  private final DistributedTransactionManager manager;
  private final DistributedStorageAdmin admin;
  // True if this instance built storage/manager/admin (and so must close them); false if they were
  // injected by the caller, who then owns their lifecycle.
  private final boolean ownsResources;
  // Stateless; constructed here rather than leaked through the public constructor as an internal
  // type that external callers have no other reason to depend on.
  private final CoordinatorGroupCommitKeyManipulator keyManipulator =
      new CoordinatorGroupCommitKeyManipulator();
  // Namespace of the restored coordinator table — read for the redo and original commit times.
  private final String coordinatorNamespace;
  // Resolves each restored table's schema on demand (cached), so the user (non-key,
  // non-tx-metadata)
  // columns read as the replay base are derived from the table rather than supplied by the caller.
  private final TransactionTableMetadataManager metadataManager;
  private final int replayBuckets;
  private final int replayWorkers;

  /**
   * Builds its own ScalarDB handles (storage, transaction manager, storage admin) from {@code
   * properties}; {@link #close()} closes them. There is no namespace argument — restore routes each
   * record by the namespace in its redo, and the coordinator namespace and replay concurrency come
   * from {@code properties}, so the caller hand-derives nothing.
   */
  public CbrlRestore(Properties properties) throws ExecutionException {
    this(properties, null, null, null);
  }

  /**
   * Uses caller-provided ScalarDB handles (the caller owns their lifecycle — e.g. a shared test
   * factory or crash-injecting proxies); everything else is still derived from {@code properties}
   * and the schema. The three handles must be all non-null or all null (the public constructor uses
   * the all-null form to build its own).
   */
  CbrlRestore(
      Properties properties,
      @Nullable DistributedStorage storage,
      @Nullable DistributedTransactionManager manager,
      @Nullable DistributedStorageAdmin admin)
      throws ExecutionException {
    boolean injected = storage != null;
    if (injected != (manager != null) || injected != (admin != null)) {
      throw new IllegalArgumentException(
          "storage, manager, and admin must all be null or all non-null");
    }
    CbrlConfig config = new CbrlConfig(properties);
    this.coordinatorNamespace = config.getCoordinatorNamespace();
    this.replayBuckets = config.getReplayBuckets();
    this.replayWorkers = config.getReplayWorkers();
    if (injected) {
      this.storage = storage;
      this.manager = manager;
      this.admin = admin;
      this.ownsResources = false;
    } else {
      StorageFactory storageFactory = StorageFactory.create(properties);
      TransactionFactory transactionFactory = TransactionFactory.create(properties);
      this.storage = storageFactory.getStorage();
      this.manager = transactionFactory.getTransactionManager();
      this.admin = storageFactory.getStorageAdmin();
      this.ownsResources = true;
    }
    // The schema does not change during a one-shot restore, so cache it for the whole run (no
    // expiry).
    this.metadataManager = new TransactionTableMetadataManager(this.admin, -1);
  }

  /** Closes the ScalarDB handles only if this instance built them. */
  @Override
  public void close() {
    if (ownsResources) {
      storage.close();
      manager.close();
      admin.close();
    }
  }

  /**
   * Reads the redo from the restored coordinator table, then for each record key recovers the copy,
   * replays the committed redo, and writes the result back via the Storage API. Every input is the
   * database: the redo and the original commit times are read from the restored coordinator table
   * here, not supplied by the caller.
   */
  public void restore() throws Exception {
    // Original commit time (the coordinator's tx_created_at) per transaction id, used to stamp the
    // restored records so they keep their commit timestamp instead of the restore-time clock.
    Map<String, Long> committedAtByTxId = new HashMap<>();
    List<RedoOperation> redoOps = new ArrayList<>();
    for (Coordinator.State state : readCoordinatorTable()) {
      // The column is filtered to present in readCoordinatorTable().
      WriteSet writeSet = state.getWriteSet().get();
      for (EntryGroup group : writeSet.getEntryGroupsList()) {
        // The writing transaction's full id is what records store and what other ops' prev_tx_id
        // chains to. For a normal group commit the row is keyed by the parent id and each child's
        // EntryGroup carries its child_id, so the full id = parent + child; otherwise the row's key
        // already is the full id.
        String txId =
            group.getChildId().isEmpty()
                ? state.getId()
                : keyManipulator.fullKey(state.getId(), group.getChildId());
        committedAtByTxId.put(txId, state.getCreatedAt());
        for (Entry entry : group.getEntriesList()) {
          if (!entry.hasTxVersion()) {
            // Key-only write set from outside the backup window (logging was off): not redo. The
            // copy carries that state instead.
            continue;
          }
          redoOps.add(new RedoOperation(txId, entry, state.getCreatedAt()));
        }
      }
    }

    List<RedoBucket> buckets = RecordShuffler.shuffle(redoOps, replayBuckets);
    new RecordApplier(key -> readCopyState(key, committedAtByTxId))
        .apply(buckets, replayWorkers, this::applyToRestore);
  }

  /**
   * The coordinator backup, read from the restored coordinator table itself (the caller has
   * physically restored it like any other table): every {@code COMMITTED} row carrying a {@code
   * tx_write_set}. This is both the redo source and the original-commit-time source — CBRL reads it
   * from the database rather than receiving it, so every restore input is the database.
   */
  private List<Coordinator.State> readCoordinatorTable() throws Exception {
    List<Coordinator.State> states = new ArrayList<>();
    try (Scanner scanner =
        storage.scan(
            Scan.newBuilder()
                .namespace(coordinatorNamespace)
                .table(Coordinator.TABLE)
                .all()
                .build())) {
      for (Result result : scanner.all()) {
        if (result.isNull(Attribute.STATE)
            || result.getInt(Attribute.STATE) != TransactionState.COMMITTED.get()
            || result.isNull(Attribute.WRITE_SET)) {
          // Not a committed-with-redo row (e.g. an ABORTED in-doubt marker): no redo to replay.
          continue;
        }
        states.add(new Coordinator.State(result));
      }
    }
    return states;
  }

  /**
   * Reads the recovered copy record as the replay base: committed tx id, version, user columns. The
   * key's in-flight copy record is first resolved against the already-restored coordinator (C4:
   * {@code recoverRecord} rolls a {@code PREPARED}/{@code DELETED} record forward/back, or no-ops
   * if it is already committed or absent), so the base reads clean. This is the per-key recovery
   * that runs on the owning pass-2 worker, with no full-table scan.
   */
  private RecordState readCopyState(RecordKey key, Map<String, Long> committedAtByTxId) {
    try {
      recoverKey(key);
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
      for (String name : userColumnNames(metadataOf(key))) {
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

  /** The key's table schema, from the cached metadata manager. */
  private TransactionTableMetadata metadataOf(RecordKey key) throws ExecutionException {
    TransactionTableMetadata metadata =
        metadataManager.getTransactionTableMetadata(key.namespace(), key.table());
    if (metadata == null) {
      throw new IllegalArgumentException(
          "Redo references a table with no schema: " + key.namespace() + "." + key.table());
    }
    return metadata;
  }

  /**
   * The user (non-key, non-tx-metadata) column names of a table, from its schema. These are the
   * columns read as the replay base and written back; the caller no longer supplies them.
   */
  private static List<String> userColumnNames(TransactionTableMetadata metadata) {
    List<String> userColumns = new ArrayList<>();
    for (String name : metadata.getColumnNames()) {
      if (!metadata.getPrimaryKeyColumnNames().contains(name)
          && !metadata.getTransactionMetaColumnNames().contains(name)) {
        userColumns.add(name);
      }
    }
    return userColumns;
  }

  /**
   * C4: resolves the key's in-flight ({@code PREPARED}/{@code DELETED}) copy record against the
   * already-restored coordinator via core's record-level recovery, so the replay base reads clean
   * committed-or-absent. With a terminal coordinator state present for the writer (the caller
   * restored the coordinator from its backup), {@code recoverRecord} only rolls the record — it
   * writes nothing to the coordinator, and never uses a transaction. It is a no-op for an
   * already-committed or absent record.
   */
  private void recoverKey(RecordKey key) throws Exception {
    Key partitionKey = toIoKey(key.partitionKey());
    Key clusteringKey = key.clusteringKey() == null ? null : toIoKey(key.clusteringKey());
    boolean resolved =
        manager.recoverRecord(key.namespace(), key.table(), partitionKey, clusteringKey);
    if (!resolved) {
      throw new IllegalStateException(
          "Copy record not recoverable — its writer has no coordinator state and has not expired;"
              + " the coordinator backup must be restored before restore(). Key: "
              + key);
    }
  }

  /**
   * Persists one reconstructed record into its restore table with the Storage API — SSR-style, no
   * transaction: a present record as a COMMITTED Consensus Commit record ({@code storage.put}), an
   * absent record removed ({@code storage.delete}). Called per key by the owning pass-2 worker. A
   * crash mid-restore leaves a partial set; a re-run re-derives the same final states and re-stamps
   * them idempotently.
   */
  private void applyToRestore(RecordKey key, RecordState state) throws Exception {
    if (state.present()) {
      storage.put(buildCommittedPut(key, state));
    } else {
      storage.delete(buildDelete(key));
    }
  }

  /**
   * A COMMITTED Consensus Commit record for a raw {@code storage.put}, mirroring {@code
   * CommitMutationComposer}'s committed image: every user column plus {@code tx_id}, {@code
   * tx_state = COMMITTED}, {@code tx_version}, and commit/prepare timestamps. It writes the
   * <b>full</b> user column set — the replayed value where the state has one, an explicit NULL
   * otherwise — because {@code storage.put} is a partial-column UPSERT: a column the replayed state
   * dropped (e.g. a re-insert with fewer columns than the non-snapshot copy still physically holds)
   * would otherwise keep its stale copy value. This mirrors {@code
   * RecordApplyService.fillUnsetColumnsWithNull} in the replication log applier, which nulls unset
   * columns for the same reason. The {@code before_*} images are already cleared on the recovered
   * base, so the omitted meta columns stay cleared.
   */
  private Put buildCommittedPut(RecordKey key, RecordState state) throws ExecutionException {
    PutBuilder.Buildable put =
        Put.newBuilder()
            .namespace(key.namespace())
            .table(key.table())
            .partitionKey(toIoKey(key.partitionKey()));
    if (key.clusteringKey() != null) {
      put.clusteringKey(toIoKey(key.clusteringKey()));
    }
    TransactionTableMetadata metadata = metadataOf(key);
    for (String name : userColumnNames(metadata)) {
      com.scalar.db.transaction.consensuscommit.proto.v1.Column column = state.columns().get(name);
      put.value(
          column != null
              ? toIoColumn(column)
              : nullColumnOf(name, metadata.getColumnDataType(name)));
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
            .namespace(key.namespace())
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
            .namespace(key.namespace())
            .table(key.table())
            .partitionKey(toIoKey(key.partitionKey()));
    if (key.clusteringKey() != null) {
      builder.clusteringKey(toIoKey(key.clusteringKey()));
    }
    return builder.build();
  }

  /** A null-valued {@link Column} of the given type, used to clear a dropped user column. */
  private static Column<?> nullColumnOf(String name, DataType type) {
    switch (type) {
      case BOOLEAN:
        return BooleanColumn.ofNull(name);
      case INT:
        return IntColumn.ofNull(name);
      case BIGINT:
        return BigIntColumn.ofNull(name);
      case FLOAT:
        return FloatColumn.ofNull(name);
      case DOUBLE:
        return DoubleColumn.ofNull(name);
      case TEXT:
        return TextColumn.ofNull(name);
      case BLOB:
        return BlobColumn.ofNull(name);
      case DATE:
        return DateColumn.ofNull(name);
      case TIME:
        return TimeColumn.ofNull(name);
      case TIMESTAMP:
        return TimestampColumn.ofNull(name);
      case TIMESTAMPTZ:
        return TimestampTZColumn.ofNull(name);
      default:
        throw new AssertionError("Unsupported data type: " + type);
    }
  }

  // Mirrors WriteSetEncoder's ColumnEncodingVisitor exactly (same TimeRelatedColumnEncodingUtils),
  // so a value read from the copy base encodes identically to the same value carried in the redo.
  static com.scalar.db.transaction.consensuscommit.proto.v1.Column ioColumnToProto(
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
    if (column instanceof FloatColumn) {
      com.scalar.db.transaction.consensuscommit.proto.v1.Column.FloatValue.Builder value =
          com.scalar.db.transaction.consensuscommit.proto.v1.Column.FloatValue.newBuilder();
      if (!column.hasNullValue()) {
        value.setValue(((FloatColumn) column).getFloatValue());
      }
      return builder.setFloatValue(value).build();
    }
    if (column instanceof DoubleColumn) {
      com.scalar.db.transaction.consensuscommit.proto.v1.Column.DoubleValue.Builder value =
          com.scalar.db.transaction.consensuscommit.proto.v1.Column.DoubleValue.newBuilder();
      if (!column.hasNullValue()) {
        value.setValue(((DoubleColumn) column).getDoubleValue());
      }
      return builder.setDoubleValue(value).build();
    }
    if (column instanceof DateColumn) {
      com.scalar.db.transaction.consensuscommit.proto.v1.Column.DateValue.Builder value =
          com.scalar.db.transaction.consensuscommit.proto.v1.Column.DateValue.newBuilder();
      if (!column.hasNullValue()) {
        value.setValue(TimeRelatedColumnEncodingUtils.encode((DateColumn) column));
      }
      return builder.setDateValue(value).build();
    }
    if (column instanceof TimeColumn) {
      com.scalar.db.transaction.consensuscommit.proto.v1.Column.TimeValue.Builder value =
          com.scalar.db.transaction.consensuscommit.proto.v1.Column.TimeValue.newBuilder();
      if (!column.hasNullValue()) {
        value.setValue(TimeRelatedColumnEncodingUtils.encode((TimeColumn) column));
      }
      return builder.setTimeValue(value).build();
    }
    if (column instanceof TimestampColumn) {
      com.scalar.db.transaction.consensuscommit.proto.v1.Column.TimestampValue.Builder value =
          com.scalar.db.transaction.consensuscommit.proto.v1.Column.TimestampValue.newBuilder();
      if (!column.hasNullValue()) {
        value.setValue(TimeRelatedColumnEncodingUtils.encode((TimestampColumn) column));
      }
      return builder.setTimestampValue(value).build();
    }
    if (column instanceof TimestampTZColumn) {
      com.scalar.db.transaction.consensuscommit.proto.v1.Column.TimestampTZValue.Builder value =
          com.scalar.db.transaction.consensuscommit.proto.v1.Column.TimestampTZValue.newBuilder();
      if (!column.hasNullValue()) {
        value.setValue(TimeRelatedColumnEncodingUtils.encode((TimestampTZColumn) column));
      }
      return builder.setTimestamptzValue(value).build();
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

  static Column<?> toIoColumn(com.scalar.db.transaction.consensuscommit.proto.v1.Column column) {
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
    if (column.hasFloatValue()) {
      return column.getFloatValue().hasValue()
          ? FloatColumn.of(name, column.getFloatValue().getValue())
          : FloatColumn.ofNull(name);
    }
    if (column.hasDoubleValue()) {
      return column.getDoubleValue().hasValue()
          ? DoubleColumn.of(name, column.getDoubleValue().getValue())
          : DoubleColumn.ofNull(name);
    }
    if (column.hasDateValue()) {
      return column.getDateValue().hasValue()
          ? DateColumn.of(
              name, TimeRelatedColumnEncodingUtils.decodeDate(column.getDateValue().getValue()))
          : DateColumn.ofNull(name);
    }
    if (column.hasTimeValue()) {
      return column.getTimeValue().hasValue()
          ? TimeColumn.of(
              name, TimeRelatedColumnEncodingUtils.decodeTime(column.getTimeValue().getValue()))
          : TimeColumn.ofNull(name);
    }
    if (column.hasTimestampValue()) {
      return column.getTimestampValue().hasValue()
          ? TimestampColumn.of(
              name,
              TimeRelatedColumnEncodingUtils.decodeTimestamp(column.getTimestampValue().getValue()))
          : TimestampColumn.ofNull(name);
    }
    if (column.hasTimestamptzValue()) {
      return column.getTimestamptzValue().hasValue()
          ? TimestampTZColumn.of(
              name,
              TimeRelatedColumnEncodingUtils.decodeTimestampTZ(
                  column.getTimestamptzValue().getValue()))
          : TimestampTZColumn.ofNull(name);
    }
    throw new IllegalStateException("Unsupported column value in redo entry: " + column);
  }
}
