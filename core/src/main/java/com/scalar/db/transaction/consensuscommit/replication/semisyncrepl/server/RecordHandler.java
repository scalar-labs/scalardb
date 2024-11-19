package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import static com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.Utils.convPutOperationMetadataToString;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder.Buildable;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.DataType;
import com.scalar.db.transaction.consensuscommit.TransactionTableMetadata;
import com.scalar.db.transaction.consensuscommit.TransactionTableMetadataManager;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.Utils;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Column;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Record;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Record.RecordKey;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Record.Value;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationRecordRepository;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RecordHandler {
  private static final Logger logger = LoggerFactory.getLogger(RecordHandler.class);
  private final ReplicationRecordRepository replicationRecordRepository;
  private final DistributedStorage backupScalarDbStorage;
  private final TransactionTableMetadataManager tableMetadataManager;
  private final MetricsLogger metricsLogger;

  RecordHandler(
      ReplicationRecordRepository replicationRecordRepository,
      DistributedStorage backupScalarDbStorage,
      TransactionTableMetadataManager tableMetadataManager,
      MetricsLogger metricsLogger) {
    this.replicationRecordRepository = replicationRecordRepository;
    this.backupScalarDbStorage = backupScalarDbStorage;
    this.tableMetadataManager = tableMetadataManager;
    this.metricsLogger = metricsLogger;
  }

  @Immutable
  static class NextValue {
    // TODO: `columns` of this field isn't used. Flat this field to `nextTxId`, `nextVersion` and
    //       so on might be good.
    public final Value nextValue;
    public final boolean deleted;
    public final Set<Value> restValues;
    public final Map<String, Column<?>> updatedColumns;
    public final Collection<String> insertTxIds;
    public final boolean shouldHandleTheSameKey;

    public NextValue(
        Value nextValue,
        boolean deleted,
        Set<Value> restValues,
        Map<String, Column<?>> updatedColumns,
        Collection<String> insertTxIds,
        boolean shouldHandleTheSameKey) {
      this.nextValue = nextValue;
      this.deleted = deleted;
      this.restValues = restValues;
      this.updatedColumns = updatedColumns;
      this.insertTxIds = insertTxIds;
      this.shouldHandleTheSameKey = shouldHandleTheSameKey;
    }

    public String toStringOnlyWithMetadata() {
      return MoreObjects.toStringHelper(this)
          .add("nextValue", nextValue.toStringOnlyWithMetadata())
          .add("deleted", deleted)
          .add("restValues", Utils.convValuesToString(restValues))
          .add("insertTxIds", insertTxIds)
          .add("shouldHandleTheSameKey", shouldHandleTheSameKey)
          .toString();
    }
  }

  @VisibleForTesting
  @Nullable
  NextValue findNextValue(RecordKey key, Record record) throws ExecutionException {
    // TODO: Use ArrayDeque instead.
    // This variable is defined as a concrete class. It's needed to use as both Queue and List...
    LinkedList<Value> valuesForInsert = new LinkedList<>();
    Map<String, Value> valuesForNonInsert = new HashMap<>();
    for (Value value : record.values) {
      if (value.type.equals("insert")) {
        if (value.prevTxId != null) {
          throw new IllegalStateException(
              String.format("`prevTxId` should be null. key:%s, value:%s", key, value));
        }
        valuesForInsert.add(value);
      } else {
        valuesForNonInsert.put(value.prevTxId, value);
      }
    }
    // TODO: milli-second timestamp can conflict. Use transaction ID as the second factor.
    valuesForInsert.sort(Comparator.comparingLong(a -> a.txCommittedAtInMillis));

    // TODO: The following concern wouldn't be needed if the order of INSERT operations is
    //       deterministic. With `repl_version` on the table in the Backup site and deterministic
    //       INSERT operation order, the merge termination at INSERT operation can be removed,
    //       probably. This will improve the performance.

    // Not merge following operations once an insert operation is found.
    // Assuming the following operations are stored in `records.values`:
    // - t1: INSERT(X: prev_tx_id=null, tx_id=v1, value=10)
    // - t2: UPDATE(X: prev_tx_id=v1, tx_id=v2, value=20)
    // (t3: DELETE is delayed)
    // - t4: INSERT(X: prev_tx_id=null, tx_id=v4, value=40)
    // `cur_tx_id` is null and merged t1 and t2 are written to the secondary database.
    // At this point, `prep_tx_id` is set to t2. But a next thread doesn't know which insert out
    // of t1 and t3 should be handled to reach t2.
    boolean suspendFollowingOperation = false;
    Value lastValue = null;
    boolean deleted = record.deleted;
    // TODO: Revisit this since it affects the performance.
    // Set<Column<?>> can not be used since it uses `value` in `equals()` and `hashcode()`.
    Map<String, Column<?>> updatedColumns = new HashMap<>();
    Set<String> insertTxIds = new HashSet<>();
    @Nullable String currentTxId = record.currentTxId;
    TransactionTableMetadata tableMetadata = null;
    while (!suspendFollowingOperation) {
      Value value;
      if (currentTxId == null || deleted) {
        if (valuesForInsert.isEmpty()) {
          value = null;
        } else {
          value = valuesForInsert.poll();
        }
      } else {
        value = valuesForNonInsert.remove(currentTxId);
      }
      if (value == null) {
        break;
      }

      if (value.type.equals("insert")) {
        /*
        // Now all columns should be null.
        if (!updatedColumns.isEmpty()) {
          throw new IllegalStateException(
              String.format(
                  "`updatedColumns` should be empty. key:%s, value:%s, updatedColumns:%s",
                  key, value, updatedColumns));
        }
         */
        if (record.insertTxIds.contains(value.txId)) {
          logger.warn(
              "This insert will be skipped since txId:{} is already handled. key:{}",
              value.txId,
              key);
          continue;
        }
        for (Column<?> column : value.columns) {
          updatedColumns.put(column.name, column);
        }
        insertTxIds.add(value.txId);
        // TODO: [Optimization] Should check the rest of the values.
        suspendFollowingOperation = true;
        deleted = false;
      } else if (value.type.equals("update")) {
        for (Column<?> column : value.columns) {
          updatedColumns.put(column.name, column);
        }
        deleted = false;
      } else if (value.type.equals("delete")) {
        // Delete operations are not so common ones. Lazy instantiation would be efficient.
        if (tableMetadata == null) {
          tableMetadata =
              tableMetadataManager.getTransactionTableMetadata(key.namespace, key.table);
        }
        // Explicit null clear is needed.
        updatedColumns.clear();
        for (String columnName : tableMetadata.getColumnNames()) {
          if (!tableMetadata.getTransactionMetaColumnNames().contains(columnName)
              && !tableMetadata.getPrimaryKeyColumnNames().contains(columnName)
              && !tableMetadata.getClusteringKeyNames().contains(columnName)
              && !tableMetadata.getSecondaryIndexNames().contains(columnName)) {
            DataType dataType = tableMetadata.getColumnDataType(columnName);
            updatedColumns.put(columnName, new Column<>(columnName, null, dataType));
          }
        }
        deleted = true;
      } else {
        throw new AssertionError();
      }
      currentTxId = value.txId;

      lastValue = value;
    }

    if (lastValue == null) {
      return null;
    }

    Set<Value> restValues = new HashSet<>(valuesForNonInsert.values());
    restValues.addAll(valuesForInsert);

    return new NextValue(
        lastValue,
        deleted,
        restValues,
        updatedColumns,
        insertTxIds,
        suspendFollowingOperation && !restValues.isEmpty());
  }

  @VisibleForTesting
  ResultOfKeyHandling handleRecord(Record record, boolean logicalDelete) throws ExecutionException {
    // TODO: Garbage collect too old values.
    NextValue nextValue = findNextValue(record.key, record);

    if (nextValue == null) {
      logger.debug("A next value is not found. Record:{}", record.toStringOnlyWithMetadata());
      return new ResultOfKeyHandling(!record.values.isEmpty(), false);
    }
    logger.debug(
        "[handleRecord]\n  Record:{}\n  NextValue:{}\n",
        record.toStringOnlyWithMetadata(),
        nextValue.toStringOnlyWithMetadata());

    Value lastValue = nextValue.nextValue;

    Buildable putBuilder =
        Put.newBuilder()
            .namespace(record.key.namespace)
            .table(record.key.table)
            .partitionKey(
                com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Key
                    .toScalarDbKey(record.key.pk));
    if (!record.key.ck.columns.isEmpty()) {
      putBuilder.clusteringKey(
          com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Key
              .toScalarDbKey(record.key.ck));
    }

    // FIXME: `before_tx_version` must not be used in production to avoid confusion. Prepare a
    //         proper new column which has bigint type.
    String versionColForConditionUpdate = "before_tx_prepared_at";
    putBuilder.bigIntValue(versionColForConditionUpdate, record.nextVersion());

    putBuilder.textValue("tx_id", lastValue.txId);
    putBuilder.intValue("tx_version", lastValue.txVersion);
    putBuilder.bigIntValue("tx_prepared_at", lastValue.txPreparedAtInMillis);
    putBuilder.bigIntValue("tx_committed_at", lastValue.txCommittedAtInMillis);

    MutationCondition mutationCondition;
    if (record.currentTxId == null) {
      // The first insert
      mutationCondition = ConditionBuilder.putIfNotExists();
    } else {
      // TODO: This should contain `putIfExists`?
      mutationCondition =
          ConditionBuilder.putIf(
                  ConditionBuilder.buildConditionalExpression(
                      BigIntColumn.of(versionColForConditionUpdate, record.nextVersion()),
                      Operator.LTE))
              .build();
    }
    putBuilder.condition(mutationCondition);

    if (lastValue.type.equals("delete")) {
      if (logicalDelete) {
        // Physical delete causes some issues when there are any following INSERT.
        // TODO: Logically deleted records will be removed by lazy recovery.
        //       But, a cleanup worker may be needed in the Semi-Sync Replication itself.
        putBuilder.intValue("tx_state", TransactionState.DELETED.get());
      } else {
        // FIXME for usecase of write heavy logging-ish situation.
        throw new AssertionError();
      }
    } else {
      putBuilder.intValue("tx_state", TransactionState.COMMITTED.get());
      for (Column<?> column : nextValue.updatedColumns.values()) {
        putBuilder.value(Column.toScalarDbColumn(column));
      }
    }

    try {
      backupScalarDbStorage.put(putBuilder.build());
    } catch (NoMutationException e) {
      // It's possible another thread inserted a record to the backup DB table.
      Put retryPut =
          Put.newBuilder(putBuilder.build())
              .condition(
                  ConditionBuilder.putIf(
                          ConditionBuilder.buildConditionalExpression(
                              BigIntColumn.of(versionColForConditionUpdate, record.nextVersion()),
                              Operator.LTE))
                      .build())
              .build();
      backupScalarDbStorage.put(retryPut);
    }

    try {
      metricsLogger.execUpdateRecord(
          () -> {
            replicationRecordRepository.updateWithValues(
                record,
                lastValue.txId,
                nextValue.deleted,
                nextValue.restValues,
                nextValue.insertTxIds);
            return null;
          });
      return new ResultOfKeyHandling(
          !nextValue.restValues.isEmpty(), nextValue.shouldHandleTheSameKey);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to update the replication DB `records` table. Record: %s, Next value: %s, Put operator: %s",
              record.toStringOnlyWithMetadata(),
              nextValue.toStringOnlyWithMetadata(),
              convPutOperationMetadataToString(putBuilder.build())),
          e);
    }
  }

  static class ResultOfKeyHandling {
    final boolean remainingValueExists;
    final boolean nextConnectedValueExists;

    ResultOfKeyHandling(boolean remainingValueExists, boolean nextConnectedValueExists) {
      this.remainingValueExists = remainingValueExists;
      this.nextConnectedValueExists = nextConnectedValueExists;
    }
  }
}
