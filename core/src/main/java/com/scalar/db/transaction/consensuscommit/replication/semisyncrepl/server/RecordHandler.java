package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import static com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.Utils.convOptBackupDbTableResultMetadataToString;
import static com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.Utils.convPutOperationMetadataToString;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Streams;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.GetBuilder.BuildableGet;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder.Buildable;
import com.scalar.db.api.Result;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Column;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Record;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Record.Value;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationRecordRepository;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RecordHandler {
  private static final Logger logger = LoggerFactory.getLogger(RecordHandler.class);
  private final ReplicationRecordRepository replicationRecordRepository;
  private final DistributedStorage backupScalarDbStorage;
  private final MetricsLogger metricsLogger;

  RecordHandler(
      ReplicationRecordRepository replicationRecordRepository,
      DistributedStorage backupScalarDbStorage,
      MetricsLogger metricsLogger) {
    this.replicationRecordRepository = replicationRecordRepository;
    this.backupScalarDbStorage = backupScalarDbStorage;
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
          .add(
              "restValues",
              "["
                  + restValues.stream()
                      .map(Value::toStringOnlyWithMetadata)
                      .collect(Collectors.joining(","))
                  + "]")
          // .add("updatedColumns", updatedColumns)
          .add("insertTxIds", insertTxIds)
          .add("shouldHandleTheSameKey", shouldHandleTheSameKey)
          .toString();
    }
  }

  static class ResultOfKeyHandling {
    final Long currentRecordVersion;
    final boolean remainingValueExists;
    final boolean nextConnectedValueExists;

    ResultOfKeyHandling(
        Long currentRecordVersion, boolean remainingValueExists, boolean nextConnectedValueExists) {
      this.currentRecordVersion = currentRecordVersion;
      this.remainingValueExists = remainingValueExists;
      this.nextConnectedValueExists = nextConnectedValueExists;
    }
  }

  @VisibleForTesting
  @Nullable
  NextValue findNextValue(Key key, Record record) {
    Queue<Value> valuesForInsert = new ArrayDeque<>();
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
    // TODO: Sort valuesForInsert just in case

    // Not merge following operations once an insert operation is found.
    // Assuming the following operations are stored in `records.values`:
    // - t1: INSERT(X: prev_tx_id=null, tx_id=v1, value=10)
    // - t2: UPDATE(X: prev_tx_id=v1, tx_id=v2, value=20)
    // (t3: DELETE is delayed)
    // - t4: INSERT(X: prev_tx_id=null, tx_id=v4, value=40)
    // `cur_tx_id` is null and merged t1 and t2 are written to the secondary database.
    // At this point, `prep_tx_id` is set to t2. But a next thread doesn't know which insert out
    // of
    // t1 and t3 should be handled to reach t2.
    boolean suspendFollowingOperation = false;
    Value lastValue = null;
    boolean deleted = record.deleted;
    // Set<Column<?>> can not be used since it uses `value` in `equals()` and `hashcode()`.
    Map<String, Column<?>> updatedColumns = new HashMap<>();
    Set<String> insertTxIds = new HashSet<>();
    @Nullable String currentTxId = record.currentTxId;
    while (!suspendFollowingOperation) {
      Value value = null;
      if (currentTxId == null || deleted) {
        if (record.prepTxId == null) {
          value = valuesForInsert.poll();
        } else {
          Iterator<Value> iterator = valuesForInsert.iterator();
          while (iterator.hasNext()) {
            Value v = iterator.next();
            if (v.txId.equals(record.prepTxId)) {
              // The previous attempt used this transaction ID. It must be used this time, too.
              value = v;
              iterator.remove();
            }
          }
          if (value == null) {
            throw new AssertionError(
                String.format(
                    "`tx_prep_id` is set, but it doesn't exist in INSERT operations. Prepared Tx ID: %s",
                    record.prepTxId));
          }
        }
      } else {
        value = valuesForNonInsert.remove(currentTxId);
      }
      if (value == null) {
        break;
      }

      if (value.type.equals("insert")) {
        if (!updatedColumns.isEmpty()) {
          throw new IllegalStateException(
              String.format(
                  "`updatedColumns` should be empty. key:%s, value:%s, updatedColumns:%s",
                  key, value, updatedColumns));
        }
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
        updatedColumns.clear();
        deleted = true;
      } else {
        throw new AssertionError();
      }
      currentTxId = value.txId;

      lastValue = value;

      if (lastValue.txId.equals(record.prepTxId)) {
        logger.debug(
            "The version chains reach prepTxId:{}. The number of remaining versions is {}",
            record.prepTxId,
            valuesForInsert.size() + valuesForNonInsert.size());
        suspendFollowingOperation = true;
      }
    }

    if (lastValue == null) {
      return null;
    }

    return new NextValue(
        lastValue,
        deleted,
        Streams.concat(valuesForInsert.stream(), valuesForNonInsert.values().stream())
            .collect(Collectors.toSet()),
        updatedColumns,
        insertTxIds,
        suspendFollowingOperation && (!valuesForInsert.isEmpty() || !valuesForNonInsert.isEmpty()));
  }

  @VisibleForTesting
  ResultOfKeyHandling handleKey(Key key, boolean logicalDelete) throws ExecutionException {
    Optional<Record> recordOpt =
        metricsLogger.execGetRecord(() -> replicationRecordRepository.get(key));
    if (!recordOpt.isPresent()) {
      logger.warn("Key:{} is not found", key);
      return new ResultOfKeyHandling(null, false, false);
    }

    Record record = recordOpt.get();

    // TODO: Garbage collect too old values.
    NextValue nextValue = findNextValue(key, record);

    if (nextValue == null) {
      logger.debug(
          "A next value is not found. Key:{}, Record:{}", key, record.toStringOnlyWithMetadata());
      return new ResultOfKeyHandling(record.version, !record.values.isEmpty(), false);
    }
    logger.debug(
        "[handleKey]\n  Key:{}\n  NextValue:{}\n", key, nextValue.toStringOnlyWithMetadata());

    Value lastValue = nextValue.nextValue;

    if (record.prepTxId == null) {
      // Write down the target transaction ID to let conflict transactions on the same page.
      metricsLogger.execSetPrepTxIdInRecord(
          () -> {
            replicationRecordRepository.updateWithPrepTxId(key, record, lastValue.txId);
            return null;
          });
    }

    Buildable putBuilder =
        Put.newBuilder()
            .namespace(record.namespace)
            .table(record.table)
            .partitionKey(
                com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Key
                    .toScalarDbKey(record.pk));
    if (!record.ck.columns.isEmpty()) {
      putBuilder.clusteringKey(
          com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Key
              .toScalarDbKey(record.ck));
    }
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
                      TextColumn.of("tx_id", record.currentTxId), Operator.EQ))
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
      BuildableGet getBuilder =
          Get.newBuilder()
              .namespace(record.namespace)
              .table(record.table)
              .partitionKey(
                  com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Key
                      .toScalarDbKey(record.pk));
      if (!record.ck.columns.isEmpty()) {
        getBuilder.clusteringKey(
            com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Key
                .toScalarDbKey(record.ck));
      }
      Optional<Result> result = backupScalarDbStorage.get(getBuilder.build());
      if (result.isPresent() && result.get().getText("tx_id").equals(lastValue.txId)) {
        // The backup DB table is already updated.
      } else {
        // TODO: Revisit this exception type.
        throw new RuntimeException(
            String.format(
                "Failed to update the secondary DB table. Record: %s, Next value: %s, Put operator: %s, Result: %s",
                record.toStringOnlyWithMetadata(),
                nextValue.toStringOnlyWithMetadata(),
                convPutOperationMetadataToString(putBuilder.build()),
                convOptBackupDbTableResultMetadataToString(result)),
            e);
      }
    }

    try {
      long newVersion =
          metricsLogger.execUpdateRecord(
              () ->
                  replicationRecordRepository.updateWithValues(
                      key,
                      record,
                      lastValue.txId,
                      nextValue.deleted,
                      nextValue.restValues,
                      nextValue.insertTxIds));
      return new ResultOfKeyHandling(
          newVersion, !nextValue.restValues.isEmpty(), nextValue.shouldHandleTheSameKey);
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

  /** @return true if handling the key has finished, false otherwise. */
  boolean handleKey(Key key) throws ExecutionException {
    ResultOfKeyHandling result = handleKey(key, true);

    if (result.currentRecordVersion == null) {
      // The record doesn't exist yet. It's possible that only the notification was handled before
      // writing the record. Therefore, a retry is needed. The notification should be reused and
      // kept.
      return false;
    } else if (result.nextConnectedValueExists) {
      // There are connected values to be handled immediately. The notification should be reused and
      // kept.
      return false;
    }

    return true;
  }
}
