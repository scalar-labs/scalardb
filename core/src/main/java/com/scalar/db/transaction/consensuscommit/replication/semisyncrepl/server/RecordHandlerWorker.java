package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
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
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.UpdatedRecord;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationRecordRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationUpdatedRecordRepository;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordHandlerWorker extends BaseHandlerWorker {
  private static final Logger logger = LoggerFactory.getLogger(RecordHandlerWorker.class);

  private final Configuration conf;
  private final ReplicationUpdatedRecordRepository replicationUpdatedRecordRepository;
  private final ReplicationRecordRepository replicationRecordRepository;
  private final KeyHandler keyHandler;
  private final MetricsLogger metricsLogger;

  @Immutable
  public static class Configuration extends BaseHandlerWorker.Configuration {
    private final int fetchSize;

    public Configuration(
        int replicationDbPartitionSize, int threadSize, int waitMillisPerPartition, int fetchSize) {
      super(replicationDbPartitionSize, threadSize, waitMillisPerPartition);
      this.fetchSize = fetchSize;
    }
  }

  enum ResultOfHandlingKey {
    NO_VALUES_PROCESSED,
    PARTIAL_VALUES_PROCESSED,
    ALL_VALUES_PROCESSED
  }

  static class KeyHandler {
    private final ReplicationRecordRepository replicationRecordRepository;
    private final DistributedStorage backupScalarDbStorage;
    private final MetricsLogger metricsLogger;

    KeyHandler(
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
      // so on might be good.
      public final Value nextValue;
      public final boolean deleted;
      public final Set<Value> restValues;
      public final Collection<Column<?>> updatedColumns;
      public final Collection<String> insertTxIds;
      public final boolean shouldHandleTheSameKey;

      public NextValue(
          Value nextValue,
          boolean deleted,
          Set<Value> restValues,
          Collection<Column<?>> updatedColumns,
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
      Set<Column<?>> updatedColumns = new HashSet<>();
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
          updatedColumns.addAll(value.columns);
          insertTxIds.add(value.txId);
          // TODO: [Optimization] Should check the rest of the values.
          suspendFollowingOperation = true;
          deleted = false;
        } else if (value.type.equals("update")) {
          updatedColumns.removeAll(value.columns);
          updatedColumns.addAll(value.columns);
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
          suspendFollowingOperation
              && (!valuesForInsert.isEmpty() || !valuesForNonInsert.isEmpty()));
    }

    @VisibleForTesting
    ResultOfHandlingKey handleKey(Key key, boolean logicalDelete) throws ExecutionException {
      Optional<Record> recordOpt =
          metricsLogger.execGetRecord(() -> replicationRecordRepository.get(key));
      if (!recordOpt.isPresent()) {
        logger.warn("key:{} is not found", key);
        return ResultOfHandlingKey.NO_VALUES_PROCESSED;
      }

      Record record = recordOpt.get();

      NextValue nextValue = findNextValue(key, record);

      if (nextValue == null) {
        logger.debug("A next value is not found. key:{}", key);
        return ResultOfHandlingKey.NO_VALUES_PROCESSED;
      }
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
        for (Column<?> column : nextValue.updatedColumns) {
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
          throw new RuntimeException(
              String.format(
                  "Failed to update the secondary DB table. Record: %s, Next value: %s, Put operator: %s, Result: %s",
                  record.toStringOnlyWithMetadata(),
                  nextValue.toStringOnlyWithMetadata(),
                  convPutOperationMetadataToString(putBuilder.build()),
                  convOptResultMetadataToString(result)),
              e);
        }
      }

      try {
        metricsLogger.execUpdateRecord(
            () -> {
              replicationRecordRepository.updateWithValues(
                  key,
                  record,
                  lastValue.txId,
                  nextValue.deleted,
                  nextValue.restValues,
                  nextValue.insertTxIds);
              return null;
            });
        if (nextValue.shouldHandleTheSameKey) {
          return ResultOfHandlingKey.PARTIAL_VALUES_PROCESSED;
        } else {
          return ResultOfHandlingKey.ALL_VALUES_PROCESSED;
        }
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

    private String convPutOperationMetadataToString(Put put) {
      return MoreObjects.toStringHelper(put)
          .add("namespace", put.forNamespace())
          .add("table", put.forTable())
          .add("partitionKey", put.getPartitionKey())
          .add("clusteringKey", put.getClusteringKey())
          .add(
              "columns",
              ImmutableMap.builder()
                  .put("tx_id", put.getColumns().get("tx_id"))
                  .put("tx_version", put.getColumns().get("tx_version"))
                  .put("tx_state", put.getColumns().get("tx_state"))
                  .put("tx_prepared_at", put.getColumns().get("tx_prepared_at"))
                  .put("tx_committed_at", put.getColumns().get("tx_committed_at"))
                  .build())
          .add("consistency", put.getConsistency())
          .add("condition", put.getCondition())
          .toString();
    }

    private String convOptResultMetadataToString(Optional<Result> optResult) {
      return optResult.map(this::convResultMetadataToString).orElse("None");
    }

    private String convResultMetadataToString(Result result) {
      return MoreObjects.toStringHelper(result)
          .add(
              "columns",
              ImmutableMap.builder()
                  .put("tx_id", result.getColumns().get("tx_id"))
                  .put("tx_version", result.getColumns().get("tx_version"))
                  .put("tx_state", result.getColumns().get("tx_state"))
                  .put("tx_prepared_at", result.getColumns().get("tx_prepared_at"))
                  .put("tx_committed_at", result.getColumns().get("tx_committed_at"))
                  .build())
          .toString();
    }
  }

  public RecordHandlerWorker(
      Configuration conf,
      ReplicationUpdatedRecordRepository replicationUpdatedRecordRepository,
      ReplicationRecordRepository replicationRecordRepository,
      DistributedStorage backupScalarDbStorage,
      MetricsLogger metricsLogger) {
    super(conf, "record", metricsLogger);
    this.conf = conf;
    this.keyHandler =
        new KeyHandler(replicationRecordRepository, backupScalarDbStorage, metricsLogger);
    this.replicationUpdatedRecordRepository = replicationUpdatedRecordRepository;
    this.replicationRecordRepository = replicationRecordRepository;
    this.metricsLogger = metricsLogger;
  }

  @Override
  protected boolean handle(int partitionId) throws ExecutionException {
    List<UpdatedRecord> scannedUpdatedRecords =
        metricsLogger.execFetchUpdatedRecords(
            () -> replicationUpdatedRecordRepository.scan(partitionId, conf.fetchSize));
    for (UpdatedRecord updatedRecord : scannedUpdatedRecords) {
      ResultOfHandlingKey result =
          keyHandler.handleKey(
              replicationRecordRepository.createKey(
                  updatedRecord.namespace, updatedRecord.table, updatedRecord.pk, updatedRecord.ck),
              true);
      switch (result) {
        case NO_VALUES_PROCESSED:
          break;
        case ALL_VALUES_PROCESSED:
          // TODO: Measure the latency.
          replicationUpdatedRecordRepository.delete(updatedRecord);
          break;
        case PARTIAL_VALUES_PROCESSED:
          // TODO: Measure the latency.
          replicationUpdatedRecordRepository.delete(updatedRecord);
          break;
      }
    }

    // TODO: Consider all the results from `handleKey()`.
    return scannedUpdatedRecords.size() >= conf.fetchSize;
  }
}
