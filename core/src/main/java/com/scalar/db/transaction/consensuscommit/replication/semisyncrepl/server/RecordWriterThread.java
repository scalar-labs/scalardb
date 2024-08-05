package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordWriterThread implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(RecordWriterThread.class);

  private final ExecutorService executorService;
  private final int threadSize;
  private final DistributedStorage backupScalarDbStorage;
  private final ReplicationRecordRepository replicationRecordRepository;
  private final BlockingQueue<Key> recordWriterQueue;
  private final MetricsLogger metricsLogger;

  public RecordWriterThread(
      int threadSize,
      ReplicationRecordRepository replicationRecordRepository,
      DistributedStorage backupScalarDbStorage,
      BlockingQueue<Key> recordWriterQueue,
      MetricsLogger metricsLogger) {
    this.threadSize = threadSize;
    this.executorService =
        Executors.newFixedThreadPool(
            threadSize,
            new ThreadFactoryBuilder()
                .setNameFormat("log-record-writer-%d")
                .setUncaughtExceptionHandler(
                    (thread, e) -> logger.error("Got an uncaught exception. thread:{}", thread, e))
                .build());
    this.backupScalarDbStorage = backupScalarDbStorage;
    this.replicationRecordRepository = replicationRecordRepository;
    this.recordWriterQueue = recordWriterQueue;
    this.metricsLogger = metricsLogger;
  }

  @Immutable
  static class NextValue {
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
    // At this point, `prep_tx_id` is set to t2. But a next thread doesn't know which insert out of
    // t1 and t3 should be handled to reach t2.
    boolean suspendFollowingOperation = false;
    Value lastValue = null;
    boolean deleted = false;
    Set<Column<?>> updatedColumns = new HashSet<>();
    Set<String> insertTxIds = new HashSet<>();
    @Nullable String currentTxId = record.currentTxId;
    while (!suspendFollowingOperation) {
      Value value;
      if (currentTxId == null || deleted) {
        value = valuesForInsert.poll();
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
      /*
      if (lastValue.txId.equals(record.prepTxId)) {
        logger.debug(
            "The version chains reach prepTxId:{}. The number of remaining versions is {}",
            record.prepTxId,
            valuesForInsert.size() + valuesForNonInsert.size());
        break;
      }
       */
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
        suspendFollowingOperation);
  }

  private boolean handleKey(Key key, boolean logicalDelete) throws ExecutionException {
    Optional<Record> recordOpt =
        metricsLogger.execGetRecord(() -> replicationRecordRepository.get(key));
    if (!recordOpt.isPresent()) {
      logger.warn("key:{} is not found", key);
      return false;
    }

    Record record = recordOpt.get();

    NextValue nextValue = findNextValue(key, record);

    if (nextValue == null) {
      logger.debug("A next value is not found. key:{}", key);
      return false;
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
        // TODO: Revisit which exception is better.
        throw e;
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
      return nextValue.shouldHandleTheSameKey;
    } catch (Exception e) {
      String message =
          String.format(
              "Failed to update the values. key:%s, txId:%s, lastValue:%s",
              key, record.currentTxId, lastValue);
      throw new RuntimeException(message, e);
    }
  }

  public RecordWriterThread run() {
    for (int i = 0; i < threadSize; i++) {
      executorService.execute(
          () -> {
            while (!executorService.isShutdown()) {
              Key key;
              try {
                key = recordWriterQueue.poll(500, TimeUnit.MILLISECONDS);
                if (key == null) {
                  continue;
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted", e);
                break;
              }

              try {
                if (handleKey(key, true)) {
                  recordWriterQueue.add(key);
                }
              } catch (Throwable e) {
                recordWriterQueue.add(key);
                logger.error("Caught an exception. Retrying...\n  key:{}", key, e);
                try {
                  // Avoid busy loop
                  TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException ex) {
                  Thread.currentThread().interrupt();
                  logger.warn("Interrupted", ex);
                  break;
                }
              }
            }
          });
    }
    return this;
  }

  @Override
  public void close() {
    executorService.shutdown();
    // TODO: Make this configurable
    try {
      if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.warn("Interrupted", e);
    }
  }
}
