package com.scalar.db.transaction.consensuscommit.replication.server;

import com.google.common.collect.Streams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteBuilder;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder.Buildable;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.replication.model.Column;
import com.scalar.db.transaction.consensuscommit.replication.model.Record;
import com.scalar.db.transaction.consensuscommit.replication.model.Record.Value;
import com.scalar.db.transaction.consensuscommit.replication.repository.ReplicationRecordRepository;
import java.io.Closeable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordWriterThread implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(ReplicationRecordRepository.class);

  private final ExecutorService executorService;
  private final int threadSize;
  private final DistributedStorage backupScalarDbStorage;
  private final BlockingQueue<Key> queue = new LinkedBlockingQueue<>();
  private final ReplicationRecordRepository replicationRecordRepository;

  public RecordWriterThread(
      int threadSize,
      ReplicationRecordRepository replicationRecordRepository,
      Properties backupScalarDbProperties) {
    this.threadSize = threadSize;
    this.executorService =
        Executors.newFixedThreadPool(
            threadSize, new ThreadFactoryBuilder().setNameFormat("log-record-writer-%d").build());
    this.backupScalarDbStorage = StorageFactory.create(backupScalarDbProperties).getStorage();
    this.replicationRecordRepository = replicationRecordRepository;
  }

  private void handleKey(Key key) throws ExecutionException {
    Optional<Record> recordOpt = replicationRecordRepository.get(key);
    if (!recordOpt.isPresent()) {
      logger.warn("key:{} is not found", key);
      return;
    }

    Record record = recordOpt.get();

    Queue<Value> valuesForInsert = new LinkedList<>();
    Map<String, Value> valuesForNonInsert = new HashMap<>();
    for (Value value : record.values()) {
      if (value.prevTxId == null) {
        valuesForInsert.add(value);
      } else {
        valuesForNonInsert.put(value.prevTxId, value);
      }
    }

    Value lastValue = null;
    Set<Column<?>> updatedColumns = new HashSet<>();
    @Nullable String currentTxId = record.currentTxId();
    while (true) {
      Value value;
      if (currentTxId == null) {
        value = valuesForInsert.poll();
      } else {
        value = valuesForNonInsert.remove(currentTxId);
      }
      if (value == null) {
        break;
      }

      if (value.type.equals("insert")) {
        assert updatedColumns.isEmpty();
        updatedColumns.addAll(value.columns);
        currentTxId = value.txId;
      } else if (value.type.equals("update")) {
        updatedColumns.addAll(value.columns);
        currentTxId = value.txId;
      } else if (value.type.equals("delete")) {
        updatedColumns.clear();
        currentTxId = null;
      } else {
        throw new AssertionError();
      }

      lastValue = value;
    }

    if (lastValue == null) {
      logger.warn("`values` in `records` table is empty. key:{}", key);
      return;
    }

    if (lastValue.type.equals("delete")) {
      DeleteBuilder.Buildable deleteBuilder =
          Delete.newBuilder()
              .namespace(record.namespace())
              .table(record.table())
              .partitionKey(
                  com.scalar.db.transaction.consensuscommit.replication.model.Key.toScalarDbKey(
                      record.pk()));
      if (!record.ck().columns.isEmpty()) {
        deleteBuilder.clusteringKey(
            com.scalar.db.transaction.consensuscommit.replication.model.Key.toScalarDbKey(
                record.ck()));
      }
      // TODO: Consider partial commit issues
      backupScalarDbStorage.delete(deleteBuilder.build());
    } else {
      Buildable pubBuilder =
          Put.newBuilder()
              .namespace(record.namespace())
              .table(record.table())
              .partitionKey(
                  com.scalar.db.transaction.consensuscommit.replication.model.Key.toScalarDbKey(
                      record.pk()));
      if (!record.ck().columns.isEmpty()) {
        pubBuilder.clusteringKey(
            com.scalar.db.transaction.consensuscommit.replication.model.Key.toScalarDbKey(
                record.ck()));
      }
      for (Column<?> column : updatedColumns) {
        pubBuilder.value(Column.toScalarDbColumn(column));
      }

      // TODO: Consider partial commit issues
      // FIXME: Support `tx_id | tx_state | tx_version | tx_prepared_at | tx_committed_at`
      backupScalarDbStorage.put(pubBuilder.build());
    }

    replicationRecordRepository.updateValues(
        key,
        record.currentTxId(),
        lastValue.txId,
        Streams.concat(valuesForInsert.stream(), valuesForNonInsert.values().stream())
            .collect(Collectors.toSet()));
  }

  public BlockingQueue<Key> queue() {
    return queue;
  }

  public RecordWriterThread run() {
    for (int i = 0; i < threadSize; i++) {
      executorService.execute(
          () -> {
            while (!executorService.isShutdown()) {
              Key key;
              try {
                key = queue.poll(500, TimeUnit.MILLISECONDS);
                if (key == null) {
                  continue;
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted", e);
                break;
              }

              try {
                handleKey(key);
              } catch (Throwable e) {
                queue.add(key);
                // TODO: Remove this
                e.printStackTrace();
                logger.error("Caught an exception. key:{}. Retrying...", key, e);
                try {
                  // Avoid busy loop
                  TimeUnit.MILLISECONDS.sleep(500);
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
