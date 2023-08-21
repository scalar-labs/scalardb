package com.scalar.db.transaction.consensuscommit.replication.server;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scanner;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.replication.model.Record.Value;
import com.scalar.db.transaction.consensuscommit.replication.repository.RecordRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisync.model.DeletedTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisync.model.InsertedTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisync.model.UpdatedTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisync.model.WrittenTuple;
import java.io.Closeable;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributorThread implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(DistributorThread.class);

  private final ExecutorService executorService;
  private final String replicationDbNamespace;
  private final String replicationDbTransactionsTable;
  private final int replicationDbPartitionSize;
  private final int threadSize;
  private final int fetchTransactionSize;
  private final DistributedStorage storage;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final TypeReference<List<WrittenTuple>> typeRefForWrittenTupleInTransactions =
      new TypeReference<List<WrittenTuple>>() {};
  private final RecordRepository recordRepository;
  private final Queue<Key> recordWriterQueue;

  public DistributorThread(
      String replicationDbNamespace,
      String replicationDbTransactionsTable,
      String replicationDbRecordsTable,
      int replicationDbPartitionSize,
      int threadSize,
      int fetchTransactionSize,
      Properties replicationDbProperties,
      Queue<Key> recordWriterQueue) {
    if (replicationDbPartitionSize % threadSize != 0) {
      throw new IllegalArgumentException(
          String.format(
              "`replicationDbPartitionSize`(%d) should be a multiple of `replicationDbThreadSize`(%d)",
              replicationDbPartitionSize, threadSize));
    }
    this.replicationDbNamespace = replicationDbNamespace;
    this.replicationDbTransactionsTable = replicationDbTransactionsTable;
    this.replicationDbPartitionSize = replicationDbPartitionSize;
    this.threadSize = threadSize;
    this.fetchTransactionSize = fetchTransactionSize;
    this.executorService =
        Executors.newFixedThreadPool(
            threadSize, new ThreadFactoryBuilder().setNameFormat("log-distributor-%d").build());
    this.storage = StorageFactory.create(replicationDbProperties).getStorage();
    this.recordRepository =
        new RecordRepository(
            storage, objectMapper, replicationDbNamespace, replicationDbRecordsTable);
    this.recordWriterQueue = recordWriterQueue;
  }

  private void fetchAndHandleTransactionRecord(int partitionId) {
    try (Scanner scan =
        storage.scan(
            Scan.newBuilder()
                .namespace(replicationDbNamespace)
                .table(replicationDbTransactionsTable)
                .partitionKey(Key.ofInt("partition_id", partitionId))
                .ordering(Ordering.asc("created_at"))
                .limit(fetchTransactionSize)
                .build())) {
      for (Result result : scan.all()) {
        String transactionId = result.getText("transaction_id");
        long createdAt = result.getBigInt("created_at");
        String writeSet = result.getText("write_set");
        List<WrittenTuple> writtenTuples =
            objectMapper.readValue(writeSet, typeRefForWrittenTupleInTransactions);
        for (WrittenTuple writtenTuple : writtenTuples) {
          Key key =
              recordRepository.createKey(
                  writtenTuple.namespace,
                  writtenTuple.table,
                  writtenTuple.partitionKey,
                  writtenTuple.clusteringKey);

          Value newValue;
          if (writtenTuple instanceof InsertedTuple) {
            InsertedTuple tuple = (InsertedTuple) writtenTuple;
            newValue = new Value(null, transactionId, "insert", tuple.columns);
          } else if (writtenTuple instanceof UpdatedTuple) {
            UpdatedTuple tuple = (UpdatedTuple) writtenTuple;
            newValue = new Value(tuple.prevTxId, transactionId, "update", tuple.columns);
          } else if (writtenTuple instanceof DeletedTuple) {
            DeletedTuple tuple = (DeletedTuple) writtenTuple;
            newValue = new Value(tuple.prevTxId, transactionId, "delete", null);
          } else {
            throw new AssertionError();
          }

          recordRepository.appendValue(key, newValue);
          recordWriterQueue.add(key);

          storage.delete(
              Delete.newBuilder()
                  .namespace(replicationDbNamespace)
                  .table(replicationDbTransactionsTable)
                  .partitionKey(Key.ofInt("partition_id", partitionId))
                  .clusteringKey(
                      Key.newBuilder()
                          .addBigInt("created_at", createdAt)
                          .addText("transaction_id", transactionId)
                          .build())
                  .build());
        }
      }
    } catch (Throwable e) {
      // TODO: Remove this
      e.printStackTrace();
      logger.error("Unexpected exception occurred", e);
      try {
        TimeUnit.SECONDS.sleep(2);
      } catch (InterruptedException ex) {
        ex.printStackTrace();
        Thread.currentThread().interrupt();
      }
    }
  }

  public DistributorThread run() {
    for (int i = 0; i < threadSize; i++) {
      int startPartitionId = i;
      executorService.execute(
          () -> {
            while (!executorService.isShutdown()) {
              for (int partitionId = startPartitionId;
                  partitionId < replicationDbPartitionSize;
                  partitionId += threadSize) {
                fetchAndHandleTransactionRecord(partitionId);
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
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) {
    // FIXME: This is only for PoC.
    Properties replicationDbProps = new Properties();
    replicationDbProps.put("scalar.db.storage", "jdbc");
    replicationDbProps.put("scalar.db.contact_points", "jdbc:mysql://localhost/replication");
    replicationDbProps.put("scalar.db.username", "root");
    replicationDbProps.put("scalar.db.password", "mysql");

    Properties scalarDbProps = new Properties();
    scalarDbProps.put("scalar.db.storage", "jdbc");
    scalarDbProps.put("scalar.db.contact_points", "jdbc:mysql://localhost/sample");
    scalarDbProps.put("scalar.db.username", "root");
    scalarDbProps.put("scalar.db.password", "mysql");

    RecordWriterThread recordWriter =
        new RecordWriterThread("replication", "records", 8, replicationDbProps, scalarDbProps)
            .run();

    DistributorThread distributorThread =
        new DistributorThread(
                "replication",
                "transactions",
                "records",
                256,
                8,
                8,
                replicationDbProps,
                recordWriter.queue())
            .run();

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  distributorThread.close();
                  recordWriter.close();
                }));
  }
}
