package com.scalar.db.transaction.consensuscommit.replication.server;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.scalar.db.api.Result;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.replication.model.Record.Value;
import com.scalar.db.transaction.consensuscommit.replication.repository.CoordinatorStateRepository;
import com.scalar.db.transaction.consensuscommit.replication.repository.ReplicationRecordRepository;
import com.scalar.db.transaction.consensuscommit.replication.repository.ReplicationTransactionRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisync.model.DeletedTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisync.model.InsertedTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisync.model.UpdatedTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisync.model.WrittenTuple;
import java.io.Closeable;
import java.io.IOException;
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
  private final int replicationDbPartitionSize;
  private final int threadSize;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final TypeReference<List<WrittenTuple>> typeRefForWrittenTupleInTransactions =
      new TypeReference<List<WrittenTuple>>() {};
  private final ReplicationRecordRepository replicationRecordRepository;
  private final ReplicationTransactionRepository replicationTransactionRepository;
  private final CoordinatorStateRepository coordinatorStateRepository;
  private final Queue<Key> recordWriterQueue;

  public DistributorThread(
      int replicationDbPartitionSize,
      int threadSize,
      CoordinatorStateRepository coordinatorStateRepository,
      ReplicationTransactionRepository replicationTransactionRepository,
      ReplicationRecordRepository replicationRecordRepository,
      Queue<Key> recordWriterQueue) {
    if (replicationDbPartitionSize % threadSize != 0) {
      throw new IllegalArgumentException(
          String.format(
              "`replicationDbPartitionSize`(%d) should be a multiple of `replicationDbThreadSize`(%d)",
              replicationDbPartitionSize, threadSize));
    }
    this.replicationDbPartitionSize = replicationDbPartitionSize;
    this.threadSize = threadSize;
    this.executorService =
        Executors.newFixedThreadPool(
            threadSize, new ThreadFactoryBuilder().setNameFormat("log-distributor-%d").build());
    this.replicationTransactionRepository = replicationTransactionRepository;
    this.coordinatorStateRepository = coordinatorStateRepository;
    this.replicationRecordRepository = replicationRecordRepository;
    this.recordWriterQueue = recordWriterQueue;
  }

  private void fetchAndHandleTransactionRecord(int partitionId)
      throws IOException, ExecutionException {
    for (Result result : replicationTransactionRepository.scan(partitionId)) {
      String transactionId = result.getText("transaction_id");
      if (!coordinatorStateRepository.isCommitted(transactionId)) {
        continue;
      }
      long createdAt = result.getBigInt("created_at");
      String writeSet = result.getText("write_set");
      List<WrittenTuple> writtenTuples =
          objectMapper.readValue(writeSet, typeRefForWrittenTupleInTransactions);
      for (WrittenTuple writtenTuple : writtenTuples) {
        Key key =
            replicationRecordRepository.createKey(
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

        replicationRecordRepository.appendValue(key, newValue);
        recordWriterQueue.add(key);

        replicationTransactionRepository.delete(partitionId, createdAt, transactionId);
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
                try {
                  fetchAndHandleTransactionRecord(partitionId);
                } catch (Throwable e) {
                  // TODO: Remove this
                  e.printStackTrace();
                  logger.error("Unexpected exception occurred", e);
                  try {
                    TimeUnit.SECONDS.sleep(2);
                  } catch (InterruptedException ex) {
                    logger.error("Interrupted", ex);
                    Thread.currentThread().interrupt();
                    break;
                  }
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

  public static void main(String[] args) {
    // FIXME: This is only for PoC.
    Properties replicationDbProps = new Properties();
    replicationDbProps.put("scalar.db.storage", "jdbc");
    replicationDbProps.put("scalar.db.contact_points", "jdbc:mysql://localhost/replication");
    replicationDbProps.put("scalar.db.username", "root");
    replicationDbProps.put("scalar.db.password", "mysql");

    Properties coordinatorDbProps = new Properties();
    coordinatorDbProps.put("scalar.db.storage", "jdbc");
    coordinatorDbProps.put("scalar.db.contact_points", "jdbc:postgresql://localhost/scalardb");
    coordinatorDbProps.put("scalar.db.username", "postgres");
    coordinatorDbProps.put("scalar.db.password", "postgres");

    Properties backupScalarDbProps = new Properties();
    backupScalarDbProps.put("scalar.db.storage", "jdbc");
    backupScalarDbProps.put("scalar.db.contact_points", "jdbc:mysql://localhost/sample");
    backupScalarDbProps.put("scalar.db.username", "root");
    backupScalarDbProps.put("scalar.db.password", "mysql");

    ObjectMapper objectMapper = new ObjectMapper();

    CoordinatorStateRepository coordinatorStateRepository =
        new CoordinatorStateRepository(
            StorageFactory.create(coordinatorDbProps).getStorage(), "coordinator", "state");

    ReplicationTransactionRepository replicationTransactionRepository =
        new ReplicationTransactionRepository(
            StorageFactory.create(replicationDbProps).getStorage(),
            objectMapper,
            "replication",
            "transactions",
            4);

    ReplicationRecordRepository replicationRecordRepository =
        new ReplicationRecordRepository(
            StorageFactory.create(replicationDbProps).getStorage(),
            objectMapper,
            "replication",
            "records");

    RecordWriterThread recordWriter =
        new RecordWriterThread(8, replicationRecordRepository, backupScalarDbProps).run();

    DistributorThread distributorThread =
        new DistributorThread(
                256,
                8,
                coordinatorStateRepository,
                replicationTransactionRepository,
                replicationRecordRepository,
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
