package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Transaction;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.UpdatedRecord;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.CoordinatorStateRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationBulkTransactionRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationRecordRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationTransactionRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationUpdatedRecordRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server.TransactionQueueConsumer.Configuration;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class LogApplier {
  private static final String ENV_VAR_BACKUP_SCALARDB_CONFIG = "LOG_APPLIER_BACKUP_SCALARDB_CONFIG";
  private static final String ENV_VAR_REPLICATION_CONFIG = "LOG_APPLIER_REPLICATION_CONFIG";
  private static final String ENV_VAR_COORDINATOR_STATE_CONFIG =
      "LOG_APPLIER_COORDINATOR_STATE_CONFIG";
  private static final String ENV_VAR_NUM_OF_BULK_TRANSACTION_HANDLER_THREADS =
      "LOG_APPLIER_NUM_OF_BULK_TRANSACTION_HANDLER_THREADS";
  private static final String ENV_VAR_NUM_OF_TRANSACTION_HANDLER_THREADS =
      "LOG_APPLIER_NUM_OF_TRANSACTION_HANDLER_THREADS";
  private static final String ENV_VAR_NUM_OF_TRANSACTION_QUEUE_CONSUMER_THREADS =
      "LOG_APPLIER_NUM_OF_TRANSACTION_QUEUE_CONSUMER_THREADS";
  private static final String ENV_VAR_NUM_OF_RECORD_HANDLER_THREADS =
      "LOG_APPLIER_NUM_OF_RECORD_HANDLER_THREADS";
  private static final String ENV_VAR_NUM_OF_RECORD_QUEUE_CONSUMER_THREADS =
      "LOG_APPLIER_NUM_OF_RECORD_QUEUE_CONSUMER_THREADS";
  private static final String ENV_VAR_TRANSACTION_FETCH_SIZE = "LOG_APPLIER_TRANSACTION_FETCH_SIZE";
  private static final String ENV_VAR_TRANSACTION_WAIT_MILLIS_PER_PARTITION =
      "LOG_APPLIER_TRANSACTION_WAIT_MILLIS_PER_PARTITION";
  private static final String ENV_VAR_THRESHOLD_MILLIS_FOR_OLD_TRANSACTION =
      "LOG_APPLIER_THRESHOLD_MILLIS_FOR_OLD_TRANSACTION";

  private static final int REPLICATION_DB_BULK_TRANSACTION_PARTITION_SIZE = 256;

  public static void main(String[] args) throws IOException, InterruptedException {
    String backupScalarDbConfigPath = System.getenv(ENV_VAR_BACKUP_SCALARDB_CONFIG);
    if (backupScalarDbConfigPath == null) {
      throw new IllegalArgumentException(
          "Backup ScalarDB config file path isn't specified. key:"
              + ENV_VAR_BACKUP_SCALARDB_CONFIG);
    }

    String replicationDbConfigPath = System.getenv(ENV_VAR_REPLICATION_CONFIG);
    if (replicationDbConfigPath == null) {
      throw new IllegalArgumentException(
          "Replication config file path isn't specified. key:" + ENV_VAR_REPLICATION_CONFIG);
    }

    String coordinatorStateConfigPath = System.getenv(ENV_VAR_COORDINATOR_STATE_CONFIG);
    if (coordinatorStateConfigPath == null) {
      throw new IllegalArgumentException(
          "CoordinatorState config file path isn't specified. key:"
              + ENV_VAR_COORDINATOR_STATE_CONFIG);
    }

    int numOfBulkTransactionHandlerThreads = 8;
    if (System.getenv(ENV_VAR_NUM_OF_BULK_TRANSACTION_HANDLER_THREADS) != null) {
      numOfBulkTransactionHandlerThreads =
          Integer.parseInt(System.getenv(ENV_VAR_NUM_OF_BULK_TRANSACTION_HANDLER_THREADS));
    }

    int numOfTransactionHandlerThreads = 8;
    if (System.getenv(ENV_VAR_NUM_OF_TRANSACTION_HANDLER_THREADS) != null) {
      numOfTransactionHandlerThreads =
          Integer.parseInt(System.getenv(ENV_VAR_NUM_OF_TRANSACTION_HANDLER_THREADS));
    }

    int numOfTransactionQueueConsumerThreads = 16;
    if (System.getenv(ENV_VAR_NUM_OF_TRANSACTION_QUEUE_CONSUMER_THREADS) != null) {
      numOfTransactionQueueConsumerThreads =
          Integer.parseInt(System.getenv(ENV_VAR_NUM_OF_TRANSACTION_QUEUE_CONSUMER_THREADS));
    }

    int numOfRecordHandlerThreads = 8;
    if (System.getenv(ENV_VAR_NUM_OF_RECORD_HANDLER_THREADS) != null) {
      numOfRecordHandlerThreads =
          Integer.parseInt(System.getenv(ENV_VAR_NUM_OF_RECORD_HANDLER_THREADS));
    }

    int numOfRecordQueueConsumerThreads = 16;
    if (System.getenv(ENV_VAR_NUM_OF_RECORD_QUEUE_CONSUMER_THREADS) != null) {
      numOfRecordQueueConsumerThreads =
          Integer.parseInt(System.getenv(ENV_VAR_NUM_OF_RECORD_QUEUE_CONSUMER_THREADS));
    }

    int transactionFetchSize = 16;
    if (System.getenv(ENV_VAR_TRANSACTION_FETCH_SIZE) != null) {
      transactionFetchSize = Integer.parseInt(System.getenv(ENV_VAR_TRANSACTION_FETCH_SIZE));
    }

    int waitMillisPerPartition = 40;
    if (System.getenv(ENV_VAR_TRANSACTION_WAIT_MILLIS_PER_PARTITION) != null) {
      waitMillisPerPartition =
          Integer.parseInt(System.getenv(ENV_VAR_TRANSACTION_WAIT_MILLIS_PER_PARTITION));
    }

    int thresholdMillisForOldTransaction = 2000;
    if (System.getenv(ENV_VAR_THRESHOLD_MILLIS_FOR_OLD_TRANSACTION) != null) {
      thresholdMillisForOldTransaction =
          Integer.parseInt(System.getenv(ENV_VAR_THRESHOLD_MILLIS_FOR_OLD_TRANSACTION));
    }

    ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    CoordinatorStateRepository coordinatorStateRepository =
        new CoordinatorStateRepository(
            StorageFactory.create(coordinatorStateConfigPath).getStorage(), "coordinator", "state");

    ReplicationRecordRepository replicationRecordRepository =
        new ReplicationRecordRepository(
            StorageFactory.create(replicationDbConfigPath).getStorage(),
            objectMapper,
            "replication",
            "records");

    ReplicationUpdatedRecordRepository replicationUpdatedRecordRepository =
        new ReplicationUpdatedRecordRepository(
            StorageFactory.create(replicationDbConfigPath).getStorage(),
            objectMapper,
            "replication",
            "updated_records");

    ReplicationTransactionRepository replicationTransactionRepository =
        new ReplicationTransactionRepository(
            StorageFactory.create(replicationDbConfigPath).getStorage(),
            objectMapper,
            "replication",
            "transactions");

    ReplicationBulkTransactionRepository replicationBulkTransactionRepository =
        new ReplicationBulkTransactionRepository(
            StorageFactory.create(replicationDbConfigPath).getStorage(),
            objectMapper,
            "replication",
            "bulk_transactions");

    List<BlockingQueue<UpdatedRecord>> updatedRecordQueues =
        new ArrayList<>(numOfRecordQueueConsumerThreads);
    for (int i = 0; i < numOfRecordQueueConsumerThreads; i++) {
      updatedRecordQueues.add(new LinkedBlockingQueue<>());
    }

    List<BlockingQueue<Transaction>> transactionQueues =
        new ArrayList<>(numOfTransactionQueueConsumerThreads);
    for (int i = 0; i < numOfTransactionQueueConsumerThreads; i++) {
      transactionQueues.add(new LinkedBlockingQueue<>());
    }

    // FIXME
    MetricsLogger metricsLogger = new MetricsLogger(updatedRecordQueues.get(0));

    Properties backupScalarDbProps = new Properties();
    try (InputStream in =
        Files.newInputStream(Paths.get(backupScalarDbConfigPath), StandardOpenOption.READ)) {
      backupScalarDbProps.load(in);
    }

    RecordHandler recordHandler =
        new RecordHandler(
            replicationUpdatedRecordRepository,
            replicationRecordRepository,
            StorageFactory.create(backupScalarDbProps).getStorage(),
            metricsLogger);

    RecordQueueConsumer recordQueueConsumer =
        new RecordQueueConsumer(
            new RecordQueueConsumer.Configuration(
                numOfRecordQueueConsumerThreads, waitMillisPerPartition),
            recordHandler,
            updatedRecordQueues);
    recordQueueConsumer.run();

    new RecordHandlerWorker(
            new RecordHandlerWorker.Configuration(
                // TODO: The partition size can be different from other partition sizes on other
                //       tables.
                REPLICATION_DB_BULK_TRANSACTION_PARTITION_SIZE,
                numOfRecordHandlerThreads,
                waitMillisPerPartition,
                transactionFetchSize,
                // FIXME
                0),
            recordHandler,
            replicationUpdatedRecordRepository,
            updatedRecordQueues,
            metricsLogger)
        .run();

    new BulkTransactionHandlerWorker(
            new BulkTransactionHandlerWorker.Configuration(
                REPLICATION_DB_BULK_TRANSACTION_PARTITION_SIZE,
                numOfBulkTransactionHandlerThreads,
                waitMillisPerPartition,
                transactionFetchSize),
            replicationBulkTransactionRepository,
            replicationTransactionRepository,
            transactionQueues,
            metricsLogger)
        .run();

    TransactionHandler transactionHandler =
        new TransactionHandler(
            REPLICATION_DB_BULK_TRANSACTION_PARTITION_SIZE,
            thresholdMillisForOldTransaction,
            replicationTransactionRepository,
            replicationUpdatedRecordRepository,
            replicationRecordRepository,
            coordinatorStateRepository,
            updatedRecordQueues,
            metricsLogger);

    new TransactionQueueConsumer(
            new Configuration(numOfTransactionQueueConsumerThreads, waitMillisPerPartition),
            transactionHandler,
            transactionQueues)
        .run();

    new TransactionHandlerWorker(
            new TransactionHandlerWorker.Configuration(
                REPLICATION_DB_BULK_TRANSACTION_PARTITION_SIZE,
                numOfTransactionHandlerThreads,
                waitMillisPerPartition,
                transactionFetchSize),
            transactionHandler,
            replicationTransactionRepository,
            metricsLogger)
        .run();

    while (true) {
      TimeUnit.MINUTES.sleep(1);
    }
  }
}
