package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Transaction;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.CoordinatorStateRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationBulkTransactionRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationRecordRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationTransactionRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server.TransactionQueueConsumer.Configuration;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogApplier {
  private static final Logger logger = LoggerFactory.getLogger(LogApplier.class);

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

    int numOfBulkTransactionHandlerThreads = 32;
    if (System.getenv(ENV_VAR_NUM_OF_BULK_TRANSACTION_HANDLER_THREADS) != null) {
      numOfBulkTransactionHandlerThreads =
          Integer.parseInt(System.getenv(ENV_VAR_NUM_OF_BULK_TRANSACTION_HANDLER_THREADS));
    }

    int numOfTransactionHandlerThreads = 32;
    if (System.getenv(ENV_VAR_NUM_OF_TRANSACTION_HANDLER_THREADS) != null) {
      numOfTransactionHandlerThreads =
          Integer.parseInt(System.getenv(ENV_VAR_NUM_OF_TRANSACTION_HANDLER_THREADS));
    }

    int numOfTransactionQueueConsumerThreads = 8;
    if (System.getenv(ENV_VAR_NUM_OF_TRANSACTION_QUEUE_CONSUMER_THREADS) != null) {
      numOfTransactionQueueConsumerThreads =
          Integer.parseInt(System.getenv(ENV_VAR_NUM_OF_TRANSACTION_QUEUE_CONSUMER_THREADS));
    }

    int numOfRecordHandlerThreads = 16;
    if (System.getenv(ENV_VAR_NUM_OF_RECORD_HANDLER_THREADS) != null) {
      numOfRecordHandlerThreads =
          Integer.parseInt(System.getenv(ENV_VAR_NUM_OF_RECORD_HANDLER_THREADS));
    }

    int transactionFetchSize = 32;
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

    ExecutorService executorServiceForRecordHandlers =
        Executors.newFixedThreadPool(
            numOfRecordHandlerThreads,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("record-handler-%d")
                .setUncaughtExceptionHandler(
                    (thread, e) -> logger.error("Got an uncaught exception. thread:{}", thread, e))
                .build());

    InMemoryQueue<Transaction> transactionQueue =
        new InMemoryQueue<>(numOfTransactionQueueConsumerThreads);

    // FIXME
    MetricsLogger metricsLogger =
        new MetricsLogger(transactionQueue, executorServiceForRecordHandlers);

    Properties backupScalarDbProps = new Properties();
    try (InputStream in =
        Files.newInputStream(Paths.get(backupScalarDbConfigPath), StandardOpenOption.READ)) {
      backupScalarDbProps.load(in);
    }

    RecordHandler recordHandler =
        new RecordHandler(
            replicationRecordRepository,
            StorageFactory.create(backupScalarDbProps).getStorage(),
            metricsLogger);

    new BulkTransactionHandlerWorker(
            new BulkTransactionHandlerWorker.Configuration(
                REPLICATION_DB_BULK_TRANSACTION_PARTITION_SIZE,
                numOfBulkTransactionHandlerThreads,
                waitMillisPerPartition,
                transactionFetchSize),
            replicationBulkTransactionRepository,
            replicationTransactionRepository,
            transactionQueue,
            metricsLogger)
        .run();

    TransactionHandler transactionHandler =
        new TransactionHandler(
            REPLICATION_DB_BULK_TRANSACTION_PARTITION_SIZE,
            thresholdMillisForOldTransaction,
            replicationTransactionRepository,
            replicationRecordRepository,
            coordinatorStateRepository,
            recordHandler,
            executorServiceForRecordHandlers,
            metricsLogger);

    new TransactionQueueConsumer(
            new Configuration(numOfTransactionQueueConsumerThreads, waitMillisPerPartition),
            transactionHandler,
            transactionQueue,
            metricsLogger)
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
