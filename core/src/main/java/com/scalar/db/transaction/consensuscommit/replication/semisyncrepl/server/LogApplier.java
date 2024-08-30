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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogApplier {
  private static final Logger logger = LoggerFactory.getLogger(LogApplier.class);

  private static final String ENV_VAR_BACKUP_SCALARDB_CONFIG = "LOG_APPLIER_BACKUP_SCALARDB_CONFIG";
  private static final String ENV_VAR_REPLICATION_CONFIG = "LOG_APPLIER_REPLICATION_CONFIG";
  private static final String ENV_VAR_COORDINATOR_STATE_CONFIG =
      "LOG_APPLIER_COORDINATOR_STATE_CONFIG";
  private static final String ENV_VAR_NUM_OF_DISTRIBUTOR_THREADS =
      "LOG_APPLIER_NUM_OF_DISTRIBUTOR_THREADS";
  private static final String ENV_VAR_NUM_OF_RECORD_WRITER_THREADS =
      "LOG_APPLIER_NUM_OF_RECORD_WRITER_THREADS";
  private static final String ENV_VAR_TRANSACTION_FETCH_SIZE = "LOG_APPLIER_TRANSACTION_FETCH_SIZE";
  private static final String ENV_VAR_TRANSACTION_WAIT_MILLIS_PER_PARTITION =
      "LOG_APPLIER_TRANSACTION_WAIT_MILLIS_PER_PARTITION";
  private static final String ENV_VAR_THRESHOLD_MILLIS_FOR_OLD_TRANSACTION =
      "LOG_APPLIER_THRESHOLD_MILLIS_FOR_OLD_TRANSACTION";

  private static final int REPLICATION_DB_PARTITION_SIZE = 256;

  public static void main(String[] args) throws IOException {
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

    int numOfDistributorThreads = 16;
    if (System.getenv(ENV_VAR_NUM_OF_DISTRIBUTOR_THREADS) != null) {
      numOfDistributorThreads = Integer.parseInt(System.getenv(ENV_VAR_NUM_OF_DISTRIBUTOR_THREADS));
    }

    int numOfRecordWriterThreads = 16;
    if (System.getenv(ENV_VAR_NUM_OF_RECORD_WRITER_THREADS) != null) {
      numOfRecordWriterThreads =
          Integer.parseInt(System.getenv(ENV_VAR_NUM_OF_RECORD_WRITER_THREADS));
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

    Properties backupScalarDbProps = new Properties();
    try (InputStream in =
        Files.newInputStream(Paths.get(backupScalarDbConfigPath), StandardOpenOption.READ)) {
      backupScalarDbProps.load(in);
    }

    ExecutorService executorServiceForRecordHandlers =
        Executors.newFixedThreadPool(
            numOfRecordWriterThreads,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("record-handler-%d")
                .setUncaughtExceptionHandler(
                    (thread, e) -> logger.error("Got an uncaught exception. thread:{}", thread, e))
                .build());

    BlockingQueue<Transaction> transactionQueue = new LinkedBlockingQueue<>();

    MetricsLogger metricsLogger =
        new MetricsLogger(transactionQueue, executorServiceForRecordHandlers);

    RecordHandler recordHandler =
        new RecordHandler(
            replicationRecordRepository,
            StorageFactory.create(backupScalarDbProps).getStorage(),
            metricsLogger);

    TransactionHandler transactionHandler =
        new TransactionHandler(
            REPLICATION_DB_PARTITION_SIZE,
            thresholdMillisForOldTransaction,
            replicationTransactionRepository,
            replicationRecordRepository,
            coordinatorStateRepository,
            recordHandler,
            executorServiceForRecordHandlers,
            metricsLogger);

    BulkTransactionHandlerWorker bulkTransactionHandlerWorker =
        new BulkTransactionHandlerWorker(
            new BulkTransactionHandlerWorker.Configuration(
                REPLICATION_DB_PARTITION_SIZE,
                numOfDistributorThreads,
                waitMillisPerPartition,
                transactionFetchSize),
            replicationBulkTransactionRepository,
            replicationTransactionRepository,
            transactionQueue,
            metricsLogger);
    bulkTransactionHandlerWorker.run();

    /*
    OldTransactionHandlerWorker transactionHandlerWorker =
        new OldTransactionHandlerWorker(
            new OldTransactionHandlerWorker.Configuration(
                REPLICATION_DB_PARTITION_SIZE,
                numOfDistributorThreads,
                waitMillisPerPartition,
                transactionFetchSize,
                thresholdMillisForOldTransaction),
            coordinatorStateRepository,
            replicationTransactionRepository,
            replicationRecordRepository,
            recordHandler,
            executorServiceForRecordHandlers,
            metricsLogger);
    transactionHandlerWorker.run();
     */
    new TransactionQueueConsumer(
            new Configuration(numOfRecordWriterThreads, waitMillisPerPartition),
            transactionHandler,
            transactionQueue,
            metricsLogger)
        .run();

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  bulkTransactionHandlerWorker.close();
                }));
  }
}
