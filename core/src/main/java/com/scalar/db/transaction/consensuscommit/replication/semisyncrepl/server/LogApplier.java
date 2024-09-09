package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.CoordinatorStateRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationBulkTransactionRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationRecordRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationTransactionRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server.TransactionHandleWorker.Configuration;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogApplier {
  private static final Logger logger = LoggerFactory.getLogger(LogApplier.class);

  private static final String ENV_VAR_BACKUP_SCALARDB_CONFIG = "LOG_APPLIER_BACKUP_SCALARDB_CONFIG";
  private static final String ENV_VAR_REPLICATION_CONFIG = "LOG_APPLIER_REPLICATION_CONFIG";
  private static final String ENV_VAR_COORDINATOR_STATE_CONFIG =
      "LOG_APPLIER_COORDINATOR_STATE_CONFIG";
  private static final String ENV_VAR_NUM_OF_BULK_TRANSACTION_SCAN_THREADS =
      "LOG_APPLIER_NUM_OF_BULK_TRANSACTION_SCAN_THREADS";
  private static final String ENV_VAR_NUM_OF_TRANSACTION_SCAN_THREADS =
      "LOG_APPLIER_NUM_OF_TRANSACTION_SCAN_THREADS";
  private static final String ENV_VAR_NUM_OF_TRANSACTION_HANDLER_THREADS =
      "LOG_APPLIER_NUM_OF_TRANSACTION_HANDLER_THREADS";
  private static final String ENV_VAR_NUM_OF_RECORD_HANDLER_THREADS =
      "LOG_APPLIER_NUM_OF_RECORD_HANDLER_THREADS";
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

    int numOfBulkTransactionScanThreads = 16;
    if (System.getenv(ENV_VAR_NUM_OF_BULK_TRANSACTION_SCAN_THREADS) != null) {
      numOfBulkTransactionScanThreads =
          Integer.parseInt(System.getenv(ENV_VAR_NUM_OF_BULK_TRANSACTION_SCAN_THREADS));
    }

    int numOfTransactionScanThreads = 16;
    if (System.getenv(ENV_VAR_NUM_OF_TRANSACTION_SCAN_THREADS) != null) {
      numOfTransactionScanThreads =
          Integer.parseInt(System.getenv(ENV_VAR_NUM_OF_TRANSACTION_SCAN_THREADS));
    }

    int numOfTransactionHandlerThreads = 16;
    if (System.getenv(ENV_VAR_NUM_OF_TRANSACTION_HANDLER_THREADS) != null) {
      numOfTransactionHandlerThreads =
          Integer.parseInt(System.getenv(ENV_VAR_NUM_OF_TRANSACTION_HANDLER_THREADS));
    }

    int numOfRecordHandlerThreads = 16;
    if (System.getenv(ENV_VAR_NUM_OF_RECORD_HANDLER_THREADS) != null) {
      numOfRecordHandlerThreads =
          Integer.parseInt(System.getenv(ENV_VAR_NUM_OF_RECORD_HANDLER_THREADS));
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

    CoordinatorStateRepository coordinatorStateRepository =
        new CoordinatorStateRepository(
            StorageFactory.create(coordinatorStateConfigPath).getStorage(), "coordinator", "state");

    ReplicationRecordRepository replicationRecordRepository =
        new ReplicationRecordRepository(
            StorageFactory.create(replicationDbConfigPath).getStorage(), "replication", "records");

    ReplicationTransactionRepository replicationTransactionRepository =
        new ReplicationTransactionRepository(
            StorageFactory.create(replicationDbConfigPath).getStorage(),
            "replication",
            "transactions");

    ReplicationBulkTransactionRepository replicationBulkTransactionRepository =
        new ReplicationBulkTransactionRepository(
            StorageFactory.create(replicationDbConfigPath).getStorage(),
            "replication",
            "bulk_transactions");

    Properties backupScalarDbProps = new Properties();
    try (InputStream in =
        Files.newInputStream(Paths.get(backupScalarDbConfigPath), StandardOpenOption.READ)) {
      backupScalarDbProps.load(in);
    }

    MetricsLogger metricsLogger = new MetricsLogger();

    RecordHandler recordHandler =
        new RecordHandler(
            replicationRecordRepository,
            StorageFactory.create(backupScalarDbProps).getStorage(),
            metricsLogger);

    TransactionHandler transactionHandler =
        new TransactionHandler(
            thresholdMillisForOldTransaction,
            replicationTransactionRepository,
            replicationRecordRepository,
            coordinatorStateRepository,
            recordHandler,
            metricsLogger);

    TransactionHandleWorker transactionHandleWorker =
        new TransactionHandleWorker(
            new Configuration(
                numOfTransactionHandlerThreads, numOfRecordHandlerThreads, waitMillisPerPartition),
            transactionHandler,
            metricsLogger);

    metricsLogger.setTransactionHandleWorker(transactionHandleWorker);

    new BulkTransactionScanWorker(
            new BulkTransactionScanWorker.Configuration(
                REPLICATION_DB_PARTITION_SIZE,
                numOfBulkTransactionScanThreads,
                waitMillisPerPartition,
                transactionFetchSize),
            replicationBulkTransactionRepository,
            replicationTransactionRepository,
            transactionHandleWorker,
            metricsLogger)
        .run();

    new TransactionScanWorker(
            new TransactionScanWorker.Configuration(
                REPLICATION_DB_PARTITION_SIZE,
                numOfTransactionScanThreads,
                waitMillisPerPartition,
                transactionFetchSize),
            replicationTransactionRepository,
            transactionHandleWorker,
            metricsLogger)
        .run();

    while (true) {
      Uninterruptibles.sleepUninterruptibly(Duration.ofMinutes(1));
    }
  }
}
