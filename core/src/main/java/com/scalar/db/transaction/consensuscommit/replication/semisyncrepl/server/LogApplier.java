package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.TransactionTableMetadataManager;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.CachedCoordinatorStateRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.CoordinatorStateRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationBulkTransactionRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationRecordRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server.TransactionHandleWorker.Configuration;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
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
  private static final String ENV_VAR_NUM_OF_TRANSACTION_HANDLER_THREADS =
      "LOG_APPLIER_NUM_OF_TRANSACTION_HANDLER_THREADS";
  private static final String ENV_VAR_NUM_OF_RECORD_HANDLER_THREADS =
      "LOG_APPLIER_NUM_OF_RECORD_HANDLER_THREADS";
  private static final String ENV_VAR_TRANSACTION_FETCH_SIZE = "LOG_APPLIER_TRANSACTION_FETCH_SIZE";
  private static final String ENV_VAR_TRANSACTION_WAIT_MILLIS_PER_PARTITION =
      "LOG_APPLIER_TRANSACTION_WAIT_MILLIS_PER_PARTITION";
  private static final String ENV_VAR_COORDINATOR_STATE_CACHE_ENABLED =
      "LOG_APPLIER_COORDINATOR_STATE_CACHE_ENABLED";

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

    boolean coordinatorStateCacheEnabled = true;
    if (System.getenv(ENV_VAR_COORDINATOR_STATE_CACHE_ENABLED) != null) {
      coordinatorStateCacheEnabled =
          Boolean.parseBoolean(System.getenv(ENV_VAR_COORDINATOR_STATE_CACHE_ENABLED));
    }

    MetricsLogger metricsLogger = new MetricsLogger();

    DistributedStorage coordinatorDbStorage =
        StorageFactory.create(coordinatorStateConfigPath).getStorage();
    DistributedStorage replDbStorage = StorageFactory.create(replicationDbConfigPath).getStorage();
    StorageFactory backupSiteStorageFactory = StorageFactory.create(backupScalarDbConfigPath);
    DistributedStorage backupSiteStorage = backupSiteStorageFactory.getStorage();
    TransactionTableMetadataManager tableMetadataManager =
        new TransactionTableMetadataManager(backupSiteStorageFactory.getStorageAdmin(), 60);

    ConsensusCommitConfig consensusCommitConfig =
        new ConsensusCommitConfig(new DatabaseConfig(Paths.get(coordinatorStateConfigPath)));
    CoordinatorStateRepository coordinatorStateRepository;
    if (coordinatorStateCacheEnabled) {
      coordinatorStateRepository =
          new CachedCoordinatorStateRepository(
              coordinatorDbStorage, consensusCommitConfig, metricsLogger);
    } else {
      coordinatorStateRepository =
          new CoordinatorStateRepository(coordinatorDbStorage, consensusCommitConfig);
    }

    ReplicationRecordRepository replicationRecordRepository =
        new ReplicationRecordRepository(backupSiteStorage, "replication", "records");

    ReplicationBulkTransactionRepository replicationBulkTransactionRepository =
        new ReplicationBulkTransactionRepository(replDbStorage, "replication", "bulk_transactions");

    RecordHandler recordHandler =
        new RecordHandler(
            replicationRecordRepository, backupSiteStorage, tableMetadataManager, metricsLogger);

    TransactionHandler transactionHandler =
        new TransactionHandler(
            replicationRecordRepository, coordinatorStateRepository, recordHandler, metricsLogger);

    TransactionHandleWorker transactionHandleWorker =
        new TransactionHandleWorker(
            new Configuration(
                numOfTransactionHandlerThreads, numOfRecordHandlerThreads, waitMillisPerPartition),
            transactionHandler,
            metricsLogger);

    metricsLogger.setTransactionHandleWorker(transactionHandleWorker);

    BulkTransactionScanWorker bulkTransactionScanWorker =
        new BulkTransactionScanWorker(
            new BulkTransactionScanWorker.Configuration(
                REPLICATION_DB_PARTITION_SIZE,
                numOfBulkTransactionScanThreads,
                waitMillisPerPartition,
                transactionFetchSize),
            replicationBulkTransactionRepository,
            transactionHandleWorker,
            metricsLogger);
    bulkTransactionScanWorker.run();

    while (true) {
      Uninterruptibles.sleepUninterruptibly(Duration.ofMinutes(1));
    }
  }
}
