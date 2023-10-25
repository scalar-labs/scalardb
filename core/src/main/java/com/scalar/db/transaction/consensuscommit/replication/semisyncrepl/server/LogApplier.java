package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.CoordinatorStateRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationRecordRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationTransactionRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server.DistributorThread.Configuration;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class LogApplier {
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
  private static final String ENV_VAR_EXTRA_WAIT_MILLIS_FOR_OLD_TRANSACTION =
      "LOG_APPLIER_EXTRA_WAIT_MILLIS_FOR_OLD_TRANSACTION";

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

    int extraWaitMillisForOldTransaction = 2000;
    if (System.getenv(ENV_VAR_EXTRA_WAIT_MILLIS_FOR_OLD_TRANSACTION) != null) {
      extraWaitMillisForOldTransaction =
          Integer.parseInt(System.getenv(ENV_VAR_EXTRA_WAIT_MILLIS_FOR_OLD_TRANSACTION));
    }

    ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    CoordinatorStateRepository coordinatorStateRepository =
        new CoordinatorStateRepository(
            StorageFactory.create(coordinatorStateConfigPath).getStorage(), "coordinator", "state");

    ReplicationTransactionRepository replicationTransactionRepository =
        new ReplicationTransactionRepository(
            StorageFactory.create(replicationDbConfigPath).getStorage(),
            objectMapper,
            "replication",
            "transactions");

    ReplicationRecordRepository replicationRecordRepository =
        new ReplicationRecordRepository(
            StorageFactory.create(replicationDbConfigPath).getStorage(),
            objectMapper,
            "replication",
            "records");

    BlockingQueue<Key> recordWriterQueue = new LinkedBlockingQueue<>();
    MetricsLogger metricsLogger = new MetricsLogger(recordWriterQueue);

    Properties backupScalarDbProps = new Properties();
    try (InputStream in =
        Files.newInputStream(Paths.get(backupScalarDbConfigPath), StandardOpenOption.READ)) {
      backupScalarDbProps.load(in);
    }
    RecordWriterThread recordWriter =
        new RecordWriterThread(
                numOfRecordWriterThreads,
                replicationRecordRepository,
                StorageFactory.create(backupScalarDbProps).getStorage(),
                recordWriterQueue,
                metricsLogger)
            .run();

    DistributorThread distributorThread =
        new DistributorThread(
                new Configuration(
                    REPLICATION_DB_PARTITION_SIZE,
                    numOfDistributorThreads,
                    transactionFetchSize,
                    waitMillisPerPartition,
                    thresholdMillisForOldTransaction,
                    extraWaitMillisForOldTransaction),
                coordinatorStateRepository,
                replicationTransactionRepository,
                replicationRecordRepository,
                recordWriterQueue,
                metricsLogger)
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
