package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.UpdatedRecord;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.CoordinatorStateRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationBulkTransactionRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationRecordRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationTransactionRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationUpdatedRecordRepository;
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
        new ArrayList<>(numOfRecordWriterThreads);
    for (int i = 0; i < numOfRecordWriterThreads; i++) {
      updatedRecordQueues.add(new LinkedBlockingQueue<>());
    }
    // FIXME
    MetricsLogger metricsLogger = new MetricsLogger(updatedRecordQueues.get(0));

    Properties backupScalarDbProps = new Properties();
    try (InputStream in =
        Files.newInputStream(Paths.get(backupScalarDbConfigPath), StandardOpenOption.READ)) {
      backupScalarDbProps.load(in);
    }

    RecordHandlerWorker recordHandlerWorker =
        new RecordHandlerWorker(
            new RecordHandlerWorker.Configuration(
                REPLICATION_DB_PARTITION_SIZE,
                numOfRecordWriterThreads,
                waitMillisPerPartition,
                transactionFetchSize),
            replicationUpdatedRecordRepository,
            replicationRecordRepository,
            StorageFactory.create(backupScalarDbProps).getStorage(),
            updatedRecordQueues,
            metricsLogger);
    recordHandlerWorker.run();

    BulkTransactionHandlerWorker bulkTransactionHandlerWorker =
        new BulkTransactionHandlerWorker(
            new BulkTransactionHandlerWorker.Configuration(
                REPLICATION_DB_PARTITION_SIZE,
                numOfDistributorThreads,
                waitMillisPerPartition,
                transactionFetchSize),
            replicationBulkTransactionRepository,
            replicationTransactionRepository,
            metricsLogger);
    bulkTransactionHandlerWorker.run();

    TransactionHandlerWorker transactionHandlerWorker =
        new TransactionHandlerWorker(
            new TransactionHandlerWorker.Configuration(
                REPLICATION_DB_PARTITION_SIZE,
                numOfDistributorThreads,
                waitMillisPerPartition,
                transactionFetchSize,
                thresholdMillisForOldTransaction),
            coordinatorStateRepository,
            replicationTransactionRepository,
            replicationUpdatedRecordRepository,
            replicationRecordRepository,
            updatedRecordQueues,
            metricsLogger);
    transactionHandlerWorker.run();

    while (true) {
      TimeUnit.MINUTES.sleep(1);
    }
  }
}