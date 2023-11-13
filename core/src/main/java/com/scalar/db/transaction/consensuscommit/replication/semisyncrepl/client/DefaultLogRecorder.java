package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.client;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.TransactionResult;
import com.scalar.db.transaction.consensuscommit.TransactionTableMetadata;
import com.scalar.db.transaction.consensuscommit.TransactionTableMetadataManager;
import com.scalar.db.transaction.consensuscommit.replication.LogRecorder;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit.GroupCommitException;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit.GroupCommitter;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Column;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.DeletedTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.InsertedTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Key;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Transaction;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.UpdatedTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.WrittenTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationTransactionRepository;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultLogRecorder implements LogRecorder {
  private static final Logger logger = LoggerFactory.getLogger(DefaultLogRecorder.class);

  private static final String ENV_VAR_GROUP_COMMIT_ENABLED = "LOG_RECORDER_GROUP_COMMIT_ENABLED";
  private static final String ENV_VAR_GROUP_COMMIT_NUM_OF_THREADS =
      "LOG_RECORDER_GROUP_COMMIT_NUM_OF_THREADS";
  private static final String ENV_VAR_GROUP_COMMIT_RETENTION_TIME_IN_MILLIS =
      "LOG_RECORDER_GROUP_COMMIT_RETENTION_TIME_IN_MILLIS";
  private static final String ENV_VAR_GROUP_COMMIT_NUM_OF_RETENTION_VALUES =
      "LOG_RECORDER_GROUP_COMMIT_NUM_OF_RETENTION_VALUES";
  private static final String ENV_VAR_GROUP_COMMIT_EXPIRATION_CHECK_INTERVAL_IN_MILLIS =
      "LOG_RECORDER_GROUP_COMMIT_EXPIRATION_CHECK_INTERVAL_IN_MILLIS";

  // TODO: Make these configurable
  private static final int REPLICATION_DB_PARTITION_SIZE = 256;

  private final TransactionTableMetadataManager tableMetadataManager;
  private final ExecutorService executorService =
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder().setNameFormat("log-recorder-%d").setDaemon(true).build());
  private final ReplicationTransactionRepository replicationTransactionRepository;
  @Nullable private final GroupCommitter<Integer, Transaction> groupCommitter;

  private String defaultNamespace;

  public void setDefaultNamespace(String defaultNamespace) {
    this.defaultNamespace = defaultNamespace;
  }

  public DefaultLogRecorder(
      TransactionTableMetadataManager tableMetadataManager,
      ReplicationTransactionRepository replicationTransactionRepository) {
    this.tableMetadataManager = tableMetadataManager;
    this.replicationTransactionRepository = replicationTransactionRepository;

    boolean groupCommitEnabled = true;
    if (System.getenv(ENV_VAR_GROUP_COMMIT_ENABLED) != null) {
      groupCommitEnabled = Boolean.parseBoolean(System.getenv(ENV_VAR_GROUP_COMMIT_ENABLED));
    }

    int groupCommitNumOfThreads = 4;
    if (System.getenv(ENV_VAR_GROUP_COMMIT_NUM_OF_THREADS) != null) {
      groupCommitNumOfThreads =
          Integer.parseInt(System.getenv(ENV_VAR_GROUP_COMMIT_NUM_OF_THREADS));
    }

    long groupCommitRetentionTimeInMillis = 40;
    if (System.getenv(ENV_VAR_GROUP_COMMIT_RETENTION_TIME_IN_MILLIS) != null) {
      groupCommitRetentionTimeInMillis =
          Long.parseLong(System.getenv(ENV_VAR_GROUP_COMMIT_RETENTION_TIME_IN_MILLIS));
    }

    int groupCommitNumOfRetentionValues = 32;
    if (System.getenv(ENV_VAR_GROUP_COMMIT_NUM_OF_RETENTION_VALUES) != null) {
      groupCommitNumOfRetentionValues =
          Integer.parseInt(System.getenv(ENV_VAR_GROUP_COMMIT_NUM_OF_RETENTION_VALUES));
    }

    long groupCommitExpirationCheckIntervalInMillis = 10;
    if (System.getenv(ENV_VAR_GROUP_COMMIT_EXPIRATION_CHECK_INTERVAL_IN_MILLIS) != null) {
      groupCommitExpirationCheckIntervalInMillis =
          Long.parseLong(System.getenv(ENV_VAR_GROUP_COMMIT_EXPIRATION_CHECK_INTERVAL_IN_MILLIS));
    }

    if (groupCommitEnabled) {
      Consumer<List<Transaction>> emitter =
          transactions -> {
            try {
              replicationTransactionRepository.bulkAdd(transactions);
            } catch (ExecutionException e) {
              // TODO: Revisit what information is needed in this log message
              throw new RuntimeException("Failed to send transactions", e);
            }
          };

      this.groupCommitter =
          new GroupCommitter<>(
              "log-recorder",
              groupCommitRetentionTimeInMillis,
              groupCommitNumOfRetentionValues,
              groupCommitExpirationCheckIntervalInMillis,
              groupCommitNumOfThreads,
              emitter);
    } else {
      this.groupCommitter = null;
    }
  }

  private void recordInternal(PrepareMutationComposerForReplication composer)
      throws ExecutionException {
    List<Mutation> mutations = composer.get();
    List<Operation> operations = composer.operations();
    List<TransactionResult> txResults = composer.transactionResultList();
    if (!(mutations.size() == operations.size() && mutations.size() == txResults.size())) {
      throw new AssertionError(
          String.format(
              "The sizes of the mutations, operations and transaction results aren't same. mutations.size=%d, operations.size=%d, transactionResults.size=%d",
              mutations.size(), operations.size(), txResults.size()));
    }

    List<WrittenTuple> writtenTuples = new ArrayList<>();
    Iterator<Mutation> mutationsIterator = mutations.iterator();
    Iterator<Operation> operationsIterator = operations.iterator();
    Iterator<TransactionResult> txResultIterator = txResults.iterator();
    while (mutationsIterator.hasNext()) {
      Put mutation = (Put) mutationsIterator.next();
      Operation op = operationsIterator.next();
      TransactionResult result = txResultIterator.next();

      String namespace = op.forNamespace().orElse(defaultNamespace);
      if (namespace == null) {
        throw new IllegalArgumentException("`namespace` isn't specified");
      }
      String table = op.forTable().get();
      int version = mutation.getIntValue("tx_version");
      long txPreparedAtInMillis = mutation.getBigIntValue("tx_prepared_at");

      TransactionTableMetadata tableMetadata = tableMetadataManager.getTransactionTableMetadata(op);

      if (op instanceof Put) {
        Put put = (Put) op;

        if (result == null) {
          List<Column<?>> columns = new ArrayList<>();
          put.getColumns()
              .values()
              .forEach(
                  column -> {
                    if (tableMetadata.getAfterImageColumnNames().contains(column.getName())) {
                      columns.add(Column.fromScalarDbColumn(column));
                    }
                  });
          writtenTuples.add(
              new InsertedTuple(
                  namespace,
                  table,
                  version,
                  txPreparedAtInMillis,
                  Key.fromScalarDbKey(put.getPartitionKey()),
                  Key.fromScalarDbKey(put.getClusteringKey().orElse(null)),
                  columns));

        } else {
          List<Column<?>> columns = new ArrayList<>();
          put.getColumns()
              .values()
              .forEach(
                  column -> {
                    if (tableMetadata.getAfterImageColumnNames().contains(column.getName())) {
                      columns.add(Column.fromScalarDbColumn(column));
                    }
                  });
          writtenTuples.add(
              new UpdatedTuple(
                  namespace,
                  table,
                  version,
                  txPreparedAtInMillis,
                  Key.fromScalarDbKey(put.getPartitionKey()),
                  Key.fromScalarDbKey(put.getClusteringKey().orElse(null)),
                  result.getId(),
                  columns));
        }
      } else if (op instanceof Delete) {
        Delete delete = (Delete) op;
        String prevTxId = result != null ? result.getId() : null;
        if (prevTxId == null) {
          logger.info(
              "Skipping Delete operation deleting a record that doesn't exist. op:{}, result:{}",
              op,
              result);
          continue;
        }
        writtenTuples.add(
            new DeletedTuple(
                namespace,
                table,
                version,
                txPreparedAtInMillis,
                Key.fromScalarDbKey(delete.getPartitionKey()),
                Key.fromScalarDbKey(delete.getClusteringKey().orElse(null)),
                prevTxId));
      } else {
        logger.warn("Skipping an unexpected operation. op:{}, result:{}", op, result);
      }
    }

    int candidatePartitionId =
        Math.abs(composer.transactionId().hashCode()) % REPLICATION_DB_PARTITION_SIZE;
    Instant now = Instant.now();

    if (groupCommitter != null) {
      try {
        groupCommitter.addValue(
            candidatePartitionId,
            partitionId ->
                new Transaction(partitionId, now, now, composer.transactionId(), writtenTuples));
      } catch (GroupCommitException e) {
        throw new RuntimeException(
            "Group commit failed. transactionId:" + composer.transactionId(), e);
      }
    } else {
      logger.info("Add start(thread_id:{})", Thread.currentThread().getId());
      replicationTransactionRepository.add(
          new Transaction(candidatePartitionId, now, now, composer.transactionId(), writtenTuples));
      logger.info(
          "Add end(thread_id:{}): {} ms",
          Thread.currentThread().getId(),
          System.currentTimeMillis() - now.toEpochMilli());
    }
  }

  @Override
  public Future<Void> record(PrepareMutationComposerForReplication composer)
      throws ExecutionException {
    return executorService.submit(
        () -> {
          recordInternal(composer);
          return null;
        });
  }
}
