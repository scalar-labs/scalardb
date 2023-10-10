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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultLogRecorder implements LogRecorder {
  private static final Logger logger = LoggerFactory.getLogger(DefaultLogRecorder.class);

  // TODO: Make these configurable
  private static final int REPLICATION_DB_PARTITION_SIZE = 256;

  private final TransactionTableMetadataManager tableMetadataManager;
  private final ReplicationTransactionRepository replicationTransactionRepository;
  private final ExecutorService executorService =
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder().setNameFormat("log-recorder-%d").setDaemon(true).build());

  private String defaultNamespace;

  public void setDefaultNamespace(String defaultNamespace) {
    this.defaultNamespace = defaultNamespace;
  }

  public DefaultLogRecorder(
      TransactionTableMetadataManager tableMetadataManager,
      ReplicationTransactionRepository replicationTransactionRepository) {
    this.tableMetadataManager = tableMetadataManager;
    this.replicationTransactionRepository = replicationTransactionRepository;
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
        throw new IllegalArgumentException("Unexpected operation: " + op);
      }
    }

    int partitionId = Math.abs(composer.transactionId().hashCode()) % REPLICATION_DB_PARTITION_SIZE;
    Instant now = Instant.now();
    replicationTransactionRepository.add(
        new Transaction(partitionId, now, now, composer.transactionId(), writtenTuples));
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
