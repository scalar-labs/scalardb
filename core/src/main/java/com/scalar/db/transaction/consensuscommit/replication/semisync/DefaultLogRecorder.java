package com.scalar.db.transaction.consensuscommit.replication.semisync;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.transaction.consensuscommit.Snapshot;
import com.scalar.db.transaction.consensuscommit.TransactionTableMetadataManager;
import com.scalar.db.transaction.consensuscommit.replication.LogRecorder;
import com.scalar.db.transaction.consensuscommit.replication.model.Transaction;
import com.scalar.db.transaction.consensuscommit.replication.repository.ReplicationTransactionRepository;
import java.time.Instant;

public class DefaultLogRecorder implements LogRecorder {
  // TODO: Make these configurable
  private static final String REPLICATION_DB_NAMESPACE = "replication";
  private static final String REPLICATION_DB_TABLE = "transactions";
  private static final int REPLICATION_DB_PARTITION_SIZE = 256;

  private final TransactionTableMetadataManager tableMetadataManager;
  private final ReplicationTransactionRepository replicationTransactionRepository;

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

  @Override
  public void record(Snapshot snapshot) throws PreparationConflictException, ExecutionException {
    WriteSetExtractor extractor =
        new WriteSetExtractor(tableMetadataManager, defaultNamespace, snapshot.getId());
    snapshot.to(extractor);

    int partitionId = Math.abs(extractor.txId().hashCode()) % REPLICATION_DB_PARTITION_SIZE;
    replicationTransactionRepository.add(
        new Transaction(partitionId, Instant.now(), extractor.txId(), extractor.writtenTuples()));
  }

  @Override
  public void close() throws Exception {}
}
