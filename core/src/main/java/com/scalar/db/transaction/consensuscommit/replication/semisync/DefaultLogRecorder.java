package com.scalar.db.transaction.consensuscommit.replication.semisync;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder.Buildable;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.Snapshot;
import com.scalar.db.transaction.consensuscommit.TransactionTableMetadataManager;
import com.scalar.db.transaction.consensuscommit.replication.LogRecorder;

public class DefaultLogRecorder implements LogRecorder {
  // TODO: Make these configurable
  private static final String REPLICATION_DB_NAMESPACE = "replication";
  private static final String REPLICATION_DB_TABLE = "transactions";
  private static final int REPLICATION_DB_PARTITION_SIZE = 256;

  private final TransactionTableMetadataManager tableMetadataManager;
  private final DistributedStorage storage;

  private String defaultNamespace;

  public void setDefaultNamespace(String defaultNamespace) {
    this.defaultNamespace = defaultNamespace;
  }

  public DefaultLogRecorder(
      TransactionTableMetadataManager tableMetadataManager, DatabaseConfig config) {
    this.tableMetadataManager = tableMetadataManager;
    storage = new StorageFactory(config).getStorage();
  }

  @Override
  public void record(Snapshot snapshot) throws PreparationConflictException, ExecutionException {
    WriteSetExtractor extractor =
        new WriteSetExtractor(tableMetadataManager, defaultNamespace, snapshot.getId());
    snapshot.to(extractor);

    Buildable builder =
        Put.newBuilder()
            .namespace(REPLICATION_DB_NAMESPACE)
            .table(REPLICATION_DB_TABLE)
            .partitionKey(
                Key.ofInt(
                    "partition_id",
                    Math.abs(extractor.txId().hashCode()) % REPLICATION_DB_PARTITION_SIZE))
            .clusteringKey(Key.ofBigInt("created_at", System.currentTimeMillis()))
            .textValue("transaction_id", extractor.txId())
            // TODO: Revisit here
            /*
            .condition(
                ConditionBuilder.putIf(
                        ConditionBuilder.column("transaction_id")
                            .isNotEqualToText(extractor.txId()))
                    .build())
             */
            // TODO: Revisit here as this would be slow. Schemaful serialization should be better
            .value(TextColumn.of("write_set", extractor.writtenTuplesAsJson()));

    storage.put(builder.build());
  }

  @Override
  public void close() throws Exception {}
}
