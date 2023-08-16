package com.scalar.db.transaction.consensuscommit.replication.semisync;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder.Buildable;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.Snapshot;
import com.scalar.db.transaction.consensuscommit.replication.LogRecorder;
import javax.annotation.Nullable;

public class DefaultLogRecorder implements LogRecorder {
  // TODO: Make these configurable
  private static final String REPLICATION_DB_NAMESPACE = "replication";
  private static final String REPLICATION_DB_TABLE = "transactions";
  private static final int REPLICATION_DB_PARTITION_SIZE = 256;

  private final TableMetadata tableMetadata;
  private final DistributedStorage storage;

  private String defaultNamespace;

  static class Record {
    final String prevTxId;
    final Column<?> column;

    public Record(@Nullable String prevTxId, Column<?> column) {
      this.prevTxId = prevTxId;
      this.column = column;
    }
  }

  public void setDefaultNamespace(String defaultNamespace) {
    this.defaultNamespace = defaultNamespace;
  }

  public DefaultLogRecorder(TableMetadata tableMetadata, DatabaseConfig config) {
    this.tableMetadata = tableMetadata;
    storage = new StorageFactory(config).getStorage();
  }

  @Override
  public void record(Snapshot snapshot) throws PreparationConflictException, ExecutionException {
    WriteSetExtractor extractor =
        new WriteSetExtractor(tableMetadata, defaultNamespace, snapshot.getId());
    snapshot.to(extractor);

    Buildable builder =
        Put.newBuilder()
            .namespace(REPLICATION_DB_NAMESPACE)
            .table(REPLICATION_DB_TABLE)
            .partitionKey(
                Key.ofInt("partition_id", extractor.hashCode() % REPLICATION_DB_PARTITION_SIZE))
            .clusteringKey(Key.ofBigInt("created_at", System.currentTimeMillis()))
            .textValue("transaction_id", extractor.txId())
            // TODO: Revisit here as this would be slow
            .condition(
                ConditionBuilder.putIf(
                        ConditionBuilder.column("transaction_id")
                            .isNotEqualToText(extractor.txId()))
                    .build())
            // TODO: Revisit here as this would be slow
            .value(TextColumn.of("write_set", extractor.writtenTuplesAsJson()));

    storage.put(builder.build());
  }

  @Override
  public void close() throws Exception {}
}
