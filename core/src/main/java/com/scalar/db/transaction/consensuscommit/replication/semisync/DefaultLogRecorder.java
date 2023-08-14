package com.scalar.db.transaction.consensuscommit.replication.semisync;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder.Buildable;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.AbstractMutationComposer;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils;
import com.scalar.db.transaction.consensuscommit.Snapshot;
import com.scalar.db.transaction.consensuscommit.TransactionResult;
import com.scalar.db.transaction.consensuscommit.replication.LogRecorder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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

  static class WriteSetExtractor extends AbstractMutationComposer {

    // TODO: Revisit this usage of Jackson databind
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String defaultNamespace;
    private final TableMetadata tableMetadata;

    public WriteSetExtractor(TableMetadata tableMetadata, String defaultNamespace, String id) {
      super(id);
      this.tableMetadata = tableMetadata;
      this.defaultNamespace = defaultNamespace;
    }

    @Override
    public void add(com.scalar.db.api.Operation op, @Nullable TransactionResult result)
        throws ExecutionException {
      String namespace = op.forNamespace().orElse(defaultNamespace);
      if (namespace == null) {
        throw new IllegalArgumentException("Namespace isn't specified");
      }

      if (op instanceof Put) {
        Put put = (Put) op;

        if (result == null) {
          Buildable builder =
              Put.newBuilder()
                  .namespace(REPLICATION_DB_NAMESPACE)
                  .table(REPLICATION_DB_TABLE)
                  .partitionKey(
                      Key.ofInt("partition_id", id.hashCode() % REPLICATION_DB_PARTITION_SIZE))
                  .clusteringKey(Key.ofBigInt("created_at", System.currentTimeMillis()))
                  .textValue("transaction_id", id);
          Map<String, Record> writeSet = new HashMap<>();
          for (Column<?> column : put.getColumns().values()) {
            if (ConsensusCommitUtils.isAfterImageColumn(column.getName(), tableMetadata)) {
              writeSet.put(column.getName(), new Record(null, column));
            }
          }
          try {
            builder.textValue("write_set", objectMapper.writeValueAsString(writeSet));
          } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
          }
          mutations.add(builder.build());
        } else {
          // TODO: Take care of UPDATE
          throw new AssertionError();
        }
      } else if (op instanceof Delete) {
        // TODO: Take care of DELETE
        throw new AssertionError();
      } else {
        throw new IllegalArgumentException("Unexpected operation: " + op);
      }
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
    List<Put> writeSet =
        extractor.get().stream()
            .map(
                mutation -> {
                  if (mutation instanceof Put) {
                    return (Put) mutation;
                  } else {
                    throw new IllegalStateException("Expected Put instance, but got " + mutation);
                  }
                })
            .collect(Collectors.toList());

    storage.put(writeSet);
  }

  @Override
  public void close() throws Exception {}
}
