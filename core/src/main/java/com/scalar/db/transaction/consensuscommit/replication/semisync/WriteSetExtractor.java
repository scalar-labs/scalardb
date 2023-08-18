package com.scalar.db.transaction.consensuscommit.replication.semisync;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.MutationComposer;
import com.scalar.db.transaction.consensuscommit.TransactionResult;
import com.scalar.db.transaction.consensuscommit.TransactionTableMetadata;
import com.scalar.db.transaction.consensuscommit.TransactionTableMetadataManager;
import com.scalar.db.transaction.consensuscommit.replication.semisync.columns.Column;
import com.scalar.db.transaction.consensuscommit.replication.semisync.columns.Key;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

class WriteSetExtractor implements MutationComposer {

  // TODO: Revisit this usage of Jackson databind
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final String txId;
  private final String defaultNamespace;
  private final TransactionTableMetadataManager tableMetadataManager;
  private final List<WrittenTuple> writtenTuples = new ArrayList<>();
  private final TypeReference<List<WrittenTuple>> typeReference =
      new TypeReference<List<WrittenTuple>>() {};

  public WriteSetExtractor(
      TransactionTableMetadataManager tableMetadataManager, String defaultNamespace, String id) {
    txId = id;
    this.tableMetadataManager = tableMetadataManager;
    this.defaultNamespace = defaultNamespace;
  }

  @Override
  public void add(com.scalar.db.api.Operation op, @Nullable TransactionResult result)
      throws ExecutionException {
    String namespace = op.forNamespace().orElse(defaultNamespace);
    if (namespace == null) {
      throw new IllegalArgumentException("Namespace isn't specified");
    }
    String table =
        op.forTable().orElseThrow(() -> new IllegalArgumentException("Table isn't specified"));

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
                    columns.add(Column.of(column));
                  }
                });
        writtenTuples.add(
            new InsertedTuple(
                namespace,
                table,
                Key.of(put.getPartitionKey()),
                Key.of(put.getClusteringKey().orElse(null)),
                columns));

      } else {
        List<Column<?>> columns = new ArrayList<>();
        put.getColumns()
            .values()
            .forEach(
                column -> {
                  if (tableMetadata.getAfterImageColumnNames().contains(column.getName())) {
                    columns.add(Column.of(column));
                  }
                });
        writtenTuples.add(
            new UpdatedTuple(
                namespace,
                table,
                Key.of(put.getPartitionKey()),
                Key.of(put.getClusteringKey().orElse(null)),
                result.getId(),
                columns));
      }
    } else if (op instanceof Delete) {
      Delete delete = (Delete) op;
      assert result != null;
      writtenTuples.add(
          new DeletedTuple(
              namespace,
              table,
              Key.of(delete.getPartitionKey()),
              Key.of(delete.getClusteringKey().orElse(null)),
              result.getId()));
    } else {
      throw new IllegalArgumentException("Unexpected operation: " + op);
    }
  }

  @Override
  public List<Mutation> get() {
    throw new AssertionError("Shouldn't be called this method");
  }

  public String txId() {
    return txId;
  }

  public List<WrittenTuple> writtenTuples() {
    return writtenTuples;
  }

  public String writtenTuplesAsJson() {
    try {
      return objectMapper.writerFor(typeReference).writeValueAsString(writtenTuples);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to convert write tuples into JSON string", e);
    }
  }
}
