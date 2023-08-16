package com.scalar.db.transaction.consensuscommit.replication.semisync;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Column;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils;
import com.scalar.db.transaction.consensuscommit.MutationComposer;
import com.scalar.db.transaction.consensuscommit.TransactionResult;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

class WriteSetExtractor implements MutationComposer {

  // TODO: Revisit this usage of Jackson databind
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final String txId;
  private final String defaultNamespace;
  private final TableMetadata tableMetadata;
  private final List<WrittenTuple> writtenTuples = new ArrayList<>();

  public WriteSetExtractor(TableMetadata tableMetadata, String defaultNamespace, String id) {
    txId = id;
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
    String table =
        op.forTable().orElseThrow(() -> new IllegalArgumentException("Table isn't specified"));

    if (op instanceof Put) {
      Put put = (Put) op;

      if (result == null) {
        List<Column<?>> columns = new ArrayList<>();
        for (Column<?> column : put.getColumns().values()) {
          if (ConsensusCommitUtils.isAfterImageColumn(column.getName(), tableMetadata)) {
            columns.add(column);
          }
        }
        writtenTuples.add(
            new InsertedTuple(
                namespace,
                table,
                put.getPartitionKey(),
                put.getClusteringKey().orElse(null),
                columns));

      } else {
        List<Column<?>> columns = new ArrayList<>();
        for (Column<?> column : put.getColumns().values()) {
          if (ConsensusCommitUtils.isAfterImageColumn(column.getName(), tableMetadata)) {
            columns.add(column);
          }
        }
        writtenTuples.add(
            new UpdatedTuple(
                namespace,
                table,
                put.getPartitionKey(),
                put.getClusteringKey().orElse(null),
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
              delete.getPartitionKey(),
              delete.getClusteringKey().orElse(null),
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
      return objectMapper.writeValueAsString(writtenTuples);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to convert write tuples into JSON string", e);
    }
  }
}
