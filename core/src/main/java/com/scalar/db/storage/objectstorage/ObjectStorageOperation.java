package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.Operation;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Column;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

@Immutable
public class ObjectStorageOperation {
  private final Operation operation;
  private final TableMetadata metadata;

  public ObjectStorageOperation(Operation operation, TableMetadata metadata) {
    this.operation = operation;
    this.metadata = metadata;
  }

  @Nonnull
  public Operation getOperation() {
    return operation;
  }

  @Nonnull
  public String getConcatenatedPartitionKey() {
    Map<String, Column<?>> keyMap = new HashMap<>();
    operation.getPartitionKey().getColumns().forEach(c -> keyMap.put(c.getName(), c));

    ConcatenationVisitor visitor = new ConcatenationVisitor();
    metadata.getPartitionKeyNames().forEach(name -> keyMap.get(name).accept(visitor));

    return visitor.build();
  }

  @Nonnull
  public String getConcatenatedClusteringKey() {
    Map<String, Column<?>> keyMap = new HashMap<>();
    operation
        .getClusteringKey()
        .ifPresent(k -> k.getColumns().forEach(c -> keyMap.put(c.getName(), c)));

    ConcatenationVisitor visitor = new ConcatenationVisitor();
    metadata.getClusteringKeyNames().forEach(name -> keyMap.get(name).accept(visitor));

    return visitor.build();
  }

  @Nonnull
  public String getRecordId() {
    if (operation.getClusteringKey().isPresent()) {
      return String.join(
          String.valueOf(ObjectStorageUtils.CONCATENATED_KEY_DELIMITER),
          getConcatenatedPartitionKey(),
          getConcatenatedClusteringKey());
    }
    return getConcatenatedPartitionKey();
  }
}
