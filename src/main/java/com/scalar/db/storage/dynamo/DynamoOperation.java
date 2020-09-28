package com.scalar.db.storage.dynamo;

import com.scalar.db.api.Operation;
import com.scalar.db.io.Value;
import com.scalar.db.storage.cosmos.ConcatenationVisitor;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/** A class to treating utilities for a operation */
public class DynamoOperation {
  static final String PARTITION_KEY = "concatenatedPartitionKey";
  static final String PARTITION_KEY_ALIAS = ":pk";
  static final String CLUSTERING_KEY_ALIAS = ":ck";
  static final String START_CLUSTERING_KEY_ALIAS = ":sck";
  static final String END_CLUSTERING_KEY_ALIAS = ":eck";
  static final String CONDITION_VALUE_ALIAS = ":cval";
  static final String VALUE_ALIAS = ":val";
  static final String RANGE_KEY_ALIAS = ":sk";
  static final String RANGE_CONDITION = " BETWEEN :sk0 AND :sk1";

  private final Operation operation;
  private final TableMetadata metadata;

  public DynamoOperation(Operation operation, TableMetadataManager metadataManager) {
    this.operation = operation;
    this.metadata = metadataManager.getTableMetadata(operation);
  }

  @Nonnull
  public Operation getOperation() {
    return operation;
  }

  @Nonnull
  public TableMetadata getMetadata() {
    return metadata;
  }

  @Nonnull
  public String getTableName() {
    return operation.forNamespace().get() + "." + operation.forTable().get();
  }

  @Nonnull
  public Map<String, AttributeValue> getKeyMap() {
    Map<String, AttributeValue> keyMap = new HashMap<>();
    String partitionKey = getConcatenatedPartitionKey();
    keyMap.put(PARTITION_KEY, AttributeValue.builder().s(partitionKey).build());

    operation
        .getClusteringKey()
        .ifPresent(
            k -> {
              keyMap.putAll(toMap(k.get()));
            });

    return keyMap;
  }

  String getConcatenatedPartitionKey() {
    Map<String, Value> keyMap = new HashMap<>();
    operation
        .getPartitionKey()
        .get()
        .forEach(
            v -> {
              keyMap.put(v.getName(), v);
            });

    ConcatenationVisitor visitor = new ConcatenationVisitor();
    metadata
        .getPartitionKeyNames()
        .forEach(
            name -> {
              if (keyMap.containsKey(name)) {
                keyMap.get(name).accept(visitor);
              } else {
                throw new IllegalArgumentException("The partition key is not properly specified.");
              }
            });

    return visitor.build();
  }

  Map<String, AttributeValue> toMap(Collection<Value> values) {
    MapVisitor visitor = new MapVisitor();
    values.forEach(v -> v.accept(visitor));

    return visitor.get();
  }

  boolean isSortKey(String key) {
    if(!metadata.getSortKeyName().isPresent()) {
      return false;
    }

    return metadata.getSortKeyName().get().equals(key);
  }
}
