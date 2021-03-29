package com.scalar.db.storage.dynamo;

import com.scalar.db.api.Operation;
import com.scalar.db.io.Value;
import com.scalar.db.storage.cosmos.ConcatenationVisitor;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/** A utility class for an operation */
public class DynamoOperation {
  static final String PARTITION_KEY = "concatenatedPartitionKey";
  static final String CLUSTERING_KEY = "concatenatedClusteringKey";
  static final String PARTITION_KEY_ALIAS = ":pk";
  static final String CLUSTERING_KEY_ALIAS = ":ck";
  static final String START_CLUSTERING_KEY_ALIAS = ":sck";
  static final String END_CLUSTERING_KEY_ALIAS = ":eck";
  static final String CONDITION_VALUE_ALIAS = ":cval";
  static final String VALUE_ALIAS = ":val";
  static final String COLUMN_NAME_ALIAS = "#col";
  static final String RANGE_KEY_ALIAS = ":sk";
  static final String RANGE_CONDITION = " BETWEEN :sk0 AND :sk1";
  static final String INDEX_NAME_PREFIX = "index";
  static final String GLOBAL_INDEX_NAME_PREFIX = "global_index";

  private final Operation operation;
  private final DynamoTableMetadata metadata;

  public DynamoOperation(Operation operation, DynamoTableMetadataManager metadataManager) {
    this.operation = operation;
    this.metadata = metadataManager.getTableMetadata(operation);
  }

  public DynamoOperation(Operation operation, DynamoTableMetadata metadata) {
    this.operation = operation;
    this.metadata = metadata;
  }

  @Nonnull
  public Operation getOperation() {
    return operation;
  }

  @Nonnull
  public DynamoTableMetadata getMetadata() {
    return metadata;
  }

  @Nonnull
  public String getTableName() {
    return operation.forFullTableName().get();
  }

  @Nonnull
  public String getIndexName(String clusteringKey) {
    return operation.forUnprefixedFullTableName().get()
        + "."
        + INDEX_NAME_PREFIX
        + "."
        + clusteringKey;
  }

  @Nonnull
  public String getGlobalIndexName(String column) {
    return operation.forUnprefixedFullTableName().get()
        + "."
        + GLOBAL_INDEX_NAME_PREFIX
        + "."
        + column;
  }

  @Nonnull
  public Map<String, AttributeValue> getKeyMap() {
    Map<String, AttributeValue> keyMap = new HashMap<>();
    String partitionKey = getConcatenatedPartitionKey();
    keyMap.put(PARTITION_KEY, AttributeValue.builder().s(partitionKey).build());

    getConcatenatedClusteringKey()
        .ifPresent(k -> keyMap.put(CLUSTERING_KEY, AttributeValue.builder().s(k).build()));

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
    metadata.getPartitionKeyNames().forEach(name -> keyMap.get(name).accept(visitor));

    return visitor.build();
  }

  Optional<String> getConcatenatedClusteringKey() {
    if (!operation.getClusteringKey().isPresent()) {
      return Optional.empty();
    }

    Map<String, Value> keyMap = new HashMap<>();
    operation
        .getClusteringKey()
        .ifPresent(
            k -> {
              k.get()
                  .forEach(
                      v -> {
                        keyMap.put(v.getName(), v);
                      });
            });

    ConcatenationVisitor visitor = new ConcatenationVisitor();
    metadata.getClusteringKeyNames().forEach(name -> keyMap.get(name).accept(visitor));

    return Optional.of(visitor.build());
  }

  Map<String, AttributeValue> toMap(Collection<Value> values) {
    MapVisitor visitor = new MapVisitor();
    values.forEach(v -> v.accept(visitor));

    return visitor.get();
  }

  boolean isSingleClusteringKey() {
    return metadata.getClusteringKeyNames().size() == 1;
  }
}
