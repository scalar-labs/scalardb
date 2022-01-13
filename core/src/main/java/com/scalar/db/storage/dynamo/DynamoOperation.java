package com.scalar.db.storage.dynamo;

import com.scalar.db.api.Operation;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Value;
import com.scalar.db.storage.dynamo.bytes.KeyBytesEncoder;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/** A utility class for an operation */
@Immutable
public class DynamoOperation {
  static final String PARTITION_KEY = "concatenatedPartitionKey";
  static final String CLUSTERING_KEY = "concatenatedClusteringKey";
  static final String PARTITION_KEY_ALIAS = ":pk";
  static final String START_CLUSTERING_KEY_ALIAS = ":sck";
  static final String END_CLUSTERING_KEY_ALIAS = ":eck";
  static final String CONDITION_VALUE_ALIAS = ":cval";
  static final String VALUE_ALIAS = ":val";
  static final String COLUMN_NAME_ALIAS = "#col";
  static final String CONDITION_COLUMN_NAME_ALIAS = "#ccol";
  static final String INDEX_NAME_PREFIX = "index";
  static final String GLOBAL_INDEX_NAME_PREFIX = "global_index";

  private final Operation operation;
  private final TableMetadata metadata;

  public DynamoOperation(Operation operation, TableMetadata metadata) {
    this.operation = operation;
    this.metadata = metadata;
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
    return operation.forFullTableName().get();
  }

  @Nonnull
  public String getIndexName(String clusteringKey) {
    return operation.forFullTableName().get() + "." + INDEX_NAME_PREFIX + "." + clusteringKey;
  }

  @Nonnull
  public String getGlobalIndexName(String column) {
    return operation.forFullTableName().get() + "." + GLOBAL_INDEX_NAME_PREFIX + "." + column;
  }

  @Nonnull
  public Map<String, AttributeValue> getKeyMap() {
    Map<String, AttributeValue> keyMap = new HashMap<>();
    ByteBuffer partitionKey = getConcatenatedPartitionKey();
    keyMap.put(
        PARTITION_KEY, AttributeValue.builder().b(SdkBytes.fromByteBuffer(partitionKey)).build());

    getConcatenatedClusteringKey()
        .ifPresent(
            k ->
                keyMap.put(
                    CLUSTERING_KEY,
                    AttributeValue.builder().b(SdkBytes.fromByteBuffer(k)).build()));

    return keyMap;
  }

  ByteBuffer getConcatenatedPartitionKey() {
    return new KeyBytesEncoder().encode(operation.getPartitionKey());
  }

  Optional<ByteBuffer> getConcatenatedClusteringKey() {
    if (!operation.getClusteringKey().isPresent()) {
      return Optional.empty();
    }
    return Optional.of(
        new KeyBytesEncoder()
            .encode(operation.getClusteringKey().get(), metadata.getClusteringOrders()));
  }

  Map<String, AttributeValue> toMap(Collection<Value<?>> values) {
    MapVisitor visitor = new MapVisitor();
    values.forEach(v -> v.accept(visitor));

    return visitor.get();
  }
}
