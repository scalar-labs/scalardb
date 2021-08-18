package com.scalar.db.storage.dynamo;

import com.scalar.db.api.Operation;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.StorageRuntimeException;
import com.scalar.db.exception.storage.UnsupportedTypeException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.common.TableMetadataManager;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;

/**
 * A manager to read and cache {@link TableMetadata} to know the type of each column
 *
 * @author Yuji Ito
 */
@ThreadSafe
public class DynamoTableMetadataManager implements TableMetadataManager {
  private static final String METADATA_TABLE = "scalardb.metadata";
  private static final String PARTITION_KEY = "partitionKey";
  private static final String CLUSTERING_KEY = "clusteringKey";
  private static final String SECONDARY_INDEX = "secondaryIndex";
  private static final String COLUMNS = "columns";

  private final DynamoDbClient client;
  private final Optional<String> namespacePrefix;
  private final Map<String, TableMetadata> tableMetadataMap;

  public DynamoTableMetadataManager(DynamoDbClient client, Optional<String> namespacePrefix) {
    this.client = client;
    this.namespacePrefix = namespacePrefix;
    this.tableMetadataMap = new ConcurrentHashMap<>();
  }

  @Override
  public TableMetadata getTableMetadata(Operation operation) {
    if (!operation.forNamespace().isPresent() || !operation.forTable().isPresent()) {
      throw new IllegalArgumentException("operation has no target namespace and table name");
    }
    return getTableMetadata(operation.forFullNamespace().get(), operation.forTable().get());
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) {
    String fullName = namespace + "." + table;
    if (!tableMetadataMap.containsKey(fullName)) {
      TableMetadata tableMetadata = readMetadata(fullName);
      if (tableMetadata == null) {
        return null;
      }
      tableMetadataMap.put(fullName, tableMetadata);
    }

    return tableMetadataMap.get(fullName);
  }

  private TableMetadata readMetadata(String fullName) {
    Map<String, AttributeValue> key = new HashMap<>();
    key.put("table", AttributeValue.builder().s(fullName).build());
    String metadataTable = namespacePrefix.map(s -> s + METADATA_TABLE).orElse(METADATA_TABLE);

    GetItemRequest request =
        GetItemRequest.builder().tableName(metadataTable).key(key).consistentRead(true).build();
    try {
      Map<String, AttributeValue> metadata = client.getItem(request).item();
      if (metadata.isEmpty()) {
        // The specified table is not found
        return null;
      }
      return createTableMetadata(metadata);
    } catch (DynamoDbException e) {
      throw new StorageRuntimeException("Failed to read the table metadata", e);
    }
  }

  private TableMetadata createTableMetadata(Map<String, AttributeValue> metadata) {
    TableMetadata.Builder builder = TableMetadata.newBuilder();
    metadata
        .get(COLUMNS)
        .m()
        .forEach((name, type) -> builder.addColumn(name, convertDataType(type.s())));
    metadata.get(PARTITION_KEY).l().stream()
        .map(AttributeValue::s)
        .forEach(builder::addPartitionKey);
    if (metadata.containsKey(CLUSTERING_KEY)) {
      // The clustering order is always ASC for now
      metadata.get(CLUSTERING_KEY).l().stream()
          .map(AttributeValue::s)
          .forEach(n -> builder.addClusteringKey(n, Scan.Ordering.Order.ASC));
    }
    if (metadata.containsKey(SECONDARY_INDEX)) {
      metadata.get(SECONDARY_INDEX).ss().forEach(builder::addSecondaryIndex);
    }
    return builder.build();
  }

  private DataType convertDataType(String columnType) {
    switch (columnType) {
      case "int":
        return DataType.INT;
      case "bigint":
        return DataType.BIGINT;
      case "float":
        return DataType.FLOAT;
      case "double":
        return DataType.DOUBLE;
      case "text": // for backwards compatibility
      case "varchar":
        return DataType.TEXT;
      case "boolean":
        return DataType.BOOLEAN;
      case "blob":
        return DataType.BLOB;
      default:
        throw new UnsupportedTypeException(columnType);
    }
  }

  @Override
  public void deleteTableMetadata(String namespace, String table) {
    // TODO To implement
    throw new UnsupportedOperationException();
  }

  @Override
  public void addTableMetadata(String namespace, String table, TableMetadata metadata) {
    // TODO To implement
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getTableNames(String namespace) {
    // TODO To implement
    throw new UnsupportedOperationException();
  }
}
