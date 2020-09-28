package com.scalar.db.storage.dynamo;

import com.scalar.db.api.Operation;
import com.scalar.db.exception.storage.StorageRuntimeException;
import java.util.Map;
import java.util.HashMap;
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
public class TableMetadataManager {
  private final String METADATA_TABLE = "scalardb.metadata";
  private final DynamoDbClient client;
  private final Map<String, TableMetadata> tableMetadataMap;

  public TableMetadataManager(DynamoDbClient client) {
    this.client = client;
    this.tableMetadataMap = new ConcurrentHashMap<>();
  }

  public TableMetadata getTableMetadata(Operation operation) {
    if (!operation.forNamespace().isPresent() || !operation.forTable().isPresent()) {
      throw new IllegalArgumentException("operation has no target namespace and table name");
    }

    return getTableMetadata(operation.forNamespace().get(), operation.forTable().get());
  }

  private TableMetadata getTableMetadata(String namespace, String tableName) {
    String fullName = namespace + "." + tableName;
    if (!tableMetadataMap.containsKey(fullName)) {
      tableMetadataMap.put(fullName, readMetadata(fullName));
    }
    return tableMetadataMap.get(fullName);
  }

  private TableMetadata readMetadata(String fullName) {
    Map<String, AttributeValue> key = new HashMap<>();
    key.put("table", AttributeValue.builder().s(fullName).build());

    GetItemRequest request =
        GetItemRequest.builder().tableName(METADATA_TABLE).key(key).consistentRead(true).build();
    try {
      return new TableMetadata(client.getItem(request).item());
    } catch (DynamoDbException e) {
      throw new StorageRuntimeException("Failed to read the table metadata", e);
    }
  }
}
