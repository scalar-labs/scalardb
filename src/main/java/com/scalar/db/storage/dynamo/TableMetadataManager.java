package com.scalar.db.storage.dynamo;

import com.scalar.db.api.Operation;
import com.scalar.db.exception.storage.StorageRuntimeException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;

/**
 * A manager to read and cache {@link DynamoTableMetadata} to know the type of each column
 *
 * @author Yuji Ito
 */
@ThreadSafe
public class TableMetadataManager {
  private final String METADATA_TABLE = "scalardb.metadata";
  private final DynamoDbClient client;
  private final Optional<String> namespacePrefix;
  private final Map<String, DynamoTableMetadata> tableMetadataMap;

  public TableMetadataManager(DynamoDbClient client, Optional<String> namespacePrefix) {
    this.client = client;
    this.namespacePrefix = namespacePrefix;
    this.tableMetadataMap = new ConcurrentHashMap<>();
  }

  public DynamoTableMetadata getTableMetadata(Operation operation) {
    if (!operation.forNamespace().isPresent() || !operation.forTable().isPresent()) {
      throw new IllegalArgumentException("operation has no target namespace and table name");
    }

    String fullName = operation.forFullNamespace().get() + "." + operation.forTable().get();
    if (!tableMetadataMap.containsKey(fullName)) {
      tableMetadataMap.put(fullName, readMetadata(fullName));
    }

    return tableMetadataMap.get(fullName);
  }

  private DynamoTableMetadata readMetadata(String fullName) {
    Map<String, AttributeValue> key = new HashMap<>();
    key.put("table", AttributeValue.builder().s(fullName).build());
    String metadataTable =
        namespacePrefix.isPresent() ? namespacePrefix.get() + METADATA_TABLE : METADATA_TABLE;

    GetItemRequest request =
        GetItemRequest.builder().tableName(metadataTable).key(key).consistentRead(true).build();
    try {
      return new DynamoTableMetadata(client.getItem(request).item());
    } catch (DynamoDbException e) {
      throw new StorageRuntimeException("Failed to read the table metadata", e);
    }
  }
}
