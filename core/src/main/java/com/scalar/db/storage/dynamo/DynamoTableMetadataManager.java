package com.scalar.db.storage.dynamo;

import static com.scalar.db.util.Utility.getFullNamespaceName;
import static com.scalar.db.util.Utility.getFullTableName;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.StorageRuntimeException;
import com.scalar.db.exception.storage.UnsupportedTypeException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.common.TableMetadataManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;

/**
 * A manager to read and cache {@link TableMetadata} to know the type of each column
 *
 * @author Yuji Ito
 */
@ThreadSafe
public class DynamoTableMetadataManager implements TableMetadataManager {
  private static final String METADATA_NAMESPACE = "scalardb";
  private static final String METADATA_TABLE = "metadata";
  private static final String PARTITION_KEY = "partitionKey";
  private static final String CLUSTERING_KEY = "clusteringKey";
  private static final String SECONDARY_INDEX = "secondaryIndex";
  private static final String COLUMNS = "columns";
  private static final String TABLE = "table";
  private static final long METADATA_TABLE_RU = 1;

  private static final int CREATING_WAITING_TIME = 3000;

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
    String fullName = getFullTableName(namespacePrefix, namespace, table);
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
    key.put(TABLE, AttributeValue.builder().s(fullName).build());
    String metadataTable = getFullTableName(namespacePrefix, METADATA_NAMESPACE, METADATA_TABLE);

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

    HashMap<String, AttributeValue> keyToDelete = new HashMap<>();
    keyToDelete.put(
        TABLE,
        AttributeValue.builder().s(getFullTableName(namespacePrefix, namespace, table)).build());
    DeleteItemRequest deleteReq =
        DeleteItemRequest.builder()
            .tableName(getFullTableName(namespacePrefix, METADATA_NAMESPACE, METADATA_TABLE))
            .key(keyToDelete)
            .build();
    try {
      client.deleteItem(deleteReq);
    } catch (DynamoDbException e) {
      throw new StorageRuntimeException("deleting metadata failed");
    }

    try {
      DescribeTableResponse describeTableResponse =
          client.describeTable(
              DescribeTableRequest.builder()
                  .tableName(getFullTableName(namespacePrefix, METADATA_NAMESPACE, METADATA_TABLE))
                  .build());
      TableDescription tableDescription = describeTableResponse.table();
      if (tableDescription.itemCount() == 0) {
        try {
          client.deleteTable(
              DeleteTableRequest.builder()
                  .tableName(getFullTableName(namespacePrefix, METADATA_NAMESPACE, METADATA_TABLE))
                  .build());
        } catch (DynamoDbException e) {
          throw new StorageRuntimeException("deleting empty metadata table failed");
        }
      }
    } catch (DynamoDbException e) {
      throw new StorageRuntimeException("getting metadata table description failed");
    }
  }

  @Override
  public void addTableMetadata(String namespace, String table, TableMetadata metadata) {
    createMetadataTableIfNotExist();
    Map<String, AttributeValue> itemValues = new HashMap<>();

    // Add metadata
    itemValues.put(
        TABLE,
        AttributeValue.builder().s(getFullTableName(namespacePrefix, namespace, table)).build());
    Map<String, AttributeValue> columns = new HashMap<>();
    for (String columnName : metadata.getColumnNames()) {
      columns.put(
          columnName,
          AttributeValue.builder()
              .s(metadata.getColumnDataType(columnName).name().toLowerCase())
              .build());
    }
    itemValues.put(COLUMNS, AttributeValue.builder().m(columns).build());
    itemValues.put(
        PARTITION_KEY,
        AttributeValue.builder()
            .l(
                metadata.getPartitionKeyNames().stream()
                    .map(pKey -> AttributeValue.builder().s(pKey).build())
                    .collect(Collectors.toList()))
            .build());
    itemValues.put(
        CLUSTERING_KEY,
        AttributeValue.builder()
            .l(
                metadata.getClusteringKeyNames().stream()
                    .map(pKey -> AttributeValue.builder().s(pKey).build())
                    .collect(Collectors.toList()))
            .build());
    itemValues.put(
        SECONDARY_INDEX, AttributeValue.builder().ss(metadata.getSecondaryIndexNames()).build());

    PutItemRequest request =
        PutItemRequest.builder()
            .tableName(getFullTableName(namespacePrefix, METADATA_NAMESPACE, METADATA_TABLE))
            .item(itemValues)
            .build();

    try {
      client.putItem(request);
    } catch (DynamoDbException e) {
      throw new StorageRuntimeException(
          "adding meta data for table "
              + getFullTableName(namespacePrefix, namespace, table)
              + " failed",
          e);
    }
  }

  @Override
  public Set<String> getTableNames(String namespace) {
    Set<String> tableSet = new HashSet<>();
    try {
      ListTablesResponse listTablesResponse = client.listTables();
      List<String> tableNames = listTablesResponse.tableNames();
      for (String tableName : tableNames) {
        if (tableName.startsWith(getFullNamespaceName(namespacePrefix, namespace))) {
          tableSet.add(tableName);
        }
      }
    } catch (DynamoDbException e) {
      throw new StorageRuntimeException("retrieving the table names failed", e);
    }
    return tableSet;
  }

  private void createMetadataTableIfNotExist() throws StorageRuntimeException {
    if (metadataTableExists()) {
      return;
    }

    CreateTableRequest.Builder requestBuilder = CreateTableRequest.builder();
    List<AttributeDefinition> columnsToAttributeDefinitions = new ArrayList<>();
    columnsToAttributeDefinitions.add(
        AttributeDefinition.builder()
            .attributeName(TABLE)
            .attributeType(ScalarAttributeType.S)
            .build());
    requestBuilder.attributeDefinitions(columnsToAttributeDefinitions);
    requestBuilder.keySchema(
        KeySchemaElement.builder().attributeName(TABLE).keyType(KeyType.HASH).build());
    requestBuilder.provisionedThroughput(
        ProvisionedThroughput.builder()
            .readCapacityUnits(METADATA_TABLE_RU)
            .writeCapacityUnits(METADATA_TABLE_RU)
            .build());
    requestBuilder.tableName(getFullTableName(namespacePrefix, METADATA_NAMESPACE, METADATA_TABLE));

    try {
      client.createTable(requestBuilder.build());
    } catch (DynamoDbException e) {
      throw new StorageRuntimeException("creating meta data table failed");
    }

    while (true) {
      try {
        Uninterruptibles.sleepUninterruptibly(CREATING_WAITING_TIME, TimeUnit.MILLISECONDS);
        DescribeTableRequest describeTableRequest =
            DescribeTableRequest.builder()
                .tableName(getFullTableName(namespacePrefix, METADATA_NAMESPACE, METADATA_TABLE))
                .build();
        DescribeTableResponse describeTableResponse = client.describeTable(describeTableRequest);
        if (describeTableResponse.table().tableStatus() == TableStatus.ACTIVE) {
          break;
        }
      } catch (DynamoDbException e) {
        throw new StorageRuntimeException("getting table description failed", e);
      }
    }
  }

  private boolean metadataTableExists() throws StorageRuntimeException {
    try {
      DescribeTableRequest describeTableRequest =
          DescribeTableRequest.builder()
              .tableName(getFullTableName(namespacePrefix, METADATA_NAMESPACE, METADATA_TABLE))
              .build();
      client.describeTable(describeTableRequest);
      return true;
    } catch (DynamoDbException e) {
      if (e instanceof ResourceNotFoundException) {
        return false;
      } else {
        throw new StorageRuntimeException("checking metadata table exist failed");
      }
    }
  }
}
