package com.scalar.db.storage.cosmos;

import static com.scalar.db.storage.cosmos.CosmosAdmin.DEFAULT_RU;
import static com.scalar.db.util.Utility.getFullNamespaceName;
import static com.scalar.db.util.Utility.getFullTableName;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.implementation.NotFoundException;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.StorageRuntimeException;
import com.scalar.db.exception.storage.UnsupportedTypeException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.common.TableMetadataManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A manager to read and cache {@link TableMetadata} to know the type of each column
 *
 * @author Yuji Ito
 */
@ThreadSafe
public class CosmosTableMetadataManager implements TableMetadataManager {
  static final String METADATA_DATABASE = "scalardb";
  static final String METADATA_CONTAINER = "metadata";
  private final CosmosClient client;
  private final Optional<String> databasePrefix;
  private final Map<String, TableMetadata> tableMetadataMap;

  public CosmosTableMetadataManager(CosmosClient client, Optional<String> databasePrefix) {
    this.client = client;
    tableMetadataMap = new ConcurrentHashMap<>();
    this.databasePrefix = databasePrefix;
  }

  @Override
  public TableMetadata getTableMetadata(Operation operation) {
    if (!operation.forNamespace().isPresent() || !operation.forTable().isPresent()) {
      throw new IllegalArgumentException("operation has no target namespace and table name");
    }
    return getTableMetadata(operation.forNamespace().get(), operation.forTable().get());
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) {
    String fullName = getFullTableName(databasePrefix, namespace, table);
    if (!tableMetadataMap.containsKey(fullName)) {
      CosmosTableMetadata cosmosTableMetadata = readMetadata(fullName);
      if (cosmosTableMetadata == null) {
        return null;
      }
      tableMetadataMap.put(fullName, convertToTableMetadata(cosmosTableMetadata));
    }

    return tableMetadataMap.get(fullName);
  }

  private CosmosTableMetadata readMetadata(String fullName) {
    try {
      return getContainer()
          .readItem(fullName, new PartitionKey(fullName), CosmosTableMetadata.class)
          .getItem();
    } catch (NotFoundException e) {
      // The specified table is not found
      return null;
    } catch (RuntimeException e) {
      throw new StorageRuntimeException("Failed to read the table metadata", e);
    }
  }

  private TableMetadata convertToTableMetadata(CosmosTableMetadata cosmosTableMetadata) {
    TableMetadata.Builder builder = TableMetadata.newBuilder();
    cosmosTableMetadata
        .getColumns()
        .forEach((name, type) -> builder.addColumn(name, convertDataType(type)));
    cosmosTableMetadata.getPartitionKeyNames().forEach(builder::addPartitionKey);
    // The clustering order is always ASC for now
    cosmosTableMetadata
        .getClusteringKeyNames()
        .forEach(n -> builder.addClusteringKey(n, Scan.Ordering.Order.ASC));
    cosmosTableMetadata.getSecondaryIndexNames().forEach(builder::addSecondaryIndex);
    return builder.build();
  }

  private CosmosTableMetadata convertToCosmosTableMetadata(
      String fullTableName, TableMetadata tableMetadata) {
    CosmosTableMetadata cosmosTableMetadata = new CosmosTableMetadata();
    cosmosTableMetadata.setId(fullTableName);
    cosmosTableMetadata.setPartitionKeyNames(new ArrayList<>(tableMetadata.getPartitionKeyNames()));
    cosmosTableMetadata.setClusteringKeyNames(
        new ArrayList<>(tableMetadata.getClusteringKeyNames()));
    cosmosTableMetadata.setSecondaryIndexNames(tableMetadata.getSecondaryIndexNames());
    Map<String, String> columnTypeByName = new HashMap<>();
    tableMetadata
        .getColumnNames()
        .forEach(
            columnName ->
                columnTypeByName.put(
                    columnName, tableMetadata.getColumnDataType(columnName).name().toLowerCase()));
    cosmosTableMetadata.setColumns(columnTypeByName);
    return cosmosTableMetadata;
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
    String fullTableName = getFullTableName(databasePrefix, namespace, table);
    try {
      getContainer()
          .deleteItem(
              fullTableName, new PartitionKey(fullTableName), new CosmosItemRequestOptions());
      tableMetadataMap.remove(fullTableName);
      // Delete the metadata container and table if there is no more metadata stored
      if (!getContainer()
          .queryItems(
              "SELECT 1 FROM " + METADATA_CONTAINER + " OFFSET 0 LIMIT 1",
              new CosmosQueryRequestOptions(),
              Object.class)
          .stream()
          .findFirst()
          .isPresent()) {
        getContainer().delete();
        client.getDatabase(getFullNamespaceName(databasePrefix, METADATA_DATABASE)).delete();
      }
    } catch (RuntimeException e) {
      throw new StorageRuntimeException("deleting the table metadata failed", e);
    }
  }

  @Override
  public void addTableMetadata(String namespace, String table, TableMetadata metadata) {
    try {
      createMetadataDatabaseAndContainerIfNotExists();

      CosmosTableMetadata cosmosTableMetadata =
          convertToCosmosTableMetadata(
              getFullTableName(databasePrefix, namespace, table), metadata);
      getContainer().upsertItem(cosmosTableMetadata);
    } catch (RuntimeException e) {
      throw new StorageRuntimeException("adding the table metadata failed", e);
    }
  }

  private void createMetadataDatabaseAndContainerIfNotExists() {
    String metadataDatabase = getFullNamespaceName(databasePrefix, METADATA_DATABASE);
    ThroughputProperties manualThroughput =
        ThroughputProperties.createManualThroughput(Integer.parseInt(DEFAULT_RU));
    client.createDatabaseIfNotExists(metadataDatabase, manualThroughput);
    CosmosContainerProperties containerProperties =
        new CosmosContainerProperties(METADATA_CONTAINER, "/id");
    client.getDatabase(metadataDatabase).createContainerIfNotExists(containerProperties);
  }

  private CosmosContainer getContainer() {
    return client
        .getDatabase(getFullNamespaceName(databasePrefix, METADATA_DATABASE))
        .getContainer(METADATA_CONTAINER);
  }

  @Override
  public Set<String> getTableNames(String namespace) {
    if (!metadataContainerExists()) {
      return Collections.emptySet();
    }
    String fullDatabase = getFullNamespaceName(databasePrefix, namespace);
    String selectAllDatabaseContainer =
        "SELECT * FROM "
            + METADATA_CONTAINER
            + " WHERE "
            + METADATA_CONTAINER
            + ".id LIKE '"
            + fullDatabase
            + ".%'";
    try {
      return getContainer()
          .queryItems(
              selectAllDatabaseContainer,
              new CosmosQueryRequestOptions(),
              CosmosTableMetadata.class)
          .stream()
          .map(tableMetadata -> tableMetadata.getId().replaceFirst("^" + fullDatabase + ".", ""))
          .collect(Collectors.toSet());
    } catch (RuntimeException e) {
      throw new StorageRuntimeException("Retrieving the table names failed", e);
    }
  }

  private boolean metadataContainerExists() {
    try {
      client
          .getDatabase(getFullNamespaceName(databasePrefix, METADATA_DATABASE))
          .getContainer(METADATA_CONTAINER)
          .read();
    } catch (RuntimeException e) {
      if (e instanceof CosmosException
          && ((CosmosException) e).getStatusCode() == CosmosErrorCode.NOT_FOUND.get()) {
        return false;
      }
      throw new StorageRuntimeException("reading the metadata container failed", e);
    }
    return true;
  }
}
