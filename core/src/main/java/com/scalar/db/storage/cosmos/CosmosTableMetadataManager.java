package com.scalar.db.storage.cosmos;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.implementation.NotFoundException;
import com.azure.cosmos.models.PartitionKey;
import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.StorageRuntimeException;
import com.scalar.db.exception.storage.UnsupportedTypeException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.common.TableMetadataManager;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A manager to read and cache {@link TableMetadata} to know the type of each column
 *
 * @author Yuji Ito
 */
@ThreadSafe
public class CosmosTableMetadataManager implements TableMetadataManager {
  private static final String METADATA_DATABASE = "scalardb";
  private static final String METADATA_CONTAINER = "metadata";

  private final CosmosContainer container;
  private final Map<String, TableMetadata> tableMetadataMap;

  public CosmosTableMetadataManager(CosmosClient client, Optional<String> namespacePrefix) {
    String metadataDatabase =
        namespacePrefix.map(s -> s + METADATA_DATABASE).orElse(METADATA_DATABASE);
    container = client.getDatabase(metadataDatabase).getContainer(METADATA_CONTAINER);
    tableMetadataMap = new ConcurrentHashMap<>();
  }

  @VisibleForTesting
  CosmosTableMetadataManager(CosmosContainer container) {
    this.container = container;
    tableMetadataMap = new ConcurrentHashMap<>();
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
      CosmosTableMetadata cosmosTableMetadata = readMetadata(fullName);
      if (cosmosTableMetadata == null) {
        return null;
      }
      tableMetadataMap.put(fullName, convertTableMetadata(cosmosTableMetadata));
    }

    return tableMetadataMap.get(fullName);
  }

  private CosmosTableMetadata readMetadata(String fullName) {
    try {
      return container
          .readItem(fullName, new PartitionKey(fullName), CosmosTableMetadata.class)
          .getItem();
    } catch (NotFoundException e) {
      // The specified table is not found
      return null;
    } catch (CosmosException e) {
      throw new StorageRuntimeException("Failed to read the table metadata", e);
    }
  }

  private TableMetadata convertTableMetadata(CosmosTableMetadata cosmosTableMetadata) {
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
    //TODO To implement
  }

  @Override
  public void addTableMetadata(String namespace, String table, TableMetadata metadata) {
    //TODO To implement
  }
}
