package com.scalar.db.storage.cosmos;

import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.implementation.NotFoundException;
import com.azure.cosmos.models.CompositePath;
import com.azure.cosmos.models.CompositePathSortOrder;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.CosmosStoredProcedureProperties;
import com.azure.cosmos.models.ExcludedPath;
import com.azure.cosmos.models.IncludedPath;
import com.azure.cosmos.models.IndexingPolicy;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.StorageInfo;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.VirtualTableInfo;
import com.scalar.db.api.VirtualTableJoinType;
import com.scalar.db.common.CoreError;
import com.scalar.db.common.StorageInfoImpl;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class CosmosAdmin implements DistributedStorageAdmin {
  private static final Logger logger = LoggerFactory.getLogger(CosmosAdmin.class);

  public static final String REQUEST_UNIT = "ru";
  public static final String DEFAULT_REQUEST_UNIT = "400";
  public static final String NO_SCALING = "no-scaling";
  public static final String DEFAULT_NO_SCALING = "false";

  public static final String METADATA_CONTAINER = "metadata";
  private static final String ID = "id";
  private static final String CONCATENATED_PARTITION_KEY = "concatenatedPartitionKey";
  private static final String PARTITION_KEY_PATH = "/" + CONCATENATED_PARTITION_KEY;
  private static final String CLUSTERING_KEY_PATH_PREFIX = "/clusteringKey/";
  private static final String SECONDARY_INDEX_KEY_PATH_PREFIX = "/values/";
  private static final String EXCLUDED_PATH = "/*";
  @VisibleForTesting public static final String STORED_PROCEDURE_FILE_NAME = "mutate.js";
  private static final String STORED_PROCEDURE_PATH =
      "cosmosdb_stored_procedure/" + STORED_PROCEDURE_FILE_NAME;
  private static final StorageInfo STORAGE_INFO =
      new StorageInfoImpl(
          "cosmos",
          StorageInfo.MutationAtomicityUnit.PARTITION,
          // No limit on the number of mutations
          Integer.MAX_VALUE,
          false);

  private final CosmosClient client;
  private final String metadataDatabase;

  @Inject
  public CosmosAdmin(DatabaseConfig databaseConfig) {
    CosmosConfig config = new CosmosConfig(databaseConfig);
    client = CosmosUtils.buildCosmosClient(config);
    metadataDatabase =
        config.getTableMetadataDatabase().orElse(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
  }

  CosmosAdmin(CosmosClient client, CosmosConfig config) {
    this.client = client;
    metadataDatabase =
        config.getTableMetadataDatabase().orElse(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    try {
      createTableInternal(namespace, table, metadata);
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new ExecutionException("Creating the container failed", e);
    }
  }

  private void createTableInternal(String namespace, String table, TableMetadata metadata)
      throws ExecutionException {
    checkMetadata(metadata);
    createContainer(namespace, table, metadata, false);
    putTableMetadata(namespace, table, metadata, true);
  }

  private void checkMetadata(TableMetadata metadata) {
    for (String clusteringKeyName : metadata.getClusteringKeyNames()) {
      if (metadata.getColumnDataType(clusteringKeyName) == DataType.BLOB) {
        throw new IllegalArgumentException(
            CoreError.COSMOS_CLUSTERING_KEY_BLOB_TYPE_NOT_SUPPORTED.buildMessage(
                clusteringKeyName));
      }
    }
  }

  private void createContainer(
      String database, String table, TableMetadata metadata, boolean ifNotExists)
      throws ExecutionException {
    CosmosDatabase cosmosDatabase = client.getDatabase(database);
    CosmosContainerProperties properties = computeContainerProperties(table, metadata);
    if (ifNotExists) {
      cosmosDatabase.createContainerIfNotExists(properties);
    } else {
      cosmosDatabase.createContainer(properties);
    }
    addStoredProcedure(database, table, ifNotExists);
  }

  private void addStoredProcedure(String namespace, String table, boolean ifNotExists)
      throws ExecutionException {
    CosmosDatabase cosmosDatabase = client.getDatabase(namespace);
    CosmosStoredProcedureProperties storedProcedureProperties =
        computeContainerStoredProcedureProperties();

    if (ifNotExists && storedProcedureExists(namespace, table)) {
      return;
    }
    cosmosDatabase
        .getContainer(table)
        .getScripts()
        .createStoredProcedure(storedProcedureProperties);
  }

  private boolean storedProcedureExists(String namespace, String table) {
    try {
      client
          .getDatabase(namespace)
          .getContainer(table)
          .getScripts()
          .getStoredProcedure(STORED_PROCEDURE_FILE_NAME)
          .read();
      return true;
    } catch (CosmosException e) {
      if (e.getStatusCode() == 404) {
        return false;
      }
      throw e;
    }
  }

  private CosmosStoredProcedureProperties computeContainerStoredProcedureProperties()
      throws ExecutionException {
    String storedProcedure;
    try (InputStream storedProcedureInputStream =
        getClass().getClassLoader().getResourceAsStream(STORED_PROCEDURE_PATH)) {
      assert storedProcedureInputStream != null;

      try (InputStreamReader inputStreamReader =
              new InputStreamReader(storedProcedureInputStream, StandardCharsets.UTF_8);
          BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
        storedProcedure =
            bufferedReader.lines().reduce("", (prev, cur) -> prev + cur + System.lineSeparator());
      }
    } catch (IOException e) {
      throw new ExecutionException("Reading the stored procedure failed", e);
    }

    return new CosmosStoredProcedureProperties(STORED_PROCEDURE_FILE_NAME, storedProcedure);
  }

  private CosmosContainerProperties computeContainerProperties(
      String table, TableMetadata metadata) {
    IndexingPolicy indexingPolicy = computeIndexingPolicy(metadata);
    return new CosmosContainerProperties(table, PARTITION_KEY_PATH)
        .setIndexingPolicy(indexingPolicy);
  }

  private IndexingPolicy computeIndexingPolicy(TableMetadata metadata) {
    IndexingPolicy indexingPolicy = new IndexingPolicy();
    List<IncludedPath> paths = new ArrayList<>();

    if (metadata.getClusteringKeyNames().isEmpty()) {
      paths.add(new IncludedPath(PARTITION_KEY_PATH + "/?"));
    } else {
      // Add a composite index when we have clustering keys
      List<CompositePath> compositePaths = new ArrayList<>();

      // Add concatenated partition key to the composite path first
      CompositePath partitionKeyCompositePath = new CompositePath();
      partitionKeyCompositePath.setPath(PARTITION_KEY_PATH);
      partitionKeyCompositePath.setOrder(CompositePathSortOrder.ASCENDING);
      compositePaths.add(partitionKeyCompositePath);

      // Then, add clustering keys to the composite path
      metadata
          .getClusteringKeyNames()
          .forEach(
              c -> {
                CompositePath compositePath = new CompositePath();
                compositePath.setPath(CLUSTERING_KEY_PATH_PREFIX + c);
                compositePath.setOrder(
                    metadata.getClusteringOrder(c) == Order.ASC
                        ? CompositePathSortOrder.ASCENDING
                        : CompositePathSortOrder.DESCENDING);
                compositePaths.add(compositePath);
              });

      indexingPolicy.setCompositeIndexes(Collections.singletonList(compositePaths));
    }

    paths.addAll(
        metadata.getSecondaryIndexNames().stream()
            .map(index -> new IncludedPath(SECONDARY_INDEX_KEY_PATH_PREFIX + index + "/?"))
            .collect(Collectors.toList()));

    if (!paths.isEmpty()) {
      indexingPolicy.setIncludedPaths(paths);
    }
    indexingPolicy.setExcludedPaths(Collections.singletonList(new ExcludedPath(EXCLUDED_PATH)));
    return indexingPolicy;
  }

  private void putTableMetadata(
      String namespace,
      String table,
      TableMetadata metadata,
      boolean createMetadataDatabaseAndContainerIfNotExists)
      throws ExecutionException {
    try {
      if (createMetadataDatabaseAndContainerIfNotExists) {
        createMetadataDatabaseAndContainerIfNotExists();
      }

      CosmosTableMetadata cosmosTableMetadata =
          convertToCosmosTableMetadata(getFullTableName(namespace, table), metadata);
      getMetadataContainer().upsertItem(cosmosTableMetadata);
    } catch (RuntimeException e) {
      throw new ExecutionException("Putting the table metadata failed", e);
    }
  }

  private void createMetadataDatabaseAndContainerIfNotExists() {
    ThroughputProperties manualThroughput =
        ThroughputProperties.createManualThroughput(Integer.parseInt(DEFAULT_REQUEST_UNIT));
    client.createDatabaseIfNotExists(metadataDatabase, manualThroughput);
    CosmosContainerProperties containerProperties =
        new CosmosContainerProperties(METADATA_CONTAINER, "/id");
    client.getDatabase(metadataDatabase).createContainerIfNotExists(containerProperties);
  }

  private CosmosContainer getMetadataContainer() {
    return client.getDatabase(metadataDatabase).getContainer(METADATA_CONTAINER);
  }

  private CosmosTableMetadata convertToCosmosTableMetadata(
      String fullTableName, TableMetadata tableMetadata) {
    Map<String, String> clusteringOrders =
        tableMetadata.getClusteringKeyNames().stream()
            .collect(Collectors.toMap(c -> c, c -> tableMetadata.getClusteringOrder(c).name()));
    Map<String, String> columnTypeByName = new HashMap<>();
    tableMetadata
        .getColumnNames()
        .forEach(
            columnName ->
                columnTypeByName.put(
                    columnName, tableMetadata.getColumnDataType(columnName).name().toLowerCase()));
    return CosmosTableMetadata.newBuilder()
        .id(fullTableName)
        .partitionKeyNames(tableMetadata.getPartitionKeyNames())
        .clusteringKeyNames(tableMetadata.getClusteringKeyNames())
        .clusteringOrders(clusteringOrders)
        .secondaryIndexNames(tableMetadata.getSecondaryIndexNames())
        .columns(columnTypeByName)
        .build();
  }

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    try {
      client.createDatabase(namespace, calculateThroughput(options));
    } catch (RuntimeException e) {
      throw new ExecutionException("Creating the database failed", e);
    }
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    CosmosDatabase database = client.getDatabase(namespace);
    try {
      database.getContainer(table).delete();
      deleteTableMetadata(namespace, table);
    } catch (RuntimeException e) {
      throw new ExecutionException("Deleting the container failed", e);
    }
  }

  private void deleteTableMetadata(String namespace, String table) throws ExecutionException {
    String fullTableName = getFullTableName(namespace, table);
    try {
      getMetadataContainer()
          .deleteItem(
              fullTableName, new PartitionKey(fullTableName), new CosmosItemRequestOptions());
      // Delete the metadata container and table if there is no more metadata stored
      if (!getMetadataContainer()
          .queryItems(
              "SELECT 1 FROM " + METADATA_CONTAINER + " OFFSET 0 LIMIT 1",
              new CosmosQueryRequestOptions(),
              Object.class)
          .stream()
          .findFirst()
          .isPresent()) {
        getMetadataContainer().delete();
        client.getDatabase(metadataDatabase).delete();
      }
    } catch (RuntimeException e) {
      throw new ExecutionException("Deleting the table metadata failed", e);
    }
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    try {
      Set<String> remainingTables = getRawTableNames(namespace);
      if (!remainingTables.isEmpty()) {
        throw new IllegalArgumentException(
            CoreError.NAMESPACE_WITH_NON_SCALARDB_TABLES_CANNOT_BE_DROPPED.buildMessage(
                namespace, remainingTables));
      }
      client.getDatabase(namespace).delete();
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new ExecutionException("Deleting the database failed", e);
    }
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    try {
      CosmosDatabase database = client.getDatabase(namespace);
      CosmosContainer container = database.getContainer(table);

      CosmosPagedIterable<Record> records =
          container.queryItems(
              "SELECT t." + ID + ", t." + CONCATENATED_PARTITION_KEY + " FROM " + "t",
              new CosmosQueryRequestOptions(),
              Record.class);
      records.forEach(
          record ->
              container.deleteItem(
                  record.getId(),
                  new PartitionKey(record.getConcatenatedPartitionKey()),
                  new CosmosItemRequestOptions()));
    } catch (RuntimeException e) {
      throw new ExecutionException("Truncating the container failed", e);
    }
  }

  @Override
  public void createIndex(
      String namespace, String table, String columnName, Map<String, String> options)
      throws ExecutionException {
    TableMetadata tableMetadata = getTableMetadata(namespace, table);
    TableMetadata newTableMetadata =
        TableMetadata.newBuilder(tableMetadata).addSecondaryIndex(columnName).build();

    updateIndexingPolicy(namespace, table, newTableMetadata);

    // update metadata
    putTableMetadata(namespace, table, newTableMetadata, true);
  }

  @Override
  public void dropIndex(String namespace, String table, String columnName)
      throws ExecutionException {
    TableMetadata tableMetadata = getTableMetadata(namespace, table);
    TableMetadata newTableMetadata =
        TableMetadata.newBuilder(tableMetadata).removeSecondaryIndex(columnName).build();

    updateIndexingPolicy(namespace, table, newTableMetadata);

    // update metadata
    putTableMetadata(namespace, table, newTableMetadata, true);
  }

  private void updateIndexingPolicy(
      String databaseName, String containerName, TableMetadata newTableMetadata)
      throws ExecutionException {
    updateIndexingPolicy(databaseName, containerName, newTableMetadata, false);
  }

  /**
   * Updates the container's indexing policy to match the given metadata. When {@code
   * skipWhenAlreadyUpToDate} is true (the repair path), the container's actual indexing policy is
   * compared against the desired one first and the (RU-consuming) container replace is skipped when
   * it already matches. The comparison is against the actual physical state -- not the ScalarDB
   * metadata -- because repair must fix the indexing policy even when the metadata already matches.
   */
  private void updateIndexingPolicy(
      String databaseName,
      String containerName,
      TableMetadata newTableMetadata,
      boolean skipWhenAlreadyUpToDate)
      throws ExecutionException {
    CosmosDatabase database = client.getDatabase(databaseName);
    try {
      // get the existing container properties
      CosmosContainerResponse response =
          database.createContainerIfNotExists(containerName, PARTITION_KEY_PATH);
      CosmosContainerProperties properties = response.getProperties();

      IndexingPolicy newIndexingPolicy = computeIndexingPolicy(newTableMetadata);
      if (skipWhenAlreadyUpToDate
          && indexingPolicyUpToDate(properties.getIndexingPolicy(), newIndexingPolicy)) {
        logger.debug(
            "The indexing policy for the {} container is already up to date; skipping the indexing policy update",
            getFullTableName(databaseName, containerName));
        return;
      }

      // set the new index policy to the container properties
      properties.setIndexingPolicy(newIndexingPolicy);

      // update the container properties
      database.getContainer(containerName).replace(properties);
    } catch (RuntimeException e) {
      throw new ExecutionException("Updating the indexing policy failed", e);
    }
  }

  /**
   * Returns whether the container's current indexing policy already matches the desired one on the
   * attributes ScalarDB controls: the set of included paths and the composite indexes. The included
   * paths cover the secondary indexes, plus the partition-key path when the table has no clustering
   * keys; the composite indexes cover the partition key and clustering keys when the table has
   * clustering keys. The excluded paths are not compared because Cosmos adds its own (e.g. for the
   * etag), which would cause spurious mismatches.
   */
  private static boolean indexingPolicyUpToDate(IndexingPolicy current, IndexingPolicy desired) {
    return includedPaths(current).equals(includedPaths(desired))
        && compositeIndexes(current).equals(compositeIndexes(desired));
  }

  private static Set<String> includedPaths(IndexingPolicy policy) {
    return policy.getIncludedPaths().stream()
        .map(IncludedPath::getPath)
        .collect(Collectors.toSet());
  }

  private static List<List<String>> compositeIndexes(IndexingPolicy policy) {
    return policy.getCompositeIndexes().stream()
        .map(
            paths ->
                paths.stream()
                    .map(path -> path.getPath() + " " + path.getOrder())
                    .collect(Collectors.toList()))
        .collect(Collectors.toList());
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    try {
      String fullName = getFullTableName(namespace, table);
      CosmosTableMetadata cosmosTableMetadata = readMetadata(fullName);
      if (cosmosTableMetadata == null) {
        return null;
      }
      return convertToTableMetadata(cosmosTableMetadata);
    } catch (RuntimeException e) {
      throw new ExecutionException("Getting the container metadata failed", e);
    }
  }

  private CosmosTableMetadata readMetadata(String fullName) {
    try {
      return getMetadataContainer()
          .readItem(fullName, new PartitionKey(fullName), CosmosTableMetadata.class)
          .getItem();
    } catch (NotFoundException e) {
      // The specified table is not found
      return null;
    }
  }

  private TableMetadata convertToTableMetadata(CosmosTableMetadata cosmosTableMetadata)
      throws ExecutionException {
    TableMetadata.Builder builder = TableMetadata.newBuilder();

    for (Entry<String, String> entry : cosmosTableMetadata.getColumns().entrySet()) {
      builder.addColumn(entry.getKey(), convertDataType(entry.getValue()));
    }
    cosmosTableMetadata.getPartitionKeyNames().forEach(builder::addPartitionKey);
    cosmosTableMetadata
        .getClusteringKeyNames()
        .forEach(
            n ->
                builder.addClusteringKey(
                    n, Order.valueOf(cosmosTableMetadata.getClusteringOrders().get(n))));
    cosmosTableMetadata.getSecondaryIndexNames().forEach(builder::addSecondaryIndex);
    return builder.build();
  }

  private DataType convertDataType(String columnType) throws ExecutionException {
    switch (columnType) {
      case "int":
        return DataType.INT;
      case "bigint":
        return DataType.BIGINT;
      case "float":
        return DataType.FLOAT;
      case "double":
        return DataType.DOUBLE;
      case "text":
        return DataType.TEXT;
      case "boolean":
        return DataType.BOOLEAN;
      case "blob":
        return DataType.BLOB;
      case "date":
        return DataType.DATE;
      case "time":
        return DataType.TIME;
      case "timestamp":
        return DataType.TIMESTAMP;
      case "timestamptz":
        return DataType.TIMESTAMPTZ;
      default:
        throw new ExecutionException("Unknown column type: " + columnType);
    }
  }

  @Override
  public void close() {
    client.close();
  }

  private ThroughputProperties calculateThroughput(Map<String, String> options) {
    int ru = Integer.parseInt(options.getOrDefault(REQUEST_UNIT, DEFAULT_REQUEST_UNIT));
    boolean noScaling = Boolean.parseBoolean(options.getOrDefault(NO_SCALING, DEFAULT_NO_SCALING));
    if (ru < 4000 || noScaling) {
      return ThroughputProperties.createManualThroughput(ru);
    } else {
      return ThroughputProperties.createAutoscaledThroughput(ru);
    }
  }

  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    if (metadataDatabase.equals(namespace)) {
      return true;
    }

    return databaseExists(namespace);
  }

  @Override
  public void repairTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    try {
      checkMetadata(metadata);
      // Always run the physical container repair. The metadata write is guarded by comparing the
      // stored ScalarDB metadata, while the indexing-policy update is guarded by comparing the
      // container's actual indexing policy (inside updateIndexingPolicy) -- repair must fix the
      // physical indexing policy even when the ScalarDB metadata already matches.
      createContainer(namespace, table, metadata, true);
      if (tableMetadataAlreadyUpToDate(namespace, table, metadata)) {
        logger.debug(
            "The metadata for the {} container is already up to date; skipping the metadata update",
            getFullTableName(namespace, table));
      } else {
        putTableMetadata(namespace, table, metadata, true);
      }
      updateIndexingPolicy(namespace, table, metadata, true);
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      throw new ExecutionException(
          String.format("Repairing the %s container failed", getFullTableName(namespace, table)),
          e);
    }
  }

  private boolean databaseExists(String id) throws ExecutionException {
    try {
      client.getDatabase(id).read();
    } catch (RuntimeException e) {
      if (e instanceof CosmosException
          && ((CosmosException) e).getStatusCode() == CosmosErrorCode.NOT_FOUND.get()) {
        return false;
      }
      throw new ExecutionException(String.format("Reading the database %s failed", id), e);
    }
    return true;
  }

  /**
   * Returns whether the stored table metadata already equals the desired metadata. Fails open: if
   * reading the current metadata throws (e.g. the metadata is corrupt and cannot be parsed), this
   * returns {@code false} so the caller rewrites the metadata rather than skipping the repair.
   */
  private boolean tableMetadataAlreadyUpToDate(
      String namespace, String table, TableMetadata metadata) {
    try {
      return metadata.equals(getTableMetadata(namespace, table));
    } catch (Exception e) {
      logger.debug(
          "Failed to read the stored metadata for the {} container; proceeding with the metadata update",
          getFullTableName(namespace, table),
          e);
      return false;
    }
  }

  @Override
  public Set<String> getNamespaceTableNames(String namespace) throws ExecutionException {
    try {
      if (!metadataContainerExists()) {
        return Collections.emptySet();
      }
      String selectAllDatabaseContainer =
          "SELECT * FROM "
              + METADATA_CONTAINER
              + " WHERE "
              + METADATA_CONTAINER
              + ".id LIKE '"
              + namespace
              + ".%'";
      return getMetadataContainer()
          .queryItems(
              selectAllDatabaseContainer,
              new CosmosQueryRequestOptions(),
              CosmosTableMetadata.class)
          .stream()
          .map(tableMetadata -> tableMetadata.getId().replaceFirst("^" + namespace + ".", ""))
          .collect(Collectors.toSet());
    } catch (RuntimeException e) {
      throw new ExecutionException("Retrieving the container names of the database failed", e);
    }
  }

  @Override
  public void addNewColumnToTable(
      String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException {
    try {
      TableMetadata currentTableMetadata = getTableMetadata(namespace, table);
      TableMetadata updatedTableMetadata =
          TableMetadata.newBuilder(currentTableMetadata).addColumn(columnName, columnType).build();
      putTableMetadata(namespace, table, updatedTableMetadata, true);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          String.format(
              "Adding the new column %s to the %s.%s table failed", columnName, namespace, table),
          e);
    }
  }

  @Override
  public void dropColumnFromTable(String namespace, String table, String columnName)
      throws ExecutionException {
    throw new UnsupportedOperationException(
        CoreError.COSMOS_DROP_COLUMN_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public void renameColumn(
      String namespace, String table, String oldColumnName, String newColumnName)
      throws ExecutionException {
    throw new UnsupportedOperationException(
        CoreError.COSMOS_RENAME_COLUMN_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public void alterColumnType(
      String namespace, String table, String columnName, DataType newColumnType)
      throws ExecutionException {
    throw new UnsupportedOperationException(
        CoreError.COSMOS_ALTER_COLUMN_TYPE_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public void renameTable(String namespace, String oldTableName, String newTableName)
      throws ExecutionException {
    throw new UnsupportedOperationException(
        CoreError.COSMOS_RENAME_TABLE_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public void importTable(
      String namespace,
      String table,
      Map<String, String> options,
      Map<String, DataType> overrideColumnsType) {
    throw new UnsupportedOperationException(CoreError.COSMOS_IMPORT_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public Set<String> getNamespaceNames() throws ExecutionException {
    try {
      if (!metadataContainerExists()) {
        return Collections.singleton(metadataDatabase);
      }

      Set<String> namespaceNames =
          getMetadataContainer()
              .queryItems(
                  "SELECT container.id FROM container",
                  new CosmosQueryRequestOptions(),
                  CosmosTableMetadata.class)
              .stream()
              .map(
                  tableMetadata ->
                      tableMetadata.getId().substring(0, tableMetadata.getId().indexOf('.')))
              .collect(Collectors.toSet());
      namespaceNames.add(metadataDatabase);

      return namespaceNames;
    } catch (RuntimeException e) {
      throw new ExecutionException("Retrieving the existing namespace names failed", e);
    }
  }

  private boolean metadataContainerExists() {
    try {
      client.getDatabase(metadataDatabase).getContainer(METADATA_CONTAINER).read();
    } catch (RuntimeException e) {
      if (e instanceof CosmosException
          && ((CosmosException) e).getStatusCode() == CosmosErrorCode.NOT_FOUND.get()) {
        return false;
      }
      throw e;
    }
    return true;
  }

  @Override
  public StorageInfo getStorageInfo(String namespace) {
    return STORAGE_INFO;
  }

  @Override
  public void createVirtualTable(
      String namespace,
      String table,
      String leftSourceNamespace,
      String leftSourceTable,
      String rightSourceNamespace,
      String rightSourceTable,
      VirtualTableJoinType joinType,
      Map<String, String> options) {
    throw new AssertionError("CommonDistributedStorageAdmin should not call this method");
  }

  @Override
  public Optional<VirtualTableInfo> getVirtualTableInfo(String namespace, String table) {
    // Virtual tables are not supported.
    return Optional.empty();
  }

  private Set<String> getRawTableNames(String namespace) {
    return client.getDatabase(namespace).readAllContainers().stream()
        .map(CosmosContainerProperties::getId)
        .collect(Collectors.toSet());
  }
}
