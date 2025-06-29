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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class CosmosAdmin implements DistributedStorageAdmin {
  public static final String REQUEST_UNIT = "ru";
  public static final String DEFAULT_REQUEST_UNIT = "400";
  public static final String NO_SCALING = "no-scaling";
  public static final String DEFAULT_NO_SCALING = "false";

  public static final String TABLE_METADATA_CONTAINER = "metadata";
  public static final String NAMESPACES_CONTAINER = "namespaces";
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
          Integer.MAX_VALUE);

  private final CosmosClient client;
  private final String metadataDatabase;

  @Inject
  public CosmosAdmin(DatabaseConfig databaseConfig) {
    CosmosConfig config = new CosmosConfig(databaseConfig);
    client = CosmosUtils.buildCosmosClient(config);
    metadataDatabase = config.getMetadataDatabase();
  }

  CosmosAdmin(CosmosClient client, CosmosConfig config) {
    this.client = client;
    metadataDatabase = config.getMetadataDatabase();
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    try {
      createMetadataDatabaseAndNamespaceContainerIfNotExists();
      createTableInternal(namespace, table, metadata, false);
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      throw new ExecutionException(
          String.format("Creating the %s container failed", getFullTableName(namespace, table)), e);
    }
  }

  private void createTableInternal(
      String namespace, String table, TableMetadata metadata, boolean ifNotExists)
      throws ExecutionException {
    checkMetadata(metadata);
    createContainer(namespace, table, metadata, ifNotExists);
    upsertTableMetadata(namespace, table, metadata);
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
      if (e.getStatusCode() == CosmosErrorCode.NOT_FOUND.get()) {
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

  private void upsertTableMetadata(String namespace, String table, TableMetadata metadata)
      throws ExecutionException {
    try {
      createMetadataDatabaseAndTableMetadataContainerIfNotExists();

      CosmosTableMetadata cosmosTableMetadata =
          convertToCosmosTableMetadata(getFullTableName(namespace, table), metadata);
      getTableMetadataContainer().upsertItem(cosmosTableMetadata);
    } catch (RuntimeException e) {
      throw new ExecutionException(
          String.format(
              "Putting the table metadata for the %s contained failed",
              getFullTableName(namespace, table)),
          e);
    }
  }

  private void createMetadataDatabaseAndTableMetadataContainerIfNotExists() {
    ThroughputProperties manualThroughput =
        ThroughputProperties.createManualThroughput(Integer.parseInt(DEFAULT_REQUEST_UNIT));
    client.createDatabaseIfNotExists(metadataDatabase, manualThroughput);
    CosmosContainerProperties containerProperties =
        new CosmosContainerProperties(TABLE_METADATA_CONTAINER, "/id");
    client.getDatabase(metadataDatabase).createContainerIfNotExists(containerProperties);
  }

  private CosmosContainer getTableMetadataContainer() {
    return client.getDatabase(metadataDatabase).getContainer(TABLE_METADATA_CONTAINER);
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
      createMetadataDatabaseAndNamespaceContainerIfNotExists();
      getNamespacesContainer().createItem(new CosmosNamespace(namespace));
    } catch (RuntimeException e) {
      throw new ExecutionException(String.format("Creating the %s database failed", namespace), e);
    }
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    CosmosDatabase database = client.getDatabase(namespace);
    try {
      database.getContainer(table).delete();
      deleteTableMetadata(namespace, table);
      deleteMetadataDatabaseIfEmpty();
    } catch (RuntimeException e) {
      throw new ExecutionException(
          String.format("Deleting the %s container failed", getFullTableName(namespace, table)), e);
    }
  }

  private void deleteTableMetadata(String namespace, String table) throws ExecutionException {
    String fullTableName = getFullTableName(namespace, table);
    try {
      getTableMetadataContainer()
          .deleteItem(
              fullTableName, new PartitionKey(fullTableName), new CosmosItemRequestOptions());
      // Delete the table metadata container if there is no more metadata stored
      if (!getTableMetadataContainer()
          .queryItems(
              "SELECT 1 FROM " + TABLE_METADATA_CONTAINER + " OFFSET 0 LIMIT 1",
              new CosmosQueryRequestOptions(),
              Object.class)
          .stream()
          .findFirst()
          .isPresent()) {
        getTableMetadataContainer().delete();
      }
    } catch (RuntimeException e) {
      throw new ExecutionException(
          String.format(
              "Deleting the table metadata for the %s container failed",
              getFullTableName(namespace, table)),
          e);
    }
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    try {
      client.getDatabase(namespace).delete();
      getNamespacesContainer()
          .deleteItem(new CosmosNamespace(namespace), new CosmosItemRequestOptions());
      deleteMetadataDatabaseIfEmpty();
    } catch (RuntimeException e) {
      throw new ExecutionException(String.format("Deleting the %s database failed", namespace), e);
    }
  }

  private void deleteMetadataDatabaseIfEmpty() {
    Set<String> namespaces =
        getNamespacesContainer()
            .queryItems(
                "SELECT * FROM container OFFSET 0 LIMIT 2",
                new CosmosQueryRequestOptions(),
                CosmosNamespace.class)
            .stream()
            .map(CosmosNamespace::getId)
            .collect(Collectors.toSet());

    boolean onlyMetadataNamespaceLeft =
        namespaces.size() == 1 && namespaces.contains(metadataDatabase);
    if (!onlyMetadataNamespaceLeft) {
      return;
    }

    // Delete the metadata database if there is only the namespaces container left
    CosmosDatabase database = client.getDatabase(metadataDatabase);
    Iterator<CosmosContainerProperties> iterator = database.readAllContainers().iterator();

    Set<String> containers = new HashSet<>();
    int count = 0;
    while (iterator.hasNext()) {
      containers.add(iterator.next().getId());
      // Only need to fetch the first two containers
      if (count++ == 2) {
        break;
      }
    }
    if (containers.size() == 1 && containers.contains(NAMESPACES_CONTAINER)) {
      database.delete();
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
      throw new ExecutionException(
          String.format("Truncating the %s container failed", getFullTableName(namespace, table)),
          e);
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
    upsertTableMetadata(namespace, table, newTableMetadata);
  }

  @Override
  public void dropIndex(String namespace, String table, String columnName)
      throws ExecutionException {
    TableMetadata tableMetadata = getTableMetadata(namespace, table);
    TableMetadata newTableMetadata =
        TableMetadata.newBuilder(tableMetadata).removeSecondaryIndex(columnName).build();

    updateIndexingPolicy(namespace, table, newTableMetadata);

    // update metadata
    upsertTableMetadata(namespace, table, newTableMetadata);
  }

  private void updateIndexingPolicy(
      String databaseName, String containerName, TableMetadata newTableMetadata)
      throws ExecutionException {
    CosmosDatabase database = client.getDatabase(databaseName);
    try {
      // get the existing container properties
      CosmosContainerResponse response =
          database.createContainerIfNotExists(containerName, PARTITION_KEY_PATH);
      CosmosContainerProperties properties = response.getProperties();

      // set the new index policy to the container properties
      properties.setIndexingPolicy(computeIndexingPolicy(newTableMetadata));

      // update the container properties
      database.getContainer(containerName).replace(properties);
    } catch (RuntimeException e) {
      throw new ExecutionException(
          String.format(
              "Updating the indexing policy for the %s table failed",
              getFullTableName(databaseName, containerName)),
          e);
    }
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    try {
      String fullName = getFullTableName(namespace, table);
      CosmosTableMetadata cosmosTableMetadata = readTableMetadata(fullName);
      if (cosmosTableMetadata == null) {
        return null;
      }
      return convertToTableMetadata(cosmosTableMetadata);
    } catch (RuntimeException e) {
      throw new ExecutionException(
          String.format(
              "Getting the table metadata for the %s container failed",
              getFullTableName(namespace, table)),
          e);
    }
  }

  private CosmosTableMetadata readTableMetadata(String fullName) {
    try {
      return getTableMetadataContainer()
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
    try {
      getNamespacesContainer()
          .readItem(namespace, new PartitionKey(namespace), CosmosNamespace.class);
      return true;
    } catch (RuntimeException e) {
      if (e instanceof CosmosException
          && ((CosmosException) e).getStatusCode() == CosmosErrorCode.NOT_FOUND.get()) {
        return false;
      }
      throw new ExecutionException(
          String.format("Checking if the %s database exists failed", namespace), e);
    }
  }

  @Override
  public void repairNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    try {
      client.createDatabaseIfNotExists(namespace, calculateThroughput(options));
      createMetadataDatabaseAndNamespaceContainerIfNotExists();
      getNamespacesContainer().upsertItem(new CosmosNamespace(namespace));
    } catch (CosmosException e) {
      throw new ExecutionException(String.format("Repairing the %s database failed", namespace), e);
    }
  }

  @Override
  public void repairTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    try {
      createTableInternal(namespace, table, metadata, true);
      updateIndexingPolicy(namespace, table, metadata);
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      throw new ExecutionException(
          String.format("Repairing the %s container failed", getFullTableName(namespace, table)),
          e);
    }
  }

  @Override
  public Set<String> getNamespaceTableNames(String namespace) throws ExecutionException {
    try {
      if (!tableMetadataContainerExists()) {
        return Collections.emptySet();
      }
      String selectAllDatabaseContainer =
          "SELECT * FROM "
              + TABLE_METADATA_CONTAINER
              + " WHERE "
              + TABLE_METADATA_CONTAINER
              + ".id LIKE '"
              + namespace
              + ".%'";
      return getTableMetadataContainer()
          .queryItems(
              selectAllDatabaseContainer,
              new CosmosQueryRequestOptions(),
              CosmosTableMetadata.class)
          .stream()
          .map(tableMetadata -> tableMetadata.getId().replaceFirst("^" + namespace + ".", ""))
          .collect(Collectors.toSet());
    } catch (RuntimeException e) {
      throw new ExecutionException(
          String.format("Retrieving the container names of the %s database failed", namespace), e);
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
      upsertTableMetadata(namespace, table, updatedTableMetadata);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          String.format(
              "Adding the new %s column to the %s container failed",
              columnName, getFullTableName(namespace, table)),
          e);
    }
  }

  @Override
  public TableMetadata getImportTableMetadata(
      String namespace, String table, Map<String, DataType> overrideColumnsType) {
    throw new UnsupportedOperationException(CoreError.COSMOS_IMPORT_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public void addRawColumnToTable(
      String namespace, String table, String columnName, DataType columnType) {
    throw new UnsupportedOperationException(CoreError.COSMOS_IMPORT_NOT_SUPPORTED.buildMessage());
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
      if (!namespacesContainerExists()) {
        return Collections.emptySet();
      }

      CosmosPagedIterable<CosmosNamespace> allNamespaces =
          getNamespacesContainer()
              .queryItems(
                  "SELECT * FROM container",
                  new CosmosQueryRequestOptions(),
                  CosmosNamespace.class);

      return allNamespaces.stream().map(CosmosNamespace::getId).collect(Collectors.toSet());
    } catch (RuntimeException e) {
      throw new ExecutionException("Retrieving the database names failed", e);
    }
  }

  @Override
  public void upgrade(Map<String, String> options) throws ExecutionException {
    try {
      if (!tableMetadataContainerExists()) {
        return;
      }
      createMetadataDatabaseAndNamespaceContainerIfNotExists();

      // Upsert namespace of existing tables in the "namespaces" container
      getTableMetadataContainer()
          .queryItems(
              "SELECT container.id FROM container",
              new CosmosQueryRequestOptions(),
              CosmosTableMetadata.class)
          .stream()
          .map(
              tableMetadata ->
                  tableMetadata.getId().substring(0, tableMetadata.getId().indexOf('.')))
          .distinct()
          .forEach(
              namespaceName ->
                  getNamespacesContainer().upsertItem(new CosmosNamespace(namespaceName)));
    } catch (RuntimeException e) {
      throw new ExecutionException("Upgrading the ScalarDB environment failed", e);
    }
  }

  private void createMetadataDatabaseAndNamespaceContainerIfNotExists() {
    if (containerExists(metadataDatabase, NAMESPACES_CONTAINER)) {
      return;
    }

    ThroughputProperties manualThroughput =
        ThroughputProperties.createManualThroughput(Integer.parseInt(DEFAULT_REQUEST_UNIT));
    client.createDatabaseIfNotExists(metadataDatabase, manualThroughput);
    client.getDatabase(metadataDatabase).createContainerIfNotExists(NAMESPACES_CONTAINER, "/id");

    // Insert the system namespace to the namespaces table
    getNamespacesContainer().createItem(new CosmosNamespace(metadataDatabase));
  }

  private CosmosContainer getNamespacesContainer() {
    return client.getDatabase(metadataDatabase).getContainer(NAMESPACES_CONTAINER);
  }

  private boolean tableMetadataContainerExists() {
    return containerExists(metadataDatabase, TABLE_METADATA_CONTAINER);
  }

  private boolean namespacesContainerExists() {
    return containerExists(metadataDatabase, NAMESPACES_CONTAINER);
  }

  private boolean containerExists(String databaseId, String containerId) throws CosmosException {
    try {
      client.getDatabase(databaseId).getContainer(containerId).read();
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
}
