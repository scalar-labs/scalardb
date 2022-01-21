package com.scalar.db.storage.cosmos;

import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.implementation.NotFoundException;
import com.azure.cosmos.models.CompositePath;
import com.azure.cosmos.models.CompositePathSortOrder;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.CosmosStoredProcedureProperties;
import com.azure.cosmos.models.ExcludedPath;
import com.azure.cosmos.models.IncludedPath;
import com.azure.cosmos.models.IndexingPolicy;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
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
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class CosmosAdmin implements DistributedStorageAdmin {
  public static final String REQUEST_UNIT = "ru";
  public static final String DEFAULT_REQUEST_UNIT = "400";
  public static final String NO_SCALING = "no-scaling";
  public static final String DEFAULT_NO_SCALING = "false";

  public static final String METADATA_DATABASE = "scalardb";
  public static final String METADATA_CONTAINER = "metadata";
  private static final String ID = "id";
  private static final String CONCATENATED_PARTITION_KEY = "concatenatedPartitionKey";
  private static final String PARTITION_KEY_PATH = "/" + CONCATENATED_PARTITION_KEY;
  private static final String CLUSTERING_KEY_PATH_PREFIX = "/clusteringKey/";
  private static final String SECONDARY_INDEX_KEY_PATH_PREFIX = "/values/";
  private static final String EXCLUDED_PATH = "/*";
  private static final String STORED_PROCEDURE_FILE_NAME = "mutate.js";
  private static final String STORED_PROCEDURE_PATH =
      "cosmosdb_stored_procedure/" + STORED_PROCEDURE_FILE_NAME;

  private final CosmosClient client;
  private final String metadataDatabase;

  @Inject
  public CosmosAdmin(CosmosConfig config) {
    client =
        new CosmosClientBuilder()
            .endpoint(config.getContactPoints().get(0))
            .key(config.getPassword().orElse(null))
            .directMode()
            .consistencyLevel(ConsistencyLevel.STRONG)
            .buildClient();
    metadataDatabase = config.getTableMetadataDatabase().orElse(METADATA_DATABASE);
  }

  public CosmosAdmin(CosmosClient client, CosmosConfig config) {
    this.client = client;
    metadataDatabase = config.getTableMetadataDatabase().orElse(METADATA_DATABASE);
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    checkMetadata(metadata);
    try {
      createContainer(namespace, table, metadata);
      addTableMetadata(namespace, table, metadata);
    } catch (RuntimeException e) {
      throw new ExecutionException("creating the container failed", e);
    }
  }

  private void checkMetadata(TableMetadata metadata) {
    for (String clusteringKeyName : metadata.getClusteringKeyNames()) {
      if (metadata.getColumnDataType(clusteringKeyName) == DataType.BLOB) {
        throw new IllegalArgumentException(
            "Currently, BLOB type is not supported for clustering keys in Cosmos DB");
      }
    }
  }

  private void createContainer(String database, String table, TableMetadata metadata)
      throws ExecutionException {
    if (!databaseExists(database)) {
      throw new ExecutionException("the database does not exists");
    }
    CosmosDatabase cosmosDatabase = client.getDatabase(database);
    CosmosContainerProperties properties = computeContainerProperties(table, metadata);
    cosmosDatabase.createContainer(properties);

    CosmosStoredProcedureProperties storedProcedureProperties =
        computeContainerStoredProcedureProperties();
    cosmosDatabase
        .getContainer(table)
        .getScripts()
        .createStoredProcedure(storedProcedureProperties);
  }

  private CosmosStoredProcedureProperties computeContainerStoredProcedureProperties()
      throws ExecutionException {
    String storedProcedure;
    try (InputStream storedProcedureInputStream =
            Objects.requireNonNull(
                getClass().getClassLoader().getResourceAsStream(STORED_PROCEDURE_PATH));
        InputStreamReader inputStreamReader =
            new InputStreamReader(storedProcedureInputStream, StandardCharsets.UTF_8);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
      storedProcedure =
          bufferedReader.lines().reduce("", (prev, cur) -> prev + cur + System.lineSeparator());
    } catch (IOException | NullPointerException e) {
      throw new ExecutionException("reading the stored procedure failed", e);
    }
    return new CosmosStoredProcedureProperties(STORED_PROCEDURE_FILE_NAME, storedProcedure);
  }

  private CosmosContainerProperties computeContainerProperties(
      String table, TableMetadata metadata) {
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

    return new CosmosContainerProperties(table, PARTITION_KEY_PATH)
        .setIndexingPolicy(indexingPolicy);
  }

  private void addTableMetadata(String namespace, String table, TableMetadata metadata)
      throws ExecutionException {
    try {
      createMetadataDatabaseAndContainerIfNotExists();

      CosmosTableMetadata cosmosTableMetadata =
          convertToCosmosTableMetadata(getFullTableName(namespace, table), metadata);
      getMetadataContainer().upsertItem(cosmosTableMetadata);
    } catch (RuntimeException e) {
      throw new ExecutionException("adding the table metadata failed", e);
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
    CosmosTableMetadata cosmosTableMetadata = new CosmosTableMetadata();
    cosmosTableMetadata.setId(fullTableName);
    cosmosTableMetadata.setPartitionKeyNames(new ArrayList<>(tableMetadata.getPartitionKeyNames()));
    cosmosTableMetadata.setClusteringKeyNames(
        new ArrayList<>(tableMetadata.getClusteringKeyNames()));
    cosmosTableMetadata.setClusteringOrders(
        tableMetadata.getClusteringKeyNames().stream()
            .collect(Collectors.toMap(c -> c, c -> tableMetadata.getClusteringOrder(c).name())));
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

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    try {
      client.createDatabase(namespace, calculateThroughput(options));
    } catch (RuntimeException e) {
      throw new ExecutionException("creating the database failed", e);
    }
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    if (!databaseExists(namespace)) {
      throw new ExecutionException("the database does not exist");
    }
    CosmosDatabase database = client.getDatabase(namespace);
    if (!containerExists(database, table)) {
      throw new ExecutionException("the container does not exist");
    }

    try {
      database.getContainer(table).delete();
      deleteTableMetadata(namespace, table);
    } catch (RuntimeException e) {
      throw new ExecutionException("deleting the container failed", e);
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
      throw new ExecutionException("deleting the table metadata failed", e);
    }
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    if (!databaseExists(namespace)) {
      throw new ExecutionException("the database does not exist");
    }

    try {
      client.getDatabase(namespace).delete();
    } catch (RuntimeException e) {
      throw new ExecutionException("deleting the database failed", e);
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
      throw new ExecutionException("truncating the container failed", e);
    }
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
      throw new ExecutionException("getting the container metadata failed", e);
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
      default:
        throw new ExecutionException("unknown column type: " + columnType);
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
    return databaseExists(namespace);
  }

  private boolean databaseExists(String id) throws ExecutionException {
    try {
      client.getDatabase(id).read();
    } catch (RuntimeException e) {
      if (e instanceof CosmosException
          && ((CosmosException) e).getStatusCode() == CosmosErrorCode.NOT_FOUND.get()) {
        return false;
      }
      throw new ExecutionException(String.format("reading the database %s failed", id), e);
    }
    return true;
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
      throw new ExecutionException("retrieving the container names of the database failed", e);
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

  private boolean containerExists(CosmosDatabase database, String containerId)
      throws ExecutionException {
    try {
      database.getContainer(containerId).read();
    } catch (RuntimeException e) {
      if (e instanceof CosmosException
          && ((CosmosException) e).getStatusCode() == CosmosErrorCode.NOT_FOUND.get()) {
        return false;
      }
      throw new ExecutionException(
          String.format("reading the container %s failed", containerId), e);
    }
    return true;
  }
}
