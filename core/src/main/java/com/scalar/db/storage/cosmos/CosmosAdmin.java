package com.scalar.db.storage.cosmos;

import static com.scalar.db.util.Utility.getFullTableName;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.implementation.NotFoundException;
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
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.UnsupportedTypeException;
import com.scalar.db.io.DataType;
import com.scalar.db.util.Utility;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
  private static final String CONTAINER_PARTITION_KEY = "/" + CONCATENATED_PARTITION_KEY;
  private static final String PARTITION_KEY_PATH = CONTAINER_PARTITION_KEY + "/?";
  private static final String CLUSTERING_KEY_PATH = "/clusteringKey/*";
  private static final String SECONDARY_INDEX_KEY_PATH = "/values/";
  private static final String EXCLUDED_PATH = "/*";
  private static final String STORED_PROCEDURE_FILE_NAME = "mutate.js";
  private static final String STORED_PROCEDURE_PATH =
      "cosmosdb_stored_procedure/" + STORED_PROCEDURE_FILE_NAME;

  private final CosmosClient client;
  private final Optional<String> databasePrefix;

  @Inject
  public CosmosAdmin(DatabaseConfig config) {
    client =
        new CosmosClientBuilder()
            .endpoint(config.getContactPoints().get(0))
            .key(config.getPassword().orElse(null))
            .directMode()
            .consistencyLevel(ConsistencyLevel.STRONG)
            .buildClient();
    databasePrefix = config.getNamespacePrefix();
  }

  public CosmosAdmin(CosmosClient client, DatabaseConfig config) {
    this.client = client;
    databasePrefix = config.getNamespacePrefix();
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    try {
      createContainer(namespace, table, metadata);
      addTableMetadata(namespace, table, metadata);
    } catch (RuntimeException e) {
      throw new ExecutionException("creating the container failed", e);
    }
  }

  private void createContainer(String database, String table, TableMetadata metadata)
      throws ExecutionException {
    if (!databaseExists(fullDatabase(database))) {
      throw new ExecutionException("the database does not exists");
    }
    CosmosDatabase cosmosDatabase = client.getDatabase(fullDatabase(database));
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
    ArrayList<IncludedPath> paths = new ArrayList<>();
    paths.add(new IncludedPath(PARTITION_KEY_PATH));
    paths.add(new IncludedPath(CLUSTERING_KEY_PATH));
    paths.addAll(
        metadata.getSecondaryIndexNames().stream()
            .map(index -> new IncludedPath(SECONDARY_INDEX_KEY_PATH + index + "/?"))
            .collect(Collectors.toList()));
    indexingPolicy.setIncludedPaths(paths);
    indexingPolicy.setExcludedPaths(Collections.singletonList(new ExcludedPath(EXCLUDED_PATH)));

    return new CosmosContainerProperties(table, CONTAINER_PARTITION_KEY)
        .setIndexingPolicy(indexingPolicy);
  }

  private void addTableMetadata(String namespace, String table, TableMetadata metadata)
      throws ExecutionException {
    try {
      createMetadataDatabaseAndContainerIfNotExists();

      CosmosTableMetadata cosmosTableMetadata =
          convertToCosmosTableMetadata(
              getFullTableName(databasePrefix, namespace, table), metadata);
      getContainer().upsertItem(cosmosTableMetadata);
    } catch (RuntimeException e) {
      throw new ExecutionException("adding the table metadata failed", e);
    }
  }

  private void createMetadataDatabaseAndContainerIfNotExists() {
    String metadataDatabase = fullMetadataDatabase();
    ThroughputProperties manualThroughput =
        ThroughputProperties.createManualThroughput(Integer.parseInt(DEFAULT_REQUEST_UNIT));
    client.createDatabaseIfNotExists(metadataDatabase, manualThroughput);
    CosmosContainerProperties containerProperties =
        new CosmosContainerProperties(METADATA_CONTAINER, "/id");
    client.getDatabase(metadataDatabase).createContainerIfNotExists(containerProperties);
  }

  private CosmosContainer getContainer() {
    return client.getDatabase(fullMetadataDatabase()).getContainer(METADATA_CONTAINER);
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

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    try {
      client.createDatabase(fullDatabase(namespace), calculateThroughput(options));
    } catch (RuntimeException e) {
      throw new ExecutionException("creating the database failed", e);
    }
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    String fullDatabase = fullDatabase(namespace);
    if (!databaseExists(fullDatabase)) {
      throw new ExecutionException("the database does not exist");
    }
    CosmosDatabase database = client.getDatabase(fullDatabase);
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
    String fullTableName = getFullTableName(databasePrefix, namespace, table);
    try {
      getContainer()
          .deleteItem(
              fullTableName, new PartitionKey(fullTableName), new CosmosItemRequestOptions());
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
        client.getDatabase(fullMetadataDatabase()).delete();
      }
    } catch (RuntimeException e) {
      throw new ExecutionException("deleting the table metadata failed", e);
    }
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    String fullDatabase = fullDatabase(namespace);
    if (!databaseExists(fullDatabase)) {
      throw new ExecutionException("the database does not exist");
    }

    try {
      client.getDatabase(fullDatabase).delete();
    } catch (RuntimeException e) {
      throw new ExecutionException("deleting the database failed", e);
    }
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    String fullDatabase = fullDatabase(namespace);
    try {
      CosmosDatabase database = client.getDatabase(fullDatabase);
      CosmosContainer container = database.getContainer(table);

      CosmosPagedIterable<Record> records =
          container.queryItems(
              "SELECT t." + ID + ", t." + CONCATENATED_PARTITION_KEY + " FROM " + table + " t",
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
      String fullName = getFullTableName(databasePrefix, namespace, table);
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
      return getContainer()
          .readItem(fullName, new PartitionKey(fullName), CosmosTableMetadata.class)
          .getItem();
    } catch (NotFoundException e) {
      // The specified table is not found
      return null;
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

  private String fullDatabase(String database) {
    return Utility.getFullNamespaceName(databasePrefix, database);
  }

  private String fullMetadataDatabase() {
    return Utility.getFullNamespaceName(databasePrefix, METADATA_DATABASE);
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
    return databaseExists(fullDatabase(namespace));
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
      String fullDatabase = fullDatabase(namespace);
      String selectAllDatabaseContainer =
          "SELECT * FROM "
              + METADATA_CONTAINER
              + " WHERE "
              + METADATA_CONTAINER
              + ".id LIKE '"
              + fullDatabase
              + ".%'";
      return getContainer()
          .queryItems(
              selectAllDatabaseContainer,
              new CosmosQueryRequestOptions(),
              CosmosTableMetadata.class)
          .stream()
          .map(tableMetadata -> tableMetadata.getId().replaceFirst("^" + fullDatabase + ".", ""))
          .collect(Collectors.toSet());
    } catch (RuntimeException e) {
      throw new ExecutionException("retrieving the container names of the database failed", e);
    }
  }

  private boolean metadataContainerExists() {
    try {
      client.getDatabase(fullMetadataDatabase()).getContainer(METADATA_CONTAINER).read();
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
