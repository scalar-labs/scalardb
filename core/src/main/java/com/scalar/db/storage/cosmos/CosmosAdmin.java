package com.scalar.db.storage.cosmos;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
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
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.StorageRuntimeException;
import com.scalar.db.util.Utility;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class CosmosAdmin implements DistributedStorageAdmin {
  public static final String RU = "ru";
  public static final String DEFAULT_RU = "400";
  public static final String NO_SCALING = "no-scaling";
  public static final String DEFAULT_NO_SCALING = "false";
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
  private final CosmosTableMetadataManager metadataManager;

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
    metadataManager = new CosmosTableMetadataManager(client, databasePrefix);
  }

  @VisibleForTesting
  CosmosAdmin(
      CosmosClient cosmosClient,
      CosmosTableMetadataManager metadataManager,
      Optional<String> databasePrefix) {
    this.client = cosmosClient;
    this.metadataManager = metadataManager;
    this.databasePrefix = databasePrefix;
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    try {
      createContainer(namespace, table, metadata);
      metadataManager.addTableMetadata(namespace, table, metadata);
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
      metadataManager.deleteTableMetadata(namespace, table);
    } catch (StorageRuntimeException e) {
      throw new ExecutionException("deleting the container failed", e);
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
    } catch (StorageRuntimeException e) {
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
      return metadataManager.getTableMetadata(namespace, table);
    } catch (StorageRuntimeException e) {
      throw new ExecutionException("getting the container metadata failed", e);
    }
  }

  private String fullDatabase(String database) {
    return Utility.getFullNamespaceName(databasePrefix, database);
  }

  @Override
  public void close() {
    client.close();
  }

  private ThroughputProperties calculateThroughput(Map<String, String> options) {
    int ru = Integer.parseInt(options.getOrDefault(RU, DEFAULT_RU));
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
      return metadataManager.getTableNames(namespace);
    } catch (RuntimeException e) {
      throw new ExecutionException("retrieving the container names of the database failed", e);
    }
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
