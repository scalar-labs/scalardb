package com.scalar.db.storage.cosmos;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.StorageRuntimeException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class CosmosAdmin implements DistributedStorageAdmin {

  private final CosmosClient client;
  private final Optional<String> namespacePrefix;
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
    namespacePrefix = config.getNamespacePrefix();
    metadataManager = new CosmosTableMetadataManager(client, namespacePrefix);
  }

  @VisibleForTesting
  CosmosAdmin(CosmosTableMetadataManager metadataManager, Optional<String> namespacePrefix) {
    client = null;
    this.metadataManager = metadataManager;
    this.namespacePrefix = namespacePrefix.map(n -> n + "_");
  }

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    throw new UnsupportedOperationException("implement later");
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    throw new UnsupportedOperationException("implement later");
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    throw new UnsupportedOperationException("implement later");
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    throw new UnsupportedOperationException("implement later");
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    throw new UnsupportedOperationException("implement later");
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    try {
      return metadataManager.getTableMetadata(fullNamespace(namespace), table);
    } catch (StorageRuntimeException e) {
      throw new ExecutionException("getting a table metadata failed", e);
    }
  }

  @Override
  public Set<String> getNamespaceTableNames(String namespace) throws ExecutionException {
    throw new UnsupportedOperationException("implement later");
  }

  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    throw new UnsupportedOperationException("implement later");
  }

  private String fullNamespace(String namespace) {
    return namespacePrefix.map(s -> s + namespace).orElse(namespace);
  }

  @Override
  public void close() {
    client.close();
  }
}
