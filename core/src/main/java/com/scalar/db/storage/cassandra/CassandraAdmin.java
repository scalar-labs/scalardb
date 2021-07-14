package com.scalar.db.storage.cassandra;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.StorageRuntimeException;
import java.util.Map;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class CassandraAdmin implements DistributedStorageAdmin {

  private final ClusterManager clusterManager;
  private final CassandraTableMetadataManager metadataManager;
  private final Optional<String> namespacePrefix;

  @Inject
  public CassandraAdmin(DatabaseConfig config) {
    clusterManager = new ClusterManager(config);
    clusterManager.getSession();
    metadataManager = new CassandraTableMetadataManager(clusterManager);
    namespacePrefix = config.getNamespacePrefix();
  }

  @VisibleForTesting
  CassandraAdmin(CassandraTableMetadataManager metadataManager, Optional<String> namespacePrefix) {
    clusterManager = null;
    this.metadataManager = metadataManager;
    this.namespacePrefix = namespacePrefix.map(n -> n + "_");
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

  private String fullNamespace(String namespace) {
    return namespacePrefix.map(s -> s + namespace).orElse(namespace);
  }

  @Override
  public void close() {
    clusterManager.close();
  }
}
