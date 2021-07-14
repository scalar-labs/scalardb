package com.scalar.db.storage.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.StorageRuntimeException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class JdbcDatabaseAdmin implements DistributedStorageAdmin {
  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcDatabaseAdmin.class);

  private final BasicDataSource dataSource;
  private final Optional<String> namespacePrefix;
  private final JdbcTableMetadataManager metadataManager;

  @Inject
  public JdbcDatabaseAdmin(JdbcDatabaseConfig config) {
    dataSource = JdbcUtils.initDataSource(config);
    namespacePrefix = config.getNamespacePrefix();
    RdbEngine rdbEngine = JdbcUtils.getRdbEngine(config.getContactPoints().get(0));
    metadataManager = new JdbcTableMetadataManager(dataSource, namespacePrefix, rdbEngine);
  }

  @VisibleForTesting
  public JdbcDatabaseAdmin(
      JdbcTableMetadataManager metadataManager, Optional<String> namespacePrefix) {
    dataSource = null;
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
    try {
      dataSource.close();
    } catch (SQLException e) {
      LOGGER.error("failed to close the dataSource", e);
    }
  }
}
