package com.scalar.db.storage.hbase;

import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import java.util.Map;
import java.util.Optional;

public class HBaseAdmin implements DistributedStorageAdmin {

  private final HBaseConnection hbaseConnection;
  private final HBaseTableMetadataManager tableMetadataManager;
  private final Optional<String> namespacePrefix;

  @Inject
  public HBaseAdmin(DatabaseConfig config) {
    String jdbcUrl = config.getContactPoints().get(0);
    hbaseConnection = new HBaseConnection(jdbcUrl);
    namespacePrefix = config.getNamespacePrefix();
    tableMetadataManager = new HBaseTableMetadataManager(hbaseConnection, namespacePrefix);
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options) {
    throw new UnsupportedOperationException("implement later");
  }

  @Override
  public void dropTable(String namespace, String table) {
    throw new UnsupportedOperationException("implement later");
  }

  @Override
  public void truncateTable(String namespace, String table) {
    throw new UnsupportedOperationException("implement later");
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) {
    return tableMetadataManager.getTableMetadata(fullNamespace(namespace), table);
  }

  private String fullNamespace(String namespace) {
    return namespacePrefix.map(s -> s + namespace).orElse(namespace);
  }

  @Override
  public void close() {}
}
