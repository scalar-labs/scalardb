package com.scalar.db.transaction.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.JdbcAdmin;
import java.util.Map;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class JdbcTransactionAdmin implements DistributedTransactionAdmin {

  private final JdbcAdmin admin;

  @Inject
  public JdbcTransactionAdmin(DatabaseConfig databaseConfig) {
    admin = new JdbcAdmin(databaseConfig);
  }

  @VisibleForTesting
  JdbcTransactionAdmin(JdbcAdmin admin) {
    this.admin = admin;
  }

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    admin.createNamespace(namespace, options);
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    admin.createTable(namespace, table, metadata, options);
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    admin.dropTable(namespace, table);
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    admin.dropNamespace(namespace);
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    admin.truncateTable(namespace, table);
  }

  @Override
  public void createIndex(
      String namespace, String table, String columnName, Map<String, String> options)
      throws ExecutionException {
    admin.createIndex(namespace, table, columnName, options);
  }

  @Override
  public void dropIndex(String namespace, String table, String columnName)
      throws ExecutionException {
    admin.dropIndex(namespace, table, columnName);
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    return admin.getTableMetadata(namespace, table);
  }

  @Override
  public Set<String> getNamespaceTableNames(String namespace) throws ExecutionException {
    return admin.getNamespaceTableNames(namespace);
  }

  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    return admin.namespaceExists(namespace);
  }

  @Override
  public void createCoordinatorTables(Map<String, String> options) {
    // Do nothing since JDBC transactions don't have coordinator tables
  }

  @Override
  public void dropCoordinatorTables() {
    // Do nothing since JDBC transactions don't have coordinator tables
  }

  @Override
  public void truncateCoordinatorTables() {
    // Do nothing since JDBC transactions don't have coordinator tables
  }

  @Override
  public boolean coordinatorTablesExist() {
    // Always return true since JDBC transactions don't have coordinator tables
    return true;
  }

  @Override
  public void repairTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    admin.repairTable(namespace, table, metadata, options);
  }

  @Override
  public void repairCoordinatorTables(Map<String, String> options) {
    // Do nothing since JDBC transactions don't have coordinator tables
  }

  @Override
  public void addNewColumnToTable(
      String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException {
    admin.addNewColumnToTable(namespace, table, columnName, columnType);
  }

  @Override
  public Set<String> getNamespaceNames() throws ExecutionException {
    return admin.getNamespaceNames();
  }

  @Override
  public void close() {
    admin.close();
  }
}
