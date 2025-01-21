package com.scalar.db.transaction.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.CheckedDistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.JdbcAdmin;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.jdbc.JdbcUtils;
import java.util.Map;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class JdbcTransactionAdmin implements DistributedTransactionAdmin {

  private final DistributedStorageAdmin jdbcAdmin;

  @Inject
  public JdbcTransactionAdmin(DatabaseConfig databaseConfig) {
    // If the database is SQLite, the namespace check is skipped because SQLite does not support
    // namespaces.
    boolean isSqlite = JdbcUtils.isSqlite(new JdbcConfig(databaseConfig));
    jdbcAdmin = new CheckedDistributedStorageAdmin(new JdbcAdmin(databaseConfig), !isSqlite);
  }

  @VisibleForTesting
  JdbcTransactionAdmin(JdbcAdmin jdbcAdmin) {
    this.jdbcAdmin = jdbcAdmin;
  }

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    jdbcAdmin.createNamespace(namespace, options);
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    jdbcAdmin.createTable(namespace, table, metadata, options);
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    jdbcAdmin.dropTable(namespace, table);
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    jdbcAdmin.dropNamespace(namespace);
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    jdbcAdmin.truncateTable(namespace, table);
  }

  @Override
  public void createIndex(
      String namespace, String table, String columnName, Map<String, String> options)
      throws ExecutionException {
    jdbcAdmin.createIndex(namespace, table, columnName, options);
  }

  @Override
  public void dropIndex(String namespace, String table, String columnName)
      throws ExecutionException {
    jdbcAdmin.dropIndex(namespace, table, columnName);
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    return jdbcAdmin.getTableMetadata(namespace, table);
  }

  @Override
  public Set<String> getNamespaceTableNames(String namespace) throws ExecutionException {
    return jdbcAdmin.getNamespaceTableNames(namespace);
  }

  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    return jdbcAdmin.namespaceExists(namespace);
  }

  @Override
  public void importTable(
      String namespace,
      String table,
      Map<String, String> options,
      Map<String, DataType> overrideColumnsType)
      throws ExecutionException {
    jdbcAdmin.importTable(namespace, table, options, overrideColumnsType);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Note that it does nothing since the JDBC transactions don't have coordinator tables.
   */
  @Override
  public void createCoordinatorTables(Map<String, String> options) {
    // Do nothing since JDBC transactions don't have coordinator tables
  }

  /**
   * {@inheritDoc}
   *
   * <p>Note that it does nothing since the JDBC transactions don't have coordinator tables.
   */
  @Override
  public void dropCoordinatorTables() {
    // Do nothing since JDBC transactions don't have coordinator tables
  }

  /**
   * {@inheritDoc}
   *
   * <p>Note that it does nothing since the JDBC transactions don't have coordinator tables.
   */
  @Override
  public void truncateCoordinatorTables() {
    // Do nothing since JDBC transactions don't have coordinator tables
  }

  /**
   * {@inheritDoc}
   *
   * <p>Note that it always returns true since JDBC transactions don't have coordinator tables.
   */
  @Override
  public boolean coordinatorTablesExist() {
    // Always return true since JDBC transactions don't have coordinator tables
    return true;
  }

  @Override
  public void repairTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    jdbcAdmin.repairTable(namespace, table, metadata, options);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Note that it does nothing since the JDBC transactions don't have coordinator tables.
   */
  @Override
  public void repairCoordinatorTables(Map<String, String> options) {
    // Do nothing since JDBC transactions don't have coordinator tables
  }

  @Override
  public void addNewColumnToTable(
      String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException {
    jdbcAdmin.addNewColumnToTable(namespace, table, columnName, columnType);
  }

  @Override
  public Set<String> getNamespaceNames() throws ExecutionException {
    return jdbcAdmin.getNamespaceNames();
  }

  @Override
  public void close() {
    jdbcAdmin.close();
  }
}
