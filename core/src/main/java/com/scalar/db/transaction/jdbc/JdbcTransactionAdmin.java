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
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class JdbcTransactionAdmin implements DistributedTransactionAdmin {

  private final DistributedStorageAdmin jdbcAdmin;

  @Inject
  public JdbcTransactionAdmin(DatabaseConfig databaseConfig) {
    jdbcAdmin = new CheckedDistributedStorageAdmin(new JdbcAdmin(databaseConfig));
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
  public void importTable(String namespace, String table, Map<String, String> options)
      throws ExecutionException {
    jdbcAdmin.importTable(namespace, table, options);
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
    jdbcAdmin.repairTable(namespace, table, metadata, options);
  }

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
  public void repairNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    throw new UnsupportedOperationException(
        "Repairing a namespace is not supported with the JDBC transaction admin");
  }

  @Override
  public void upgrade(Map<String, String> options) throws ExecutionException {
    throw new UnsupportedOperationException(
        "Upgrading the ScalarDB environment is not supported with the JDBC transaction admin");
  }

  @Override
  public void close() {
    jdbcAdmin.close();
  }

  @Override
  public void createUser(String username, @Nullable String password, UserOption... userOption) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterUser(String username, @Nullable String password, UserOption... userOption) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropUser(String username) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void grant(String username, String namespaceName, Privilege... privileges) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void grant(
      String username, String namespaceName, String tableName, Privilege... privileges) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void revoke(String username, String namespaceName, Privilege... privileges) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void revoke(
      String username, String namespaceName, String tableName, Privilege... privileges) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<User> getUsers() {
    throw new UnsupportedOperationException();
  }

  @Override
  public EnumSet<Privilege> getPrivileges(String username, String namespaceName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public EnumSet<Privilege> getPrivileges(String username, String namespaceName, String tableName) {
    throw new UnsupportedOperationException();
  }
}
