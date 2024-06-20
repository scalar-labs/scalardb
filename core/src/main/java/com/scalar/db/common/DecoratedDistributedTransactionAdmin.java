package com.scalar.db.common;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

public abstract class DecoratedDistributedTransactionAdmin implements DistributedTransactionAdmin {

  private final DistributedTransactionAdmin decoratedDistributedTransactionAdmin;

  public DecoratedDistributedTransactionAdmin(
      DistributedTransactionAdmin decoratedDistributedTransactionAdmin) {
    this.decoratedDistributedTransactionAdmin = decoratedDistributedTransactionAdmin;
  }

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    decoratedDistributedTransactionAdmin.createNamespace(namespace, options);
  }

  @Override
  public void createNamespace(String namespace, boolean ifNotExists, Map<String, String> options)
      throws ExecutionException {
    decoratedDistributedTransactionAdmin.createNamespace(namespace, ifNotExists, options);
  }

  @Override
  public void createNamespace(String namespace, boolean ifNotExists) throws ExecutionException {
    decoratedDistributedTransactionAdmin.createNamespace(namespace, ifNotExists);
  }

  @Override
  public void createNamespace(String namespace) throws ExecutionException {
    decoratedDistributedTransactionAdmin.createNamespace(namespace);
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    decoratedDistributedTransactionAdmin.createTable(namespace, table, metadata, options);
  }

  @Override
  public void createTable(
      String namespace,
      String table,
      TableMetadata metadata,
      boolean ifNotExists,
      Map<String, String> options)
      throws ExecutionException {
    decoratedDistributedTransactionAdmin.createTable(
        namespace, table, metadata, ifNotExists, options);
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, boolean ifNotExists)
      throws ExecutionException {
    decoratedDistributedTransactionAdmin.createTable(namespace, table, metadata, ifNotExists);
  }

  @Override
  public void createTable(String namespace, String table, TableMetadata metadata)
      throws ExecutionException {
    decoratedDistributedTransactionAdmin.createTable(namespace, table, metadata);
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    decoratedDistributedTransactionAdmin.dropTable(namespace, table);
  }

  @Override
  public void dropTable(String namespace, String table, boolean ifExists)
      throws ExecutionException {
    decoratedDistributedTransactionAdmin.dropTable(namespace, table, ifExists);
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    decoratedDistributedTransactionAdmin.dropNamespace(namespace);
  }

  @Override
  public void dropNamespace(String namespace, boolean ifExists) throws ExecutionException {
    decoratedDistributedTransactionAdmin.dropNamespace(namespace, ifExists);
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    decoratedDistributedTransactionAdmin.truncateTable(namespace, table);
  }

  @Override
  public void createIndex(
      String namespace, String table, String columnName, Map<String, String> options)
      throws ExecutionException {
    decoratedDistributedTransactionAdmin.createIndex(namespace, table, columnName, options);
  }

  @Override
  public void createIndex(
      String namespace,
      String table,
      String columnName,
      boolean ifNotExists,
      Map<String, String> options)
      throws ExecutionException {
    decoratedDistributedTransactionAdmin.createIndex(
        namespace, table, columnName, ifNotExists, options);
  }

  @Override
  public void createIndex(String namespace, String table, String columnName, boolean ifNotExists)
      throws ExecutionException {
    decoratedDistributedTransactionAdmin.createIndex(namespace, table, columnName, ifNotExists);
  }

  @Override
  public void createIndex(String namespace, String table, String columnName)
      throws ExecutionException {
    decoratedDistributedTransactionAdmin.createIndex(namespace, table, columnName);
  }

  @Override
  public void dropIndex(String namespace, String table, String columnName)
      throws ExecutionException {
    decoratedDistributedTransactionAdmin.dropIndex(namespace, table, columnName);
  }

  @Override
  public void dropIndex(String namespace, String table, String columnName, boolean ifExists)
      throws ExecutionException {
    decoratedDistributedTransactionAdmin.dropIndex(namespace, table, columnName, ifExists);
  }

  @Override
  public boolean indexExists(String namespace, String table, String columnName)
      throws ExecutionException {
    return decoratedDistributedTransactionAdmin.indexExists(namespace, table, columnName);
  }

  @Nullable
  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    return decoratedDistributedTransactionAdmin.getTableMetadata(namespace, table);
  }

  @Override
  public Set<String> getNamespaceTableNames(String namespace) throws ExecutionException {
    return decoratedDistributedTransactionAdmin.getNamespaceTableNames(namespace);
  }

  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    return decoratedDistributedTransactionAdmin.namespaceExists(namespace);
  }

  @Override
  public boolean tableExists(String namespace, String table) throws ExecutionException {
    return decoratedDistributedTransactionAdmin.tableExists(namespace, table);
  }

  @Override
  public void repairNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    decoratedDistributedTransactionAdmin.repairNamespace(namespace, options);
  }

  @Override
  public void repairTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    decoratedDistributedTransactionAdmin.repairTable(namespace, table, metadata, options);
  }

  @Override
  public void addNewColumnToTable(
      String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException {
    decoratedDistributedTransactionAdmin.addNewColumnToTable(
        namespace, table, columnName, columnType);
  }

  @Override
  public void addNewColumnToTable(
      String namespace, String table, String columnName, DataType columnType, boolean encrypted)
      throws ExecutionException {
    decoratedDistributedTransactionAdmin.addNewColumnToTable(
        namespace, table, columnName, columnType, encrypted);
  }

  @Override
  public void importTable(String namespace, String table, Map<String, String> options)
      throws ExecutionException {
    decoratedDistributedTransactionAdmin.importTable(namespace, table, options);
  }

  @Override
  public Set<String> getNamespaceNames() throws ExecutionException {
    return decoratedDistributedTransactionAdmin.getNamespaceNames();
  }

  @Override
  public void upgrade(Map<String, String> options) throws ExecutionException {
    decoratedDistributedTransactionAdmin.upgrade(options);
  }

  @Override
  public void createCoordinatorTables(Map<String, String> options) throws ExecutionException {
    decoratedDistributedTransactionAdmin.createCoordinatorTables(options);
  }

  @Override
  public void createCoordinatorTables(boolean ifNotExist, Map<String, String> options)
      throws ExecutionException {
    decoratedDistributedTransactionAdmin.createCoordinatorTables(ifNotExist, options);
  }

  @Override
  public void createCoordinatorTables(boolean ifNotExist) throws ExecutionException {
    decoratedDistributedTransactionAdmin.createCoordinatorTables(ifNotExist);
  }

  @Override
  public void createCoordinatorTables() throws ExecutionException {
    decoratedDistributedTransactionAdmin.createCoordinatorTables();
  }

  @Override
  public void dropCoordinatorTables() throws ExecutionException {
    decoratedDistributedTransactionAdmin.dropCoordinatorTables();
  }

  @Override
  public void dropCoordinatorTables(boolean ifExist) throws ExecutionException {
    decoratedDistributedTransactionAdmin.dropCoordinatorTables(ifExist);
  }

  @Override
  public void truncateCoordinatorTables() throws ExecutionException {
    decoratedDistributedTransactionAdmin.truncateCoordinatorTables();
  }

  @Override
  public boolean coordinatorTablesExist() throws ExecutionException {
    return decoratedDistributedTransactionAdmin.coordinatorTablesExist();
  }

  @Override
  public void repairCoordinatorTables(Map<String, String> options) throws ExecutionException {
    decoratedDistributedTransactionAdmin.repairCoordinatorTables(options);
  }

  @Override
  public void createUser(String username, @Nullable String password, UserOption... userOptions)
      throws ExecutionException {
    decoratedDistributedTransactionAdmin.createUser(username, password, userOptions);
  }

  @Override
  public void alterUser(String username, @Nullable String password, UserOption... userOptions)
      throws ExecutionException {
    decoratedDistributedTransactionAdmin.alterUser(username, password, userOptions);
  }

  @Override
  public void dropUser(String username) throws ExecutionException {
    decoratedDistributedTransactionAdmin.dropUser(username);
  }

  @Override
  public void grant(
      String username, String namespaceName, String tableName, Privilege... privileges)
      throws ExecutionException {
    decoratedDistributedTransactionAdmin.grant(username, namespaceName, tableName, privileges);
  }

  @Override
  public void grant(String username, String namespaceName, Privilege... privileges)
      throws ExecutionException {
    decoratedDistributedTransactionAdmin.grant(username, namespaceName, privileges);
  }

  @Override
  public void revoke(
      String username, String namespaceName, String tableName, Privilege... privileges)
      throws ExecutionException {
    decoratedDistributedTransactionAdmin.revoke(username, namespaceName, tableName, privileges);
  }

  @Override
  public void revoke(String username, String namespaceName, Privilege... privileges)
      throws ExecutionException {
    decoratedDistributedTransactionAdmin.revoke(username, namespaceName, privileges);
  }

  @Override
  public Optional<User> getUser(String username) throws ExecutionException {
    return decoratedDistributedTransactionAdmin.getUser(username);
  }

  @Override
  public List<User> getUsers() throws ExecutionException {
    return decoratedDistributedTransactionAdmin.getUsers();
  }

  @Override
  public Set<Privilege> getPrivileges(String username, String namespaceName)
      throws ExecutionException {
    return decoratedDistributedTransactionAdmin.getPrivileges(username, namespaceName);
  }

  @Override
  public Set<Privilege> getPrivileges(String username, String namespaceName, String tableName)
      throws ExecutionException {
    return decoratedDistributedTransactionAdmin.getPrivileges(username, namespaceName, tableName);
  }

  @Override
  public void close() {
    decoratedDistributedTransactionAdmin.close();
  }
}
