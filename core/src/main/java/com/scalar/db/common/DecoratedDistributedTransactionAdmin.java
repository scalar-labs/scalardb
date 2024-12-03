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

  private final DistributedTransactionAdmin distributedTransactionAdmin;

  public DecoratedDistributedTransactionAdmin(
      DistributedTransactionAdmin distributedTransactionAdmin) {
    this.distributedTransactionAdmin = distributedTransactionAdmin;
  }

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    distributedTransactionAdmin.createNamespace(namespace, options);
  }

  @Override
  public void createNamespace(String namespace, boolean ifNotExists, Map<String, String> options)
      throws ExecutionException {
    distributedTransactionAdmin.createNamespace(namespace, ifNotExists, options);
  }

  @Override
  public void createNamespace(String namespace, boolean ifNotExists) throws ExecutionException {
    distributedTransactionAdmin.createNamespace(namespace, ifNotExists);
  }

  @Override
  public void createNamespace(String namespace) throws ExecutionException {
    distributedTransactionAdmin.createNamespace(namespace);
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    distributedTransactionAdmin.createTable(namespace, table, metadata, options);
  }

  @Override
  public void createTable(
      String namespace,
      String table,
      TableMetadata metadata,
      boolean ifNotExists,
      Map<String, String> options)
      throws ExecutionException {
    distributedTransactionAdmin.createTable(namespace, table, metadata, ifNotExists, options);
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, boolean ifNotExists)
      throws ExecutionException {
    distributedTransactionAdmin.createTable(namespace, table, metadata, ifNotExists);
  }

  @Override
  public void createTable(String namespace, String table, TableMetadata metadata)
      throws ExecutionException {
    distributedTransactionAdmin.createTable(namespace, table, metadata);
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    distributedTransactionAdmin.dropTable(namespace, table);
  }

  @Override
  public void dropTable(String namespace, String table, boolean ifExists)
      throws ExecutionException {
    distributedTransactionAdmin.dropTable(namespace, table, ifExists);
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    distributedTransactionAdmin.dropNamespace(namespace);
  }

  @Override
  public void dropNamespace(String namespace, boolean ifExists) throws ExecutionException {
    distributedTransactionAdmin.dropNamespace(namespace, ifExists);
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    distributedTransactionAdmin.truncateTable(namespace, table);
  }

  @Override
  public void createIndex(
      String namespace, String table, String columnName, Map<String, String> options)
      throws ExecutionException {
    distributedTransactionAdmin.createIndex(namespace, table, columnName, options);
  }

  @Override
  public void createIndex(
      String namespace,
      String table,
      String columnName,
      boolean ifNotExists,
      Map<String, String> options)
      throws ExecutionException {
    distributedTransactionAdmin.createIndex(namespace, table, columnName, ifNotExists, options);
  }

  @Override
  public void createIndex(String namespace, String table, String columnName, boolean ifNotExists)
      throws ExecutionException {
    distributedTransactionAdmin.createIndex(namespace, table, columnName, ifNotExists);
  }

  @Override
  public void createIndex(String namespace, String table, String columnName)
      throws ExecutionException {
    distributedTransactionAdmin.createIndex(namespace, table, columnName);
  }

  @Override
  public void dropIndex(String namespace, String table, String columnName)
      throws ExecutionException {
    distributedTransactionAdmin.dropIndex(namespace, table, columnName);
  }

  @Override
  public void dropIndex(String namespace, String table, String columnName, boolean ifExists)
      throws ExecutionException {
    distributedTransactionAdmin.dropIndex(namespace, table, columnName, ifExists);
  }

  @Override
  public boolean indexExists(String namespace, String table, String columnName)
      throws ExecutionException {
    return distributedTransactionAdmin.indexExists(namespace, table, columnName);
  }

  @Nullable
  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    return distributedTransactionAdmin.getTableMetadata(namespace, table);
  }

  @Override
  public Set<String> getNamespaceTableNames(String namespace) throws ExecutionException {
    return distributedTransactionAdmin.getNamespaceTableNames(namespace);
  }

  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    return distributedTransactionAdmin.namespaceExists(namespace);
  }

  @Override
  public boolean tableExists(String namespace, String table) throws ExecutionException {
    return distributedTransactionAdmin.tableExists(namespace, table);
  }

  @Override
  public void repairNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    distributedTransactionAdmin.repairNamespace(namespace, options);
  }

  @Override
  public void repairTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    distributedTransactionAdmin.repairTable(namespace, table, metadata, options);
  }

  @Override
  public void addNewColumnToTable(
      String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException {
    distributedTransactionAdmin.addNewColumnToTable(namespace, table, columnName, columnType);
  }

  @Override
  public void addNewColumnToTable(
      String namespace, String table, String columnName, DataType columnType, boolean encrypted)
      throws ExecutionException {
    distributedTransactionAdmin.addNewColumnToTable(
        namespace, table, columnName, columnType, encrypted);
  }

  @Override
  public void importTable(String namespace, String table, Map<String, String> options)
      throws ExecutionException {
    distributedTransactionAdmin.importTable(namespace, table, options);
  }

  @Override
  public Set<String> getNamespaceNames() throws ExecutionException {
    return distributedTransactionAdmin.getNamespaceNames();
  }

  @Override
  public void upgrade(Map<String, String> options) throws ExecutionException {
    distributedTransactionAdmin.upgrade(options);
  }

  @Override
  public void createCoordinatorTables(Map<String, String> options) throws ExecutionException {
    distributedTransactionAdmin.createCoordinatorTables(options);
  }

  @Override
  public void createCoordinatorTables(boolean ifNotExist, Map<String, String> options)
      throws ExecutionException {
    distributedTransactionAdmin.createCoordinatorTables(ifNotExist, options);
  }

  @Override
  public void createCoordinatorTables(boolean ifNotExist) throws ExecutionException {
    distributedTransactionAdmin.createCoordinatorTables(ifNotExist);
  }

  @Override
  public void createCoordinatorTables() throws ExecutionException {
    distributedTransactionAdmin.createCoordinatorTables();
  }

  @Override
  public void dropCoordinatorTables() throws ExecutionException {
    distributedTransactionAdmin.dropCoordinatorTables();
  }

  @Override
  public void dropCoordinatorTables(boolean ifExist) throws ExecutionException {
    distributedTransactionAdmin.dropCoordinatorTables(ifExist);
  }

  @Override
  public void truncateCoordinatorTables() throws ExecutionException {
    distributedTransactionAdmin.truncateCoordinatorTables();
  }

  @Override
  public boolean coordinatorTablesExist() throws ExecutionException {
    return distributedTransactionAdmin.coordinatorTablesExist();
  }

  @Override
  public void repairCoordinatorTables(Map<String, String> options) throws ExecutionException {
    distributedTransactionAdmin.repairCoordinatorTables(options);
  }

  @Override
  public void createUser(String username, @Nullable String password, UserOption... userOptions)
      throws ExecutionException {
    distributedTransactionAdmin.createUser(username, password, userOptions);
  }

  @Override
  public void alterUser(String username, @Nullable String password, UserOption... userOptions)
      throws ExecutionException {
    distributedTransactionAdmin.alterUser(username, password, userOptions);
  }

  @Override
  public void dropUser(String username) throws ExecutionException {
    distributedTransactionAdmin.dropUser(username);
  }

  @Override
  public void grant(
      String username, String namespaceName, String tableName, Privilege... privileges)
      throws ExecutionException {
    distributedTransactionAdmin.grant(username, namespaceName, tableName, privileges);
  }

  @Override
  public void grant(String username, String namespaceName, Privilege... privileges)
      throws ExecutionException {
    distributedTransactionAdmin.grant(username, namespaceName, privileges);
  }

  @Override
  public void revoke(
      String username, String namespaceName, String tableName, Privilege... privileges)
      throws ExecutionException {
    distributedTransactionAdmin.revoke(username, namespaceName, tableName, privileges);
  }

  @Override
  public void revoke(String username, String namespaceName, Privilege... privileges)
      throws ExecutionException {
    distributedTransactionAdmin.revoke(username, namespaceName, privileges);
  }

  @Override
  public Optional<User> getUser(String username) throws ExecutionException {
    return distributedTransactionAdmin.getUser(username);
  }

  @Override
  public List<User> getUsers() throws ExecutionException {
    return distributedTransactionAdmin.getUsers();
  }

  @Override
  public User getCurrentUser() throws ExecutionException {
    return distributedTransactionAdmin.getCurrentUser();
  }

  @Override
  public Set<Privilege> getPrivileges(String username, String namespaceName)
      throws ExecutionException {
    return distributedTransactionAdmin.getPrivileges(username, namespaceName);
  }

  @Override
  public Set<Privilege> getPrivileges(String username, String namespaceName, String tableName)
      throws ExecutionException {
    return distributedTransactionAdmin.getPrivileges(username, namespaceName, tableName);
  }

  @Override
  public void close() {
    distributedTransactionAdmin.close();
  }
}
