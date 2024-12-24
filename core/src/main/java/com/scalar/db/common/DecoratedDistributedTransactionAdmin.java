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
  public void createPolicy(String policyName, @Nullable String dataTagColumnName)
      throws ExecutionException {
    distributedTransactionAdmin.createPolicy(policyName, dataTagColumnName);
  }

  @Override
  public void enablePolicy(String policyName) throws ExecutionException {
    distributedTransactionAdmin.enablePolicy(policyName);
  }

  @Override
  public void disablePolicy(String policyName) throws ExecutionException {
    distributedTransactionAdmin.disablePolicy(policyName);
  }

  @Override
  public Optional<Policy> getPolicy(String policyName) throws ExecutionException {
    return distributedTransactionAdmin.getPolicy(policyName);
  }

  @Override
  public List<Policy> getPolicies() throws ExecutionException {
    return distributedTransactionAdmin.getPolicies();
  }

  @Override
  public void createLevel(
      String policyName, String levelShortName, String levelLongName, int levelNumber)
      throws ExecutionException {
    distributedTransactionAdmin.createLevel(policyName, levelShortName, levelLongName, levelNumber);
  }

  @Override
  public void dropLevel(String policyName, String levelShortName) throws ExecutionException {
    distributedTransactionAdmin.dropLevel(policyName, levelShortName);
  }

  @Override
  public Optional<Level> getLevel(String policyName, String levelShortName)
      throws ExecutionException {
    return distributedTransactionAdmin.getLevel(policyName, levelShortName);
  }

  @Override
  public List<Level> getLevels(String policyName) throws ExecutionException {
    return distributedTransactionAdmin.getLevels(policyName);
  }

  @Override
  public void createCompartment(
      String policyName, String compartmentShortName, String compartmentLongName)
      throws ExecutionException {
    distributedTransactionAdmin.createCompartment(
        policyName, compartmentShortName, compartmentLongName);
  }

  @Override
  public void dropCompartment(String policyName, String compartmentShortName)
      throws ExecutionException {
    distributedTransactionAdmin.dropCompartment(policyName, compartmentShortName);
  }

  @Override
  public Optional<Compartment> getCompartment(String policyName, String compartmentShortName)
      throws ExecutionException {
    return distributedTransactionAdmin.getCompartment(policyName, compartmentShortName);
  }

  @Override
  public List<Compartment> getCompartments(String policyName) throws ExecutionException {
    return distributedTransactionAdmin.getCompartments(policyName);
  }

  @Override
  public void createGroup(
      String policyName,
      String groupShortName,
      String groupLongName,
      @Nullable String parentGroupShortName)
      throws ExecutionException {
    distributedTransactionAdmin.createGroup(
        policyName, groupShortName, groupLongName, parentGroupShortName);
  }

  @Override
  public void dropGroup(String policyName, String groupShortName) throws ExecutionException {
    distributedTransactionAdmin.dropGroup(policyName, groupShortName);
  }

  @Override
  public Optional<Group> getGroup(String policyName, String groupShortName)
      throws ExecutionException {
    return distributedTransactionAdmin.getGroup(policyName, groupShortName);
  }

  @Override
  public List<Group> getGroups(String policyName) throws ExecutionException {
    return distributedTransactionAdmin.getGroups(policyName);
  }

  @Override
  public void setLevelsToUser(
      String policyName,
      String username,
      String levelShortName,
      @Nullable String defaultLevelShortName,
      @Nullable String rowLevelShortName)
      throws ExecutionException {
    distributedTransactionAdmin.setLevelsToUser(
        policyName, username, levelShortName, defaultLevelShortName, rowLevelShortName);
  }

  @Override
  public void addCompartmentToUser(
      String policyName,
      String username,
      String compartmentShortName,
      AccessMode accessMode,
      boolean defaultCompartment,
      boolean rowCompartment)
      throws ExecutionException {
    distributedTransactionAdmin.addCompartmentToUser(
        policyName, username, compartmentShortName, accessMode, defaultCompartment, rowCompartment);
  }

  @Override
  public void removeCompartmentFromUser(
      String policyName, String username, String compartmentShortName) throws ExecutionException {
    distributedTransactionAdmin.removeCompartmentFromUser(
        policyName, username, compartmentShortName);
  }

  @Override
  public void addGroupToUser(
      String policyName,
      String username,
      String groupShortName,
      AccessMode accessMode,
      boolean defaultGroup,
      boolean rowGroup)
      throws ExecutionException {
    distributedTransactionAdmin.addGroupToUser(
        policyName, username, groupShortName, accessMode, defaultGroup, rowGroup);
  }

  @Override
  public void removeGroupFromUser(String policyName, String username, String groupShortName)
      throws ExecutionException {
    distributedTransactionAdmin.removeGroupFromUser(policyName, username, groupShortName);
  }

  @Override
  public void dropUserTagInfoFromUser(String policyName, String username)
      throws ExecutionException {
    distributedTransactionAdmin.dropUserTagInfoFromUser(policyName, username);
  }

  @Override
  public Optional<UserTagInfo> getUserTagInfo(String policyName, String username)
      throws ExecutionException {
    return distributedTransactionAdmin.getUserTagInfo(policyName, username);
  }

  @Override
  public void applyPolicyToNamespace(String policyName, String namespaceName)
      throws ExecutionException {
    distributedTransactionAdmin.applyPolicyToNamespace(policyName, namespaceName);
  }

  @Override
  public void enableNamespacePolicy(String policyName, String namespaceName)
      throws ExecutionException {
    distributedTransactionAdmin.enableNamespacePolicy(policyName, namespaceName);
  }

  @Override
  public void disableNamespacePolicy(String policyName, String namespaceName)
      throws ExecutionException {
    distributedTransactionAdmin.disableNamespacePolicy(policyName, namespaceName);
  }

  @Override
  public Optional<NamespacePolicy> getNamespacePolicy(String policyName, String namespaceName)
      throws ExecutionException {
    return distributedTransactionAdmin.getNamespacePolicy(policyName, namespaceName);
  }

  @Override
  public List<NamespacePolicy> getNamespacePolicies() throws ExecutionException {
    return distributedTransactionAdmin.getNamespacePolicies();
  }

  @Override
  public void applyPolicyToTable(String policyName, String namespaceName, String tableName)
      throws ExecutionException {
    distributedTransactionAdmin.applyPolicyToTable(policyName, namespaceName, tableName);
  }

  @Override
  public void enableTablePolicy(String policyName, String namespaceName, String tableName)
      throws ExecutionException {
    distributedTransactionAdmin.enableTablePolicy(policyName, namespaceName, tableName);
  }

  @Override
  public void disableTablePolicy(String policyName, String namespaceName, String tableName)
      throws ExecutionException {
    distributedTransactionAdmin.disableTablePolicy(policyName, namespaceName, tableName);
  }

  @Override
  public Optional<TablePolicy> getTablePolicy(
      String policyName, String namespaceName, String tableName) throws ExecutionException {
    return distributedTransactionAdmin.getTablePolicy(policyName, namespaceName, tableName);
  }

  @Override
  public List<TablePolicy> getTablePolicies() throws ExecutionException {
    return distributedTransactionAdmin.getTablePolicies();
  }

  @Override
  public void close() {
    distributedTransactionAdmin.close();
  }
}
