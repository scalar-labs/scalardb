package com.scalar.db.common;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.StorageInfo;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.VirtualTableInfo;
import com.scalar.db.api.VirtualTableJoinType;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

public class CommonDistributedStorageAdmin implements DistributedStorageAdmin {

  private final DistributedStorageAdmin admin;
  private final String systemNamespaceName;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public CommonDistributedStorageAdmin(DistributedStorageAdmin admin, DatabaseConfig config) {
    this.admin = admin;
    systemNamespaceName = config.getSystemNamespaceName();
  }

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    if (systemNamespaceName.equals(namespace)) {
      throw new IllegalArgumentException(
          CoreError.SYSTEM_NAMESPACE_SPECIFIED.buildMessage(namespace));
    }
    if (namespaceExists(namespace)) {
      throw new IllegalArgumentException(
          CoreError.NAMESPACE_ALREADY_EXISTS.buildMessage(namespace));
    }

    try {
      admin.createNamespace(namespace, options);
    } catch (ExecutionException e) {
      throw new ExecutionException(CoreError.CREATING_NAMESPACE_FAILED.buildMessage(namespace), e);
    }
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    if (!namespaceExists(namespace)) {
      throw new IllegalArgumentException(CoreError.NAMESPACE_NOT_FOUND.buildMessage(namespace));
    }
    if (tableExists(namespace, table)) {
      throw new IllegalArgumentException(
          CoreError.TABLE_ALREADY_EXISTS.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table)));
    }

    if (!metadata.getEncryptedColumnNames().isEmpty()) {
      throw new UnsupportedOperationException(CoreError.ENCRYPTION_NOT_ENABLED.buildMessage());
    }

    try {
      admin.createTable(namespace, table, metadata, options);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          CoreError.CREATING_TABLE_FAILED.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table)),
          e);
    }
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    if (!tableExists(namespace, table)) {
      throw new IllegalArgumentException(
          CoreError.TABLE_NOT_FOUND.buildMessage(ScalarDbUtils.getFullTableName(namespace, table)));
    }

    try {
      admin.dropTable(namespace, table);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          CoreError.DROPPING_TABLE_FAILED.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table)),
          e);
    }
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    if (systemNamespaceName.equals(namespace)) {
      throw new IllegalArgumentException(
          CoreError.SYSTEM_NAMESPACE_SPECIFIED.buildMessage(namespace));
    }
    if (!namespaceExists(namespace)) {
      throw new IllegalArgumentException(CoreError.NAMESPACE_NOT_FOUND.buildMessage(namespace));
    }
    if (!getNamespaceTableNames(namespace).isEmpty()) {
      throw new IllegalArgumentException(
          CoreError.NAMESPACE_NOT_EMPTY.buildMessage(namespace, getNamespaceTableNames(namespace)));
    }

    try {
      admin.dropNamespace(namespace);
    } catch (ExecutionException e) {
      throw new ExecutionException(CoreError.DROPPING_NAMESPACE_FAILED.buildMessage(namespace), e);
    }
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    if (!tableExists(namespace, table)) {
      throw new IllegalArgumentException(
          CoreError.TABLE_NOT_FOUND.buildMessage(ScalarDbUtils.getFullTableName(namespace, table)));
    }

    try {
      admin.truncateTable(namespace, table);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          CoreError.TRUNCATING_TABLE_FAILED.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table)),
          e);
    }
  }

  @Override
  public void createIndex(
      String namespace, String table, String columnName, Map<String, String> options)
      throws ExecutionException {
    TableMetadata tableMetadata = getTableMetadata(namespace, table);
    if (tableMetadata == null) {
      throw new IllegalArgumentException(
          CoreError.TABLE_NOT_FOUND.buildMessage(ScalarDbUtils.getFullTableName(namespace, table)));
    }
    if (!tableMetadata.getColumnNames().contains(columnName)) {
      throw new IllegalArgumentException(
          CoreError.COLUMN_NOT_FOUND2.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table), columnName));
    }

    if (indexExists(namespace, table, columnName)) {
      throw new IllegalArgumentException(
          CoreError.INDEX_ALREADY_EXISTS.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table), columnName));
    }

    try {
      admin.createIndex(namespace, table, columnName, options);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          CoreError.CREATING_INDEX_FAILED.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table), columnName),
          e);
    }
  }

  @Override
  public void dropIndex(String namespace, String table, String columnName)
      throws ExecutionException {
    if (!tableExists(namespace, table)) {
      throw new IllegalArgumentException(
          CoreError.TABLE_NOT_FOUND.buildMessage(ScalarDbUtils.getFullTableName(namespace, table)));
    }

    if (!indexExists(namespace, table, columnName)) {
      throw new IllegalArgumentException(
          CoreError.INDEX_NOT_FOUND.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table), columnName));
    }

    try {
      admin.dropIndex(namespace, table, columnName);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          CoreError.DROPPING_INDEX_FAILED.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table), columnName),
          e);
    }
  }

  @Nullable
  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    try {
      return admin.getTableMetadata(namespace, table);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          CoreError.GETTING_TABLE_METADATA_FAILED.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table)),
          e);
    }
  }

  @Override
  public Set<String> getNamespaceTableNames(String namespace) throws ExecutionException {
    try {
      return admin.getNamespaceTableNames(namespace);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          CoreError.GETTING_TABLE_NAMES_IN_NAMESPACE_FAILED.buildMessage(namespace), e);
    }
  }

  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    if (systemNamespaceName.equals(namespace)) {
      return true;
    }
    try {
      return admin.namespaceExists(namespace);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          CoreError.CHECKING_NAMESPACE_EXISTENCE_FAILED.buildMessage(namespace), e);
    }
  }

  @Override
  public boolean tableExists(String namespace, String table) throws ExecutionException {
    try {
      return DistributedStorageAdmin.super.tableExists(namespace, table);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          CoreError.CHECKING_TABLE_EXISTENCE_FAILED.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table)),
          e);
    }
  }

  @Override
  public boolean indexExists(String namespace, String table, String columnName)
      throws ExecutionException {
    try {
      return DistributedStorageAdmin.super.indexExists(namespace, table, columnName);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          CoreError.CHECKING_INDEX_EXISTENCE_FAILED.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table), columnName),
          e);
    }
  }

  @Override
  public void repairNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    try {
      admin.repairNamespace(namespace, options);
    } catch (ExecutionException e) {
      throw new ExecutionException(CoreError.REPAIRING_NAMESPACE_FAILED.buildMessage(namespace), e);
    }
  }

  @Override
  public void repairTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    if (!metadata.getEncryptedColumnNames().isEmpty()) {
      throw new UnsupportedOperationException(CoreError.ENCRYPTION_NOT_ENABLED.buildMessage());
    }

    try {
      admin.repairTable(namespace, table, metadata, options);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          CoreError.REPAIRING_TABLE_FAILED.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table)),
          e);
    }
  }

  @Override
  public void addNewColumnToTable(
      String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException {
    TableMetadata tableMetadata = getTableMetadata(namespace, table);
    if (tableMetadata == null) {
      throw new IllegalArgumentException(
          CoreError.TABLE_NOT_FOUND.buildMessage(ScalarDbUtils.getFullTableName(namespace, table)));
    }

    if (tableMetadata.getColumnNames().contains(columnName)) {
      throw new IllegalArgumentException(
          CoreError.COLUMN_ALREADY_EXISTS.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table), columnName));
    }

    try {
      admin.addNewColumnToTable(namespace, table, columnName, columnType);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          CoreError.ADDING_NEW_COLUMN_TO_TABLE_FAILED.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table), columnName, columnType),
          e);
    }
  }

  @Override
  public void dropColumnFromTable(String namespace, String table, String columnName)
      throws ExecutionException {
    TableMetadata tableMetadata = getTableMetadata(namespace, table);
    if (tableMetadata == null) {
      throw new IllegalArgumentException(
          CoreError.TABLE_NOT_FOUND.buildMessage(ScalarDbUtils.getFullTableName(namespace, table)));
    }

    if (!tableMetadata.getColumnNames().contains(columnName)) {
      throw new IllegalArgumentException(
          CoreError.COLUMN_NOT_FOUND2.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table), columnName));
    }

    if (tableMetadata.getPartitionKeyNames().contains(columnName)
        || tableMetadata.getClusteringKeyNames().contains(columnName)) {
      throw new IllegalArgumentException(
          CoreError.DROP_PRIMARY_KEY_COLUMN_NOT_SUPPORTED.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table), columnName));
    }

    if (tableMetadata.getSecondaryIndexNames().contains(columnName)) {
      dropIndex(namespace, table, columnName);
    }

    try {
      admin.dropColumnFromTable(namespace, table, columnName);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          CoreError.DROPPING_COLUMN_FROM_TABLE_FAILED.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table), columnName),
          e);
    }
  }

  @Override
  public void renameColumn(
      String namespace, String table, String oldColumnName, String newColumnName)
      throws ExecutionException {
    TableMetadata tableMetadata = getTableMetadata(namespace, table);
    if (tableMetadata == null) {
      throw new IllegalArgumentException(
          CoreError.TABLE_NOT_FOUND.buildMessage(ScalarDbUtils.getFullTableName(namespace, table)));
    }

    if (!tableMetadata.getColumnNames().contains(oldColumnName)) {
      throw new IllegalArgumentException(
          CoreError.COLUMN_NOT_FOUND2.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table), oldColumnName));
    }

    if (tableMetadata.getColumnNames().contains(newColumnName)) {
      throw new IllegalArgumentException(
          CoreError.COLUMN_ALREADY_EXISTS.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table), newColumnName));
    }

    try {
      admin.renameColumn(namespace, table, oldColumnName, newColumnName);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          CoreError.RENAMING_COLUMN_FAILED.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table), oldColumnName, newColumnName),
          e);
    }
  }

  @Override
  public void alterColumnType(
      String namespace, String table, String columnName, DataType newColumnType)
      throws ExecutionException {
    TableMetadata tableMetadata = getTableMetadata(namespace, table);
    if (tableMetadata == null) {
      throw new IllegalArgumentException(
          CoreError.TABLE_NOT_FOUND.buildMessage(ScalarDbUtils.getFullTableName(namespace, table)));
    }

    if (!tableMetadata.getColumnNames().contains(columnName)) {
      throw new IllegalArgumentException(
          CoreError.COLUMN_NOT_FOUND2.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table), columnName));
    }

    if (tableMetadata.getPartitionKeyNames().contains(columnName)
        || tableMetadata.getClusteringKeyNames().contains(columnName)
        || tableMetadata.getSecondaryIndexNames().contains(columnName)) {
      throw new IllegalArgumentException(
          CoreError.ALTER_PRIMARY_OR_INDEX_KEY_COLUMN_TYPE_NOT_SUPPORTED.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table), columnName));
    }

    DataType currentColumnType = tableMetadata.getColumnDataType(columnName);
    if (currentColumnType == newColumnType) {
      return;
    }
    if (!isTypeConversionValid(currentColumnType, newColumnType)) {
      throw new IllegalArgumentException(
          CoreError.INVALID_COLUMN_TYPE_CONVERSION.buildMessage(
              currentColumnType, newColumnType, columnName));
    }

    try {
      admin.alterColumnType(namespace, table, columnName, newColumnType);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          CoreError.ALTERING_COLUMN_TYPE_FAILED.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table), columnName, newColumnType),
          e);
    }
  }

  @Override
  public void renameTable(String namespace, String oldTableName, String newTableName)
      throws ExecutionException {
    TableMetadata tableMetadata = getTableMetadata(namespace, oldTableName);
    if (tableMetadata == null) {
      throw new IllegalArgumentException(
          CoreError.TABLE_NOT_FOUND.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, oldTableName)));
    }

    if (tableExists(namespace, newTableName)) {
      throw new IllegalArgumentException(
          CoreError.TABLE_ALREADY_EXISTS.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, newTableName)));
    }

    try {
      admin.renameTable(namespace, oldTableName, newTableName);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          CoreError.RENAMING_TABLE_FAILED.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, oldTableName),
              ScalarDbUtils.getFullTableName(namespace, newTableName)),
          e);
    }
  }

  @Override
  public Set<String> getNamespaceNames() throws ExecutionException {
    try {
      Set<String> namespaceNames = admin.getNamespaceNames();
      if (namespaceNames.contains(systemNamespaceName)) {
        return namespaceNames;
      }

      // If the system namespace does not exist, add it to the set
      namespaceNames = new HashSet<>(namespaceNames);
      namespaceNames.add(systemNamespaceName);
      return namespaceNames;
    } catch (ExecutionException e) {
      throw new ExecutionException(CoreError.GETTING_NAMESPACE_NAMES_FAILED.buildMessage(), e);
    }
  }

  @Override
  public void importTable(
      String namespace,
      String table,
      Map<String, String> options,
      Map<String, DataType> overrideColumnsType)
      throws ExecutionException {
    TableMetadata tableMetadata = getTableMetadata(namespace, table);
    if (tableMetadata != null) {
      throw new IllegalArgumentException(
          CoreError.TABLE_ALREADY_EXISTS.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table)));
    }

    try {
      admin.importTable(namespace, table, options, overrideColumnsType);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          CoreError.IMPORTING_TABLE_FAILED.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table)),
          e);
    }
  }

  @Override
  public void upgrade(Map<String, String> options) throws ExecutionException {
    try {
      admin.upgrade(options);
    } catch (ExecutionException e) {
      throw new ExecutionException(CoreError.UPGRADING_SCALAR_DB_ENV_FAILED.buildMessage(), e);
    }
  }

  @Override
  public StorageInfo getStorageInfo(String namespace) throws ExecutionException {
    if (!namespaceExists(namespace)) {
      throw new IllegalArgumentException(CoreError.NAMESPACE_NOT_FOUND.buildMessage(namespace));
    }

    try {
      return admin.getStorageInfo(namespace);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          CoreError.GETTING_STORAGE_INFO_FAILED.buildMessage(namespace), e);
    }
  }

  @Override
  public void createVirtualTable(
      String namespace,
      String table,
      String leftSourceNamespace,
      String leftSourceTable,
      String rightSourceNamespace,
      String rightSourceTable,
      VirtualTableJoinType joinType,
      Map<String, String> options)
      throws ExecutionException {
    StorageInfo storageInfo = getStorageInfo(leftSourceNamespace);
    switch (storageInfo.getAtomicityUnit()) {
      case STORAGE:
        break;
      case NAMESPACE:
        if (!leftSourceNamespace.equals(rightSourceNamespace)) {
          throw new IllegalArgumentException(
              CoreError.VIRTUAL_TABLE_SOURCE_TABLES_OUTSIDE_OF_ATOMICITY_UNIT.buildMessage(
                  storageInfo.getStorageName(),
                  storageInfo.getAtomicityUnit(),
                  ScalarDbUtils.getFullTableName(leftSourceNamespace, leftSourceTable),
                  ScalarDbUtils.getFullTableName(rightSourceNamespace, rightSourceTable)));
        }
        break;
      default:
        throw new UnsupportedOperationException(
            CoreError.VIRTUAL_TABLE_NOT_SUPPORTED_IN_STORAGE.buildMessage(
                storageInfo.getStorageName(), storageInfo.getAtomicityUnit()));
    }

    if (!namespaceExists(namespace)) {
      throw new IllegalArgumentException(CoreError.NAMESPACE_NOT_FOUND.buildMessage(namespace));
    }

    if (tableExists(namespace, table)) {
      throw new IllegalArgumentException(
          CoreError.TABLE_ALREADY_EXISTS.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table)));
    }

    TableMetadata leftSourceTableMetadata = getTableMetadata(leftSourceNamespace, leftSourceTable);
    if (leftSourceTableMetadata == null) {
      throw new IllegalArgumentException(
          CoreError.TABLE_NOT_FOUND.buildMessage(
              ScalarDbUtils.getFullTableName(leftSourceNamespace, leftSourceTable)));
    }

    TableMetadata rightSourceTableMetadata =
        getTableMetadata(rightSourceNamespace, rightSourceTable);
    if (rightSourceTableMetadata == null) {
      throw new IllegalArgumentException(
          CoreError.TABLE_NOT_FOUND.buildMessage(
              ScalarDbUtils.getFullTableName(rightSourceNamespace, rightSourceTable)));
    }

    // Check that the partition key and the clustering key names match
    if (!leftSourceTableMetadata
        .getPartitionKeyNames()
        .equals(rightSourceTableMetadata.getPartitionKeyNames())) {
      throw new IllegalArgumentException(
          CoreError.VIRTUAL_TABLE_SOURCE_TABLES_HAVE_DIFFERENT_PRIMARY_KEY.buildMessage(
              ScalarDbUtils.getFullTableName(leftSourceNamespace, leftSourceTable),
              ScalarDbUtils.getFullTableName(rightSourceNamespace, rightSourceTable)));
    }
    if (!leftSourceTableMetadata
        .getClusteringKeyNames()
        .equals(rightSourceTableMetadata.getClusteringKeyNames())) {
      throw new IllegalArgumentException(
          CoreError.VIRTUAL_TABLE_SOURCE_TABLES_HAVE_DIFFERENT_PRIMARY_KEY.buildMessage(
              ScalarDbUtils.getFullTableName(leftSourceNamespace, leftSourceTable),
              ScalarDbUtils.getFullTableName(rightSourceNamespace, rightSourceTable)));
    }

    // Check that partition key data types match
    for (String partitionKey : leftSourceTableMetadata.getPartitionKeyNames()) {
      if (leftSourceTableMetadata.getColumnDataType(partitionKey)
          != rightSourceTableMetadata.getColumnDataType(partitionKey)) {
        throw new IllegalArgumentException(
            CoreError.VIRTUAL_TABLE_SOURCE_TABLES_HAVE_DIFFERENT_PRIMARY_KEY_TYPES.buildMessage(
                partitionKey,
                ScalarDbUtils.getFullTableName(leftSourceNamespace, leftSourceTable),
                ScalarDbUtils.getFullTableName(rightSourceNamespace, rightSourceTable)));
      }
    }

    // Check that clustering key data types and clustering orders match
    for (String clusteringKey : leftSourceTableMetadata.getClusteringKeyNames()) {
      if (leftSourceTableMetadata.getColumnDataType(clusteringKey)
          != rightSourceTableMetadata.getColumnDataType(clusteringKey)) {
        throw new IllegalArgumentException(
            CoreError.VIRTUAL_TABLE_SOURCE_TABLES_HAVE_DIFFERENT_PRIMARY_KEY_TYPES.buildMessage(
                clusteringKey,
                ScalarDbUtils.getFullTableName(leftSourceNamespace, leftSourceTable),
                ScalarDbUtils.getFullTableName(rightSourceNamespace, rightSourceTable)));
      }
      if (leftSourceTableMetadata.getClusteringOrder(clusteringKey)
          != rightSourceTableMetadata.getClusteringOrder(clusteringKey)) {
        throw new IllegalArgumentException(
            CoreError.VIRTUAL_TABLE_SOURCE_TABLES_HAVE_DIFFERENT_CLUSTERING_ORDERS.buildMessage(
                clusteringKey,
                ScalarDbUtils.getFullTableName(leftSourceNamespace, leftSourceTable),
                ScalarDbUtils.getFullTableName(rightSourceNamespace, rightSourceTable)));
      }
    }

    // Check for non-key column name conflicts between sources
    Set<String> primaryKeyColumns = new HashSet<>(leftSourceTableMetadata.getPartitionKeyNames());
    primaryKeyColumns.addAll(leftSourceTableMetadata.getClusteringKeyNames());

    Set<String> leftNonKeyColumns = new HashSet<>(leftSourceTableMetadata.getColumnNames());
    leftNonKeyColumns.removeAll(primaryKeyColumns);

    Set<String> rightNonKeyColumns = new HashSet<>(rightSourceTableMetadata.getColumnNames());
    rightNonKeyColumns.removeAll(primaryKeyColumns);

    Set<String> conflictingColumns = new HashSet<>(leftNonKeyColumns);
    conflictingColumns.retainAll(rightNonKeyColumns);

    if (!conflictingColumns.isEmpty()) {
      throw new IllegalArgumentException(
          CoreError.VIRTUAL_TABLE_SOURCE_TABLES_HAVE_CONFLICTING_COLUMN_NAMES.buildMessage(
              ScalarDbUtils.getFullTableName(leftSourceNamespace, leftSourceTable),
              ScalarDbUtils.getFullTableName(rightSourceNamespace, rightSourceTable),
              conflictingColumns));
    }

    // Check that virtual tables are not used as sources
    Optional<VirtualTableInfo> leftSourceTableInfo =
        getVirtualTableInfo(leftSourceNamespace, leftSourceTable);
    if (leftSourceTableInfo.isPresent()) {
      throw new IllegalArgumentException(
          CoreError.VIRTUAL_TABLE_CANNOT_USE_VIRTUAL_TABLE_AS_SOURCE.buildMessage(
              ScalarDbUtils.getFullTableName(leftSourceNamespace, leftSourceTable)));
    }

    Optional<VirtualTableInfo> rightSourceTableInfo =
        getVirtualTableInfo(rightSourceNamespace, rightSourceTable);
    if (rightSourceTableInfo.isPresent()) {
      throw new IllegalArgumentException(
          CoreError.VIRTUAL_TABLE_CANNOT_USE_VIRTUAL_TABLE_AS_SOURCE.buildMessage(
              ScalarDbUtils.getFullTableName(rightSourceNamespace, rightSourceTable)));
    }

    try {
      admin.createVirtualTable(
          namespace,
          table,
          leftSourceNamespace,
          leftSourceTable,
          rightSourceNamespace,
          rightSourceTable,
          joinType,
          options);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          CoreError.CREATING_VIRTUAL_TABLE_FAILED.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table),
              ScalarDbUtils.getFullTableName(leftSourceNamespace, leftSourceTable),
              ScalarDbUtils.getFullTableName(rightSourceNamespace, rightSourceTable)),
          e);
    }
  }

  @Override
  public Optional<VirtualTableInfo> getVirtualTableInfo(String namespace, String table)
      throws ExecutionException {
    if (!tableExists(namespace, table)) {
      throw new IllegalArgumentException(
          CoreError.TABLE_NOT_FOUND.buildMessage(ScalarDbUtils.getFullTableName(namespace, table)));
    }

    try {
      return admin.getVirtualTableInfo(namespace, table);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          CoreError.GETTING_VIRTUAL_TABLE_INFO_FAILED.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table)),
          e);
    }
  }

  @Override
  public void close() {
    admin.close();
  }

  private boolean isTypeConversionValid(DataType from, DataType to) {
    if (from == to) {
      return true;
    }
    switch (from) {
      case BOOLEAN:
      case BIGINT:
      case DOUBLE:
      case BLOB:
      case DATE:
      case TIME:
      case TIMESTAMP:
      case TIMESTAMPTZ:
        return to == DataType.TEXT;
      case INT:
        return to == DataType.BIGINT || to == DataType.TEXT;
      case FLOAT:
        return to == DataType.DOUBLE || to == DataType.TEXT;
      case TEXT:
        return false;
      default:
        throw new AssertionError("Unknown data type: " + from);
    }
  }
}
