package com.scalar.db.common;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

public class CheckedDistributedStorageAdmin implements DistributedStorageAdmin {

  private final DistributedStorageAdmin admin;
  private final String systemNamespaceName;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public CheckedDistributedStorageAdmin(DistributedStorageAdmin admin, DatabaseConfig config) {
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
      throw new UnsupportedOperationException(
          CoreError.TRANSPARENT_DATA_ENCRYPTION_NOT_ENABLED.buildMessage());
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
      throw new UnsupportedOperationException(
          CoreError.TRANSPARENT_DATA_ENCRYPTION_NOT_ENABLED.buildMessage());
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
  public TableMetadata getImportTableMetadata(String namespace, String table)
      throws ExecutionException {
    try {
      return admin.getImportTableMetadata(namespace, table);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          CoreError.GETTING_IMPORT_TABLE_METADATA_FAILED.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table)),
          e);
    }
  }

  @Override
  public void importTable(String namespace, String table, Map<String, String> options)
      throws ExecutionException {
    TableMetadata tableMetadata = getTableMetadata(namespace, table);
    if (tableMetadata != null) {
      throw new IllegalArgumentException(
          CoreError.TABLE_ALREADY_EXISTS.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table)));
    }

    try {
      admin.importTable(namespace, table, options);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          CoreError.IMPORTING_TABLE_FAILED.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table)),
          e);
    }
  }

  @Override
  public void addRawColumnToTable(
      String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException {
    try {
      admin.addRawColumnToTable(namespace, table, columnName, columnType);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          CoreError.ADDING_RAW_COLUMN_TO_TABLE_FAILED.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table), columnName, columnType),
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
  public void close() {
    admin.close();
  }
}
