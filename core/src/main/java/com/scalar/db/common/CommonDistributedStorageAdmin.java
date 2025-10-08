package com.scalar.db.common;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.StorageInfo;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonDistributedStorageAdmin implements DistributedStorageAdmin {

  private static final Logger logger = LoggerFactory.getLogger(CommonDistributedStorageAdmin.class);

  private final DistributedStorageAdmin admin;

  /**
   * Whether to check if the namespace exists or not. Set false when the storage does not support
   * namespaces.
   */
  private final boolean checkNamespace;

  public CommonDistributedStorageAdmin(DistributedStorageAdmin admin) {
    this(admin, true);
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public CommonDistributedStorageAdmin(DistributedStorageAdmin admin, boolean checkNamespace) {
    this.admin = admin;
    this.checkNamespace = checkNamespace;
  }

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    if (checkNamespace && namespaceExists(namespace)) {
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
    if (checkNamespace && !namespaceExists(namespace)) {
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
    if (checkNamespace && !namespaceExists(namespace)) {
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
  public TableMetadata getImportTableMetadata(
      String namespace, String table, Map<String, DataType> overrideColumnsType)
      throws ExecutionException {
    try {
      return admin.getImportTableMetadata(namespace, table, overrideColumnsType);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          CoreError.GETTING_IMPORT_TABLE_METADATA_FAILED.buildMessage(
              ScalarDbUtils.getFullTableName(namespace, table)),
          e);
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
  public Set<String> getNamespaceNames() throws ExecutionException {
    logger.warn(
        "getNamespaceNames() extracts the namespace names of user tables dynamically. As a result, "
            + "only namespaces that contain tables are returned. Starting from ScalarDB 4.0, we "
            + "plan to improve the design to remove this limitation.");

    try {
      return admin.getNamespaceNames();
    } catch (ExecutionException e) {
      throw new ExecutionException(CoreError.GETTING_NAMESPACE_NAMES_FAILED.buildMessage(), e);
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
