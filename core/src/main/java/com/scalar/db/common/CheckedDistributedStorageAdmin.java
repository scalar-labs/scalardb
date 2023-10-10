package com.scalar.db.common;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

public class CheckedDistributedStorageAdmin implements DistributedStorageAdmin {

  private final DistributedStorageAdmin admin;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public CheckedDistributedStorageAdmin(DistributedStorageAdmin admin) {
    this.admin = admin;
  }

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    if (namespaceExists(namespace)) {
      throw new IllegalArgumentException("Namespace already exists: " + namespace);
    }

    try {
      admin.createNamespace(namespace, options);
    } catch (ExecutionException e) {
      throw new ExecutionException("Creating the namespace failed: " + namespace, e);
    }
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    if (!namespaceExists(namespace)) {
      throw new IllegalArgumentException("Namespace does not exist: " + namespace);
    }
    if (tableExists(namespace, table)) {
      throw new IllegalArgumentException(
          "Table already exists: " + ScalarDbUtils.getFullTableName(namespace, table));
    }

    try {
      admin.createTable(namespace, table, metadata, options);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          "Creating the table failed: " + ScalarDbUtils.getFullTableName(namespace, table), e);
    }
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    if (!tableExists(namespace, table)) {
      throw new IllegalArgumentException(
          "Table does not exist: " + ScalarDbUtils.getFullTableName(namespace, table));
    }

    try {
      admin.dropTable(namespace, table);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          "Dropping the table failed: " + ScalarDbUtils.getFullTableName(namespace, table), e);
    }
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    if (!namespaceExists(namespace)) {
      throw new IllegalArgumentException("Namespace does not exist: " + namespace);
    }
    if (!getNamespaceTableNames(namespace).isEmpty()) {
      throw new IllegalArgumentException(
          "Namespace is not empty: " + namespace + ", " + getNamespaceTableNames(namespace));
    }

    try {
      admin.dropNamespace(namespace);
    } catch (ExecutionException e) {
      throw new ExecutionException("Dropping the namespace failed: " + namespace, e);
    }
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    if (!tableExists(namespace, table)) {
      throw new IllegalArgumentException(
          "Table does not exist: " + ScalarDbUtils.getFullTableName(namespace, table));
    }

    try {
      admin.truncateTable(namespace, table);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          "Truncating the table failed: " + ScalarDbUtils.getFullTableName(namespace, table), e);
    }
  }

  @Override
  public void createIndex(
      String namespace, String table, String columnName, Map<String, String> options)
      throws ExecutionException {
    TableMetadata tableMetadata = getTableMetadata(namespace, table);
    if (tableMetadata == null) {
      throw new IllegalArgumentException(
          "Table does not exist: " + ScalarDbUtils.getFullTableName(namespace, table));
    }
    if (!tableMetadata.getColumnNames().contains(columnName)) {
      throw new IllegalArgumentException(
          "Column does not exist: "
              + ScalarDbUtils.getFullTableName(namespace, table)
              + ", "
              + columnName);
    }

    if (indexExists(namespace, table, columnName)) {
      throw new IllegalArgumentException(
          "Index already exists: "
              + ScalarDbUtils.getFullTableName(namespace, table)
              + ", "
              + columnName);
    }

    try {
      admin.createIndex(namespace, table, columnName, options);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          "Creating the index failed: "
              + ScalarDbUtils.getFullTableName(namespace, table)
              + ", "
              + columnName,
          e);
    }
  }

  @Override
  public void dropIndex(String namespace, String table, String columnName)
      throws ExecutionException {
    if (!tableExists(namespace, table)) {
      throw new IllegalArgumentException(
          "Table does not exist: " + ScalarDbUtils.getFullTableName(namespace, table));
    }

    if (!indexExists(namespace, table, columnName)) {
      throw new IllegalArgumentException(
          "Index does not exist: "
              + ScalarDbUtils.getFullTableName(namespace, table)
              + ", "
              + columnName);
    }

    try {
      admin.dropIndex(namespace, table, columnName);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          "Dropping the index failed: "
              + ScalarDbUtils.getFullTableName(namespace, table)
              + ", "
              + columnName,
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
          "Getting the table metadata failed: " + ScalarDbUtils.getFullTableName(namespace, table),
          e);
    }
  }

  @Override
  public Set<String> getNamespaceTableNames(String namespace) throws ExecutionException {
    try {
      return admin.getNamespaceTableNames(namespace);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          "Getting the table names in the namespace failed: " + namespace, e);
    }
  }

  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    try {
      return admin.namespaceExists(namespace);
    } catch (ExecutionException e) {
      throw new ExecutionException("Checking if the namespace exists failed: " + namespace, e);
    }
  }

  @Override
  public boolean tableExists(String namespace, String table) throws ExecutionException {
    try {
      return DistributedStorageAdmin.super.tableExists(namespace, table);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          "Checking if the table exists failed: "
              + ScalarDbUtils.getFullTableName(namespace, table),
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
          "Checking if the index exists failed: "
              + ScalarDbUtils.getFullTableName(namespace, table)
              + ", "
              + columnName,
          e);
    }
  }

  @Override
  public void repairTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    try {
      admin.repairTable(namespace, table, metadata, options);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          "Repairing the table failed: " + ScalarDbUtils.getFullTableName(namespace, table), e);
    }
  }

  @Override
  public void addNewColumnToTable(
      String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException {
    TableMetadata tableMetadata = getTableMetadata(namespace, table);
    if (tableMetadata == null) {
      throw new IllegalArgumentException(
          "Table does not exist: " + ScalarDbUtils.getFullTableName(namespace, table));
    }

    if (tableMetadata.getColumnNames().contains(columnName)) {
      throw new IllegalArgumentException(
          "Column already exists: "
              + ScalarDbUtils.getFullTableName(namespace, table)
              + ", "
              + columnName);
    }

    try {
      admin.addNewColumnToTable(namespace, table, columnName, columnType);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          "Adding new column to the table failed: "
              + ScalarDbUtils.getFullTableName(namespace, table)
              + ", "
              + columnName,
          e);
    }
  }

  @Override
  public Set<String> getNamespaceNames() throws ExecutionException {
    try {
      return admin.getNamespaceNames();
    } catch (ExecutionException e) {
      throw new ExecutionException("Getting the namespace names failed", e);
    }
  }

  @Override
  public TableMetadata getImportTableMetadata(String namespace, String table)
      throws ExecutionException {
    try {
      return admin.getImportTableMetadata(namespace, table);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          "Getting the table metadata of the importing table failed: "
              + ScalarDbUtils.getFullTableName(namespace, table),
          e);
    }
  }

  @Override
  public void importTable(String namespace, String table) throws ExecutionException {
    TableMetadata tableMetadata = getTableMetadata(namespace, table);
    if (tableMetadata != null) {
      throw new IllegalArgumentException(
          "Table already exists: " + ScalarDbUtils.getFullTableName(namespace, table));
    }

    try {
      admin.importTable(namespace, table);
    } catch (ExecutionException e) {
      throw new ExecutionException(
          "Importing the table failed: " + ScalarDbUtils.getFullTableName(namespace, table), e);
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
          "Adding the raw column to the table failed: "
              + ScalarDbUtils.getFullTableName(namespace, table),
          e);
    }
  }

  @Override
  public void repairNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    try {
      admin.repairNamespace(namespace, options);
    } catch (ExecutionException e) {
      throw new ExecutionException("Repairing the namespace failed: " + namespace, e);
    }
  }

  @Override
  public void close() {
    admin.close();
  }
}
