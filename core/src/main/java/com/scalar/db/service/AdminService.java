package com.scalar.db.service;

import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;

/** @deprecated As of release 3.5.0. Will be removed in release 5.0.0 */
@Deprecated
@ThreadSafe
public class AdminService implements DistributedStorageAdmin {

  private final DistributedStorageAdmin admin;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  @Inject
  public AdminService(DistributedStorageAdmin admin) {
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
  public void repairTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    admin.repairTable(namespace, table, metadata, options);
  }

  @Override
  public void addNewColumnToTable(
      String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException {
    admin.addNewColumnToTable(namespace, table, columnName, columnType);
  }

  @Override
  public TableMetadata getImportTableMetadata(String namespace, String table)
      throws ExecutionException {
    return admin.getImportTableMetadata(namespace, table);
  }

  @Override
  public void addRawColumnToTable(
      String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException {
    admin.addRawColumnToTable(namespace, table, columnName, columnType);
  }

  @Override
  public void importTable(String namespace, String table) throws ExecutionException {
    admin.importTable(namespace, table);
  }

  @Override
  public Set<String> getNamespaceNames() throws ExecutionException {
    return admin.getNamespaceNames();
  }

  @Override
  public void repairNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    admin.repairNamespace(namespace, options);
  }

  @Override
  public void upgrade(Map<String, String> options) throws ExecutionException {
    admin.upgrade(options);
  }

  @Override
  public void close() {
    admin.close();
  }
}
