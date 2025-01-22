package com.scalar.db.transaction.singlecrudoperation;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.service.StorageFactory;
import java.util.Map;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class SingleCrudOperationTransactionAdmin implements DistributedTransactionAdmin {

  private final DistributedStorageAdmin distributedStorageAdmin;

  @Inject
  public SingleCrudOperationTransactionAdmin(DatabaseConfig databaseConfig) {
    StorageFactory storageFactory = StorageFactory.create(databaseConfig.getProperties());
    distributedStorageAdmin = storageFactory.getStorageAdmin();
  }

  @VisibleForTesting
  SingleCrudOperationTransactionAdmin(DistributedStorageAdmin distributedStorageAdmin) {
    this.distributedStorageAdmin = distributedStorageAdmin;
  }

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    distributedStorageAdmin.createNamespace(namespace, options);
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    distributedStorageAdmin.createTable(namespace, table, metadata, options);
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    distributedStorageAdmin.dropTable(namespace, table);
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    distributedStorageAdmin.dropNamespace(namespace);
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    distributedStorageAdmin.truncateTable(namespace, table);
  }

  @Override
  public void createIndex(
      String namespace, String table, String columnName, Map<String, String> options)
      throws ExecutionException {
    distributedStorageAdmin.createIndex(namespace, table, columnName, options);
  }

  @Override
  public void dropIndex(String namespace, String table, String columnName)
      throws ExecutionException {
    distributedStorageAdmin.dropIndex(namespace, table, columnName);
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    return distributedStorageAdmin.getTableMetadata(namespace, table);
  }

  @Override
  public Set<String> getNamespaceTableNames(String namespace) throws ExecutionException {
    return distributedStorageAdmin.getNamespaceTableNames(namespace);
  }

  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    return distributedStorageAdmin.namespaceExists(namespace);
  }

  @Override
  public void importTable(
      String namespace,
      String table,
      Map<String, String> options,
      Map<String, DataType> overrideColumnsType)
      throws ExecutionException {
    distributedStorageAdmin.importTable(namespace, table, options, overrideColumnsType);
  }

  @Override
  public void repairTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    distributedStorageAdmin.repairTable(namespace, table, metadata, options);
  }

  @Override
  public void addNewColumnToTable(
      String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException {
    distributedStorageAdmin.addNewColumnToTable(namespace, table, columnName, columnType);
  }

  @Override
  public Set<String> getNamespaceNames() throws ExecutionException {
    return distributedStorageAdmin.getNamespaceNames();
  }

  /**
   * {@inheritDoc}
   *
   * <p>Note that it does nothing since the single CRUD operation transactions don't have
   * coordinator tables.
   */
  @Override
  public void createCoordinatorTables(Map<String, String> options) {
    // Do nothing since the single CRUD operation transactions don't have coordinator tables
  }

  /**
   * {@inheritDoc}
   *
   * <p>Note that it does nothing since the single CRUD operation transactions don't have
   * coordinator tables.
   */
  @Override
  public void dropCoordinatorTables() {
    // Do nothing since the single CRUD operation transactions don't have coordinator tables
  }

  /**
   * {@inheritDoc}
   *
   * <p>Note that it does nothing since the single CRUD operation transactions don't have
   * coordinator tables.
   */
  @Override
  public void truncateCoordinatorTables() {
    // Do nothing since the single CRUD operation transactions don't have coordinator tables
  }

  /**
   * {@inheritDoc}
   *
   * <p>Note that it always returns true since the single CRUD operation transactions don't have
   * coordinator tables.
   */
  @Override
  public boolean coordinatorTablesExist() {
    // Always return true since the single CRUD operation transactions don't have coordinator tables
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Note that it does nothing since the single CRUD operation transactions don't have
   * coordinator tables.
   */
  @Override
  public void repairCoordinatorTables(Map<String, String> options) {
    // Do nothing since the single CRUD operation transactions don't have coordinator tables
  }

  @Override
  public void close() {
    distributedStorageAdmin.close();
  }
}
