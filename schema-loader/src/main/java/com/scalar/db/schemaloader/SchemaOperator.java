package com.scalar.db.schemaloader;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.schemaloader.alteration.TableMetadataAlteration;
import com.scalar.db.schemaloader.alteration.TableMetadataAlterationProcessor;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.service.TransactionFactory;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class SchemaOperator implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(SchemaOperator.class);

  private final Supplier<DistributedStorageAdmin> storageAdmin;
  private final AtomicBoolean storageAdminLoaded = new AtomicBoolean();
  private final Supplier<DistributedTransactionAdmin> transactionAdmin;
  private final AtomicBoolean transactionAdminLoaded = new AtomicBoolean();
  private final TableMetadataAlterationProcessor alterationProcessor;

  public SchemaOperator(Path propertiesFilePath) throws IOException {
    this(StorageFactory.create(propertiesFilePath), TransactionFactory.create(propertiesFilePath));
  }

  public SchemaOperator(Properties properties) {
    this(StorageFactory.create(properties), TransactionFactory.create(properties));
  }

  // For the SpotBugs warning CT_CONSTRUCTOR_THROW
  @Override
  protected final void finalize() {}

  @VisibleForTesting
  SchemaOperator(StorageFactory storageFactory, TransactionFactory transactionFactory) {
    storageAdmin =
        Suppliers.memoize(
            () -> {
              storageAdminLoaded.set(true);
              return storageFactory.getStorageAdmin();
            });
    transactionAdmin =
        Suppliers.memoize(
            () -> {
              transactionAdminLoaded.set(true);
              return transactionFactory.getTransactionAdmin();
            });
    alterationProcessor = new TableMetadataAlterationProcessor();
  }

  @VisibleForTesting
  SchemaOperator(
      DistributedStorageAdmin storageAdmin,
      DistributedTransactionAdmin transactionAdmin,
      TableMetadataAlterationProcessor alterationProcessor) {
    this.storageAdmin = () -> storageAdmin;
    storageAdminLoaded.set(true);
    this.transactionAdmin = () -> transactionAdmin;
    transactionAdminLoaded.set(true);
    this.alterationProcessor = alterationProcessor;
  }

  public void createTables(List<TableSchema> tableSchemaList) throws SchemaLoaderException {
    for (TableSchema tableSchema : tableSchemaList) {
      String namespace = tableSchema.getNamespace();
      String tableName = tableSchema.getTable();

      createNamespace(namespace, tableSchema.getOptions());
      if (tableExists(namespace, tableName, tableSchema.isTransactionTable())) {
        logger.warn("Table {} in the namespace {} already exists", tableName, namespace);
      } else {
        createTable(tableSchema);
      }
    }
  }

  public void repairNamespaces(List<TableSchema> tableSchemaList) throws SchemaLoaderException {
    // Arbitrarily select the configuration of one of the table for each namespace for the
    // reparation according to:
    // - if the namespace contains only transaction table, use the configuration of the first table
    // listed
    // - if the namespace contains transaction and storage tables, use the configuration of the
    // first transaction table listed
    // - if the namespace contains only storage table, use the configuration of the first table
    // listed
    //
    // That means if tables of a namespace have different options (resource unit, replication
    // factor, etc.), only the option of the selected table will be used for the reparation
    Map<String, TableSchema> namespaceToSelectedTableSchema = new HashMap<>();
    for (TableSchema table : tableSchemaList) {
      TableSchema selectedTable = namespaceToSelectedTableSchema.get(table.getNamespace());

      if (selectedTable != null
          && !selectedTable.isTransactionTable()
          && table.isTransactionTable()) {
        // Case if a namespace contains storage and transaction tables
        // Replace the selected "storage" table by the first "transaction" table listed
        namespaceToSelectedTableSchema.put(table.getNamespace(), table);
      } else {
        namespaceToSelectedTableSchema.putIfAbsent(table.getNamespace(), table);
      }
    }
    for (TableSchema tableSchema : namespaceToSelectedTableSchema.values()) {
      try {
        if (tableSchema.isTransactionTable()) {
          transactionAdmin
              .get()
              .repairNamespace(tableSchema.getNamespace(), tableSchema.getOptions());
        } else {
          storageAdmin.get().repairNamespace(tableSchema.getNamespace(), tableSchema.getOptions());
        }
        logger.info("Repairing the namespace {} succeeded", tableSchema.getNamespace());
      } catch (ExecutionException e) {
        throw new SchemaLoaderException(
            "Repairing the namespace " + tableSchema.getNamespace() + " failed", e);
      }
    }
  }

  public void repairTables(List<TableSchema> tableSchemaList) throws SchemaLoaderException {
    for (TableSchema tableSchema : tableSchemaList) {
      String namespace = tableSchema.getNamespace();
      String tableName = tableSchema.getTable();
      try {
        if (tableSchema.isTransactionTable()) {
          transactionAdmin
              .get()
              .repairTable(
                  namespace, tableName, tableSchema.getTableMetadata(), tableSchema.getOptions());
        } else {
          storageAdmin
              .get()
              .repairTable(
                  namespace, tableName, tableSchema.getTableMetadata(), tableSchema.getOptions());
        }
        logger.info("Repairing the table {} in the namespace {} succeeded", tableName, namespace);
      } catch (ExecutionException e) {
        throw new SchemaLoaderException(
            "Repairing the table " + tableName + " in the namespace " + namespace + " failed", e);
      }
    }
  }

  private void createNamespace(String namespace, Map<String, String> options)
      throws SchemaLoaderException {
    try {
      // always use transactionAdmin since we are not sure this namespace is for transaction or
      // storage
      transactionAdmin.get().createNamespace(namespace, true, options);
    } catch (ExecutionException e) {
      throw new SchemaLoaderException("Creating the namespace " + namespace + " failed", e);
    }
  }

  private void createTable(TableSchema tableSchema) throws SchemaLoaderException {
    String namespace = tableSchema.getNamespace();
    String tableName = tableSchema.getTable();
    try {
      if (tableSchema.isTransactionTable()) {
        transactionAdmin
            .get()
            .createTable(
                namespace, tableName, tableSchema.getTableMetadata(), tableSchema.getOptions());
      } else {
        storageAdmin
            .get()
            .createTable(
                namespace, tableName, tableSchema.getTableMetadata(), tableSchema.getOptions());
      }
      logger.info("Creating the table {} in the namespace {} succeeded", tableName, namespace);
    } catch (ExecutionException e) {
      throw new SchemaLoaderException(
          "Creating the table " + tableName + " in the namespace " + namespace + " failed", e);
    }
  }

  public void deleteTables(List<TableSchema> tableSchemaList) throws SchemaLoaderException {
    Set<String> namespaces = new HashSet<>();
    for (TableSchema tableSchema : tableSchemaList) {
      String namespace = tableSchema.getNamespace();
      String tableName = tableSchema.getTable();
      boolean isTransactional = tableSchema.isTransactionTable();

      if (!tableExists(namespace, tableName, isTransactional)) {
        logger.warn("Table {} in the namespace {} doesn't exist", tableName, namespace);
      } else {
        dropTable(namespace, tableName, isTransactional);
        namespaces.add(namespace);
      }
    }
    dropNamespaces(namespaces);
  }

  private void dropTable(String namespace, String tableName, boolean isTransactional)
      throws SchemaLoaderException {
    try {
      if (isTransactional) {
        transactionAdmin.get().dropTable(namespace, tableName);
      } else {
        storageAdmin.get().dropTable(namespace, tableName);
      }
      logger.info("Deleting the table {} in the namespace {} succeeded", tableName, namespace);
    } catch (ExecutionException e) {
      throw new SchemaLoaderException(
          "Deleting the table " + tableName + " in the namespace " + namespace + " failed", e);
    }
  }

  private void dropNamespaces(Set<String> namespaces) throws SchemaLoaderException {
    for (String namespace : namespaces) {
      try {
        // always use transactionAdmin since we are not sure this namespace is for transaction or
        // storage
        if (transactionAdmin.get().getNamespaceTableNames(namespace).isEmpty()) {
          transactionAdmin.get().dropNamespace(namespace, true);
        }
      } catch (ExecutionException e) {
        throw new SchemaLoaderException("Deleting the namespace " + namespace + " failed", e);
      }
    }
  }

  private boolean tableExists(String namespace, String tableName, boolean isTransactional)
      throws SchemaLoaderException {
    try {
      if (isTransactional) {
        return transactionAdmin.get().tableExists(namespace, tableName);
      } else {
        return storageAdmin.get().tableExists(namespace, tableName);
      }
    } catch (ExecutionException e) {
      throw new SchemaLoaderException(
          "Checking the existence of the table "
              + tableName
              + " in the namespace "
              + namespace
              + " failed",
          e);
    }
  }

  public void createCoordinatorTables(Map<String, String> options) throws SchemaLoaderException {
    if (coordinatorTablesExist()) {
      logger.warn("The coordinator tables already exist");
      return;
    }
    try {
      transactionAdmin.get().createCoordinatorTables(options);
      logger.info("Creating the coordinator tables succeeded");
    } catch (ExecutionException e) {
      throw new SchemaLoaderException("Creating the coordinator tables failed", e);
    }
  }

  public void dropCoordinatorTables() throws SchemaLoaderException {
    if (!coordinatorTablesExist()) {
      logger.warn("The coordinator tables don't exist");
      return;
    }
    try {
      transactionAdmin.get().dropCoordinatorTables();
      logger.info("Deleting the coordinator tables succeeded");
    } catch (ExecutionException e) {
      throw new SchemaLoaderException("Deleting the coordinator tables failed", e);
    }
  }

  private boolean coordinatorTablesExist() throws SchemaLoaderException {
    try {
      return transactionAdmin.get().coordinatorTablesExist();
    } catch (ExecutionException e) {
      throw new SchemaLoaderException("Checking the existence of the coordinator tables failed", e);
    }
  }

  public void repairCoordinatorTables(Map<String, String> options) throws SchemaLoaderException {
    try {
      transactionAdmin.get().repairCoordinatorTables(options);
      logger.info("Repairing the coordinator tables succeeded");
    } catch (ExecutionException e) {
      throw new SchemaLoaderException("Repairing the coordinator tables failed", e);
    }
  }

  public void alterTables(List<TableSchema> tableSchemaList, Map<String, String> options)
      throws SchemaLoaderException {
    for (TableSchema tableSchema : tableSchemaList) {
      String namespace = tableSchema.getNamespace();
      String table = tableSchema.getTable();
      boolean isTransactional = tableSchema.isTransactionTable();

      try {
        if (!tableExists(namespace, table, isTransactional)) {
          throw new UnsupportedOperationException(
              String.format(
                  "Altering the table %s.%s is not possible as the table was not created beforehand",
                  namespace, table));
        }
        TableMetadata currentMetadata = getCurrentTableMetadata(namespace, table, isTransactional);
        TableMetadataAlteration metadataAlteration =
            alterationProcessor.computeAlteration(
                namespace, table, currentMetadata, tableSchema.getTableMetadata());
        if (metadataAlteration.hasAlterations()) {
          alterTable(namespace, table, isTransactional, metadataAlteration, options);
        } else {
          logger.info(
              String.format("No alterations were detected for the table %s.%s", namespace, table));
        }
      } catch (ExecutionException e) {
        throw new SchemaLoaderException(
            String.format("Altering the table %s.%s failed", namespace, table), e);
      }
    }
  }

  private TableMetadata getCurrentTableMetadata(
      String namespace, String table, boolean isTransactional) throws ExecutionException {
    if (isTransactional) {
      return transactionAdmin.get().getTableMetadata(namespace, table);
    } else {
      return storageAdmin.get().getTableMetadata(namespace, table);
    }
  }

  private void alterTable(
      String namespace,
      String table,
      boolean isTransactional,
      TableMetadataAlteration metadataAlteration,
      Map<String, String> options)
      throws ExecutionException {
    addNewColumnsToTable(namespace, table, metadataAlteration, isTransactional);
    addNewSecondaryIndexesToTable(namespace, table, metadataAlteration, isTransactional, options);
    deleteSecondaryIndexesFromTable(namespace, table, metadataAlteration, isTransactional);
  }

  private void deleteSecondaryIndexesFromTable(
      String namespace,
      String table,
      TableMetadataAlteration alteredMetadata,
      boolean isTransactional)
      throws ExecutionException {
    for (String deletedIndex : alteredMetadata.getDeletedSecondaryIndexNames()) {
      if (isTransactional) {
        transactionAdmin.get().dropIndex(namespace, table, deletedIndex);
      } else {
        storageAdmin.get().dropIndex(namespace, table, deletedIndex);
      }
      logger.info(
          String.format(
              "Deleting the secondary index %s from the table %s.%s succeeded",
              deletedIndex, namespace, table));
    }
  }

  private void addNewSecondaryIndexesToTable(
      String namespace,
      String table,
      TableMetadataAlteration alteredMetadata,
      boolean isTransactional,
      Map<String, String> options)
      throws ExecutionException {
    for (String addedIndex : alteredMetadata.getAddedSecondaryIndexNames()) {
      if (isTransactional) {
        transactionAdmin.get().createIndex(namespace, table, addedIndex, options);
      } else {
        storageAdmin.get().createIndex(namespace, table, addedIndex, options);
      }
      logger.info(
          String.format(
              "Adding the secondary index %s to the table %s.%s succeeded",
              addedIndex, namespace, table));
    }
  }

  private void addNewColumnsToTable(
      String namespace,
      String table,
      TableMetadataAlteration alteredMetadata,
      boolean isTransactional)
      throws ExecutionException {
    for (String addedColumn : alteredMetadata.getAddedColumnNames()) {
      DataType addedColumnDataType = alteredMetadata.getAddedColumnDataTypes().get(addedColumn);
      if (isTransactional) {
        transactionAdmin
            .get()
            .addNewColumnToTable(namespace, table, addedColumn, addedColumnDataType);
      } else {
        storageAdmin.get().addNewColumnToTable(namespace, table, addedColumn, addedColumnDataType);
      }
      logger.info(
          String.format(
              "Adding the column %s of type %s to the table %s.%s succeeded",
              addedColumn, addedColumnDataType, namespace, table));
    }
  }

  public void importTables(List<ImportTableSchema> tableSchemaList, Map<String, String> options)
      throws SchemaLoaderException {
    for (ImportTableSchema tableSchema : tableSchemaList) {
      String namespace = tableSchema.getNamespace();
      String table = tableSchema.getTable();
      try {
        if (tableSchema.isTransactionTable()) {
          transactionAdmin.get().importTable(namespace, table, options);
        } else {
          storageAdmin.get().importTable(namespace, table, options);
        }
        logger.info("Importing the table {} in the namespace {} succeeded", table, namespace);
      } catch (ExecutionException e) {
        throw new SchemaLoaderException(
            String.format("Importing the table %s.%s failed", namespace, table), e);
      }
    }
  }

  public void upgrade(Map<String, String> options) throws SchemaLoaderException {
    try {
      // As of 4.0.0, upgrade() implementation is identical for the storage or transaction admin.
      // This could change in the future but for now using either admin regardless of the schema is
      // fine
      transactionAdmin.get().upgrade(options);
      logger.info("Upgrading the environment succeeded.");
    } catch (ExecutionException e) {
      throw new RuntimeException("Upgrading the environment failed", e);
    }
  }

  @Override
  public void close() {
    if (storageAdminLoaded.get()) {
      storageAdmin.get().close();
    }
    if (transactionAdminLoaded.get()) {
      transactionAdmin.get().close();
    }
  }
}
