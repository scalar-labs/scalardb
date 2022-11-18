package com.scalar.db.schemaloader;

import com.google.common.annotations.VisibleForTesting;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class SchemaOperator {
  private static final Logger logger = LoggerFactory.getLogger(SchemaOperator.class);

  private final DistributedStorageAdmin storageAdmin;
  private final DistributedTransactionAdmin transactionAdmin;
  private final TableMetadataAlterationProcessor alterationProcessor;

  public SchemaOperator(Path propertiesFilePath) throws IOException {
    StorageFactory storageFactory = StorageFactory.create(propertiesFilePath);
    storageAdmin = storageFactory.getStorageAdmin();
    TransactionFactory transactionFactory = TransactionFactory.create(propertiesFilePath);
    transactionAdmin = transactionFactory.getTransactionAdmin();
    alterationProcessor = new TableMetadataAlterationProcessor();
  }

  public SchemaOperator(Properties properties) {
    StorageFactory storageFactory = StorageFactory.create(properties);
    storageAdmin = storageFactory.getStorageAdmin();
    TransactionFactory transactionFactory = TransactionFactory.create(properties);
    transactionAdmin = transactionFactory.getTransactionAdmin();
    alterationProcessor = new TableMetadataAlterationProcessor();
  }

  @VisibleForTesting
  SchemaOperator(
      DistributedStorageAdmin storageAdmin,
      DistributedTransactionAdmin transactionAdmin,
      TableMetadataAlterationProcessor alterationProcessor) {
    this.storageAdmin = storageAdmin;
    this.transactionAdmin = transactionAdmin;
    this.alterationProcessor = alterationProcessor;
  }

  public void createTables(List<TableSchema> tableSchemaList) throws SchemaLoaderException {
    for (TableSchema tableSchema : tableSchemaList) {
      String namespace = tableSchema.getNamespace();
      String tableName = tableSchema.getTable();

      createNamespace(namespace, tableSchema.getOptions());
      if (tableExists(namespace, tableName)) {
        logger.warn("Table {} in the namespace {} already exists.", tableName, namespace);
      } else {
        createTable(tableSchema);
      }
    }
  }

  public void repairTables(List<TableSchema> tableSchemaList) throws SchemaLoaderException {
    for (TableSchema tableSchema : tableSchemaList) {
      String namespace = tableSchema.getNamespace();
      String tableName = tableSchema.getTable();
      try {
        if (tableSchema.isTransactionTable()) {
          transactionAdmin.repairTable(
              namespace, tableName, tableSchema.getTableMetadata(), tableSchema.getOptions());
        } else {
          storageAdmin.repairTable(
              namespace, tableName, tableSchema.getTableMetadata(), tableSchema.getOptions());
        }
        logger.info("Repairing the table {} in the namespace {} succeeded.", tableName, namespace);
      } catch (ExecutionException e) {
        throw new SchemaLoaderException(
            "Repairing the table " + tableName + " in the namespace " + namespace + " failed.", e);
      }
    }
  }

  private void createNamespace(String namespace, Map<String, String> options)
      throws SchemaLoaderException {
    try {
      storageAdmin.createNamespace(namespace, true, options);
    } catch (ExecutionException e) {
      throw new SchemaLoaderException("Creating the namespace " + namespace + " failed.", e);
    }
  }

  private void createTable(TableSchema tableSchema) throws SchemaLoaderException {
    String namespace = tableSchema.getNamespace();
    String tableName = tableSchema.getTable();
    try {
      if (tableSchema.isTransactionTable()) {
        transactionAdmin.createTable(
            namespace, tableName, tableSchema.getTableMetadata(), tableSchema.getOptions());
      } else {
        storageAdmin.createTable(
            namespace, tableName, tableSchema.getTableMetadata(), tableSchema.getOptions());
      }
      logger.info("Creating the table {} in the namespace {} succeeded.", tableName, namespace);
    } catch (ExecutionException e) {
      throw new SchemaLoaderException(
          "Creating the table " + tableName + " in the namespace " + namespace + " failed.", e);
    }
  }

  public void deleteTables(List<TableSchema> tableSchemaList) throws SchemaLoaderException {
    Set<String> namespaces = new HashSet<>();
    for (TableSchema tableSchema : tableSchemaList) {
      String namespace = tableSchema.getNamespace();
      String tableName = tableSchema.getTable();

      if (!tableExists(namespace, tableName)) {
        logger.warn("Table {} in the namespace {} doesn't exist.", tableName, namespace);
      } else {
        dropTable(namespace, tableName);
        namespaces.add(namespace);
      }
    }
    dropNamespaces(namespaces);
  }

  private void dropTable(String namespace, String tableName) throws SchemaLoaderException {
    try {
      storageAdmin.dropTable(namespace, tableName);
      logger.info("Deleting the table {} in the namespace {} succeeded.", tableName, namespace);
    } catch (ExecutionException e) {
      throw new SchemaLoaderException(
          "Deleting the table " + tableName + " in the namespace " + namespace + " failed.", e);
    }
  }

  private void dropNamespaces(Set<String> namespaces) throws SchemaLoaderException {
    for (String namespace : namespaces) {
      try {
        if (storageAdmin.getNamespaceTableNames(namespace).isEmpty()) {
          storageAdmin.dropNamespace(namespace, true);
        }
      } catch (ExecutionException e) {
        throw new SchemaLoaderException("Deleting the namespace " + namespace + " failed.", e);
      }
    }
  }

  private boolean tableExists(String namespace, String tableName) throws SchemaLoaderException {
    try {
      return storageAdmin.tableExists(namespace, tableName);
    } catch (ExecutionException e) {
      throw new SchemaLoaderException(
          "Checking the existence of the table "
              + tableName
              + " in the namespace "
              + namespace
              + " failed.",
          e);
    }
  }

  public void createCoordinatorTables(Map<String, String> options) throws SchemaLoaderException {
    if (coordinatorTablesExist()) {
      logger.warn("The coordinator tables already exist.");
      return;
    }
    try {
      transactionAdmin.createCoordinatorTables(options);
      logger.info("Creating the coordinator tables succeeded.");
    } catch (ExecutionException e) {
      throw new SchemaLoaderException("Creating the coordinator tables failed.", e);
    }
  }

  public void dropCoordinatorTables() throws SchemaLoaderException {
    if (!coordinatorTablesExist()) {
      logger.warn("The coordinator tables don't exist.");
      return;
    }
    try {
      transactionAdmin.dropCoordinatorTables();
      logger.info("Deleting the coordinator tables succeeded.");
    } catch (ExecutionException e) {
      throw new SchemaLoaderException("Deleting the coordinator tables failed.", e);
    }
  }

  private boolean coordinatorTablesExist() throws SchemaLoaderException {
    try {
      return transactionAdmin.coordinatorTablesExist();
    } catch (ExecutionException e) {
      throw new SchemaLoaderException(
          "Checking the existence of the coordinator tables failed.", e);
    }
  }

  public void repairCoordinatorTables(Map<String, String> options) throws SchemaLoaderException {
    try {
      transactionAdmin.repairCoordinatorTables(options);
      logger.info("Repairing the coordinator tables succeeded.");
    } catch (ExecutionException e) {
      throw new SchemaLoaderException("Repairing the coordinator tables failed.", e);
    }
  }

  public void alterTables(List<TableSchema> tableSchemaList, Map<String, String> options)
      throws SchemaLoaderException {
    for (TableSchema tableSchema : tableSchemaList) {
      String namespace = tableSchema.getNamespace();
      String table = tableSchema.getTable();
      boolean isTransactional = tableSchema.isTransactionTable();

      try {
        if (!tableExists(namespace, table)) {
          throw new UnsupportedOperationException(
              String.format(
                  "Altering the table %s.%s is not possible as the table was not created beforehand.",
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
              String.format("No alterations were detected for the table %s.%s.", namespace, table));
        }
      } catch (ExecutionException e) {
        throw new SchemaLoaderException(
            String.format("Altering the table %s.%s failed.", namespace, table), e);
      }
    }
  }

  private TableMetadata getCurrentTableMetadata(
      String namespace, String table, boolean isTransactional) throws ExecutionException {
    if (isTransactional) {
      return transactionAdmin.getTableMetadata(namespace, table);
    } else {
      return storageAdmin.getTableMetadata(namespace, table);
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
        transactionAdmin.dropIndex(namespace, table, deletedIndex);
      } else {
        storageAdmin.dropIndex(namespace, table, deletedIndex);
      }
      logger.info(
          String.format(
              "Deleting the secondary index %s from the table %s.%s succeeded.",
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
        transactionAdmin.createIndex(namespace, table, addedIndex, options);
      } else {
        storageAdmin.createIndex(namespace, table, addedIndex, options);
      }
      logger.info(
          String.format(
              "Adding the secondary index %s to the table %s.%s succeeded.",
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
        transactionAdmin.addNewColumnToTable(namespace, table, addedColumn, addedColumnDataType);
      } else {
        storageAdmin.addNewColumnToTable(namespace, table, addedColumn, addedColumnDataType);
      }
      logger.info(
          String.format(
              "Adding the column %s of type %s to the table %s.%s succeeded.",
              addedColumn, addedColumnDataType, namespace, table));
    }
  }

  public void close() {
    storageAdmin.close();
    transactionAdmin.close();
  }
}
