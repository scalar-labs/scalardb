package com.scalar.db.schemaloader;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.service.TransactionFactory;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdmin;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class SchemaOperator {
  private static final Logger logger = LoggerFactory.getLogger(SchemaOperator.class);

  private final DistributedStorageAdmin storageAdmin;
  private final DistributedTransactionAdmin transactionAdmin;

  public SchemaOperator(DatabaseConfig config) {
    StorageFactory storageFactory = new StorageFactory(config);
    storageAdmin = storageFactory.getAdmin();
    TransactionFactory transactionFactory = new TransactionFactory(config);
    transactionAdmin = transactionFactory.getTransactionAdmin();
  }

  @VisibleForTesting
  SchemaOperator(DistributedStorageAdmin admin, ConsensusCommitAdmin transactionAdmin) {
    this.storageAdmin = admin;
    this.transactionAdmin = transactionAdmin;
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
      if (tableSchema.isTransactionalTable()) {
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
        storageAdmin.dropNamespace(namespace, true);
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

  public void close() {
    storageAdmin.close();
    transactionAdmin.close();
  }
}
