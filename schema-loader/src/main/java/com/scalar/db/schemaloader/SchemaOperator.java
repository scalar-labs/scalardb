package com.scalar.db.schemaloader;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdmin;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class SchemaOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaOperator.class);

  private final DistributedStorageAdmin admin;
  private final ConsensusCommitAdmin consensusCommitAdmin;

  public SchemaOperator(DatabaseConfig config) {
    StorageFactory storageFactory = new StorageFactory(config);
    admin = storageFactory.getAdmin();
    consensusCommitAdmin =
        new ConsensusCommitAdmin(admin, new ConsensusCommitConfig(config.getProperties()));
  }

  @VisibleForTesting
  SchemaOperator(DistributedStorageAdmin admin, ConsensusCommitAdmin consensusCommitAdmin) {
    this.admin = admin;
    this.consensusCommitAdmin = consensusCommitAdmin;
  }

  public void createTables(List<TableSchema> tableSchemaList) throws SchemaLoaderException {
    for (TableSchema tableSchema : tableSchemaList) {
      String namespace = tableSchema.getNamespace();
      String tableName = tableSchema.getTable();

      createNamespace(namespace, tableSchema.getOptions());
      if (tableExists(namespace, tableName)) {
        LOGGER.warn("Table {} in the namespace {} already exists.", tableName, namespace);
      } else {
        createTable(tableSchema);
      }
    }
  }

  private void createNamespace(String namespace, Map<String, String> options)
      throws SchemaLoaderException {
    try {
      admin.createNamespace(namespace, true, options);
    } catch (ExecutionException e) {
      throw new SchemaLoaderException("Creating the namespace " + namespace + " failed.", e);
    }
  }

  private void createTable(TableSchema tableSchema) throws SchemaLoaderException {
    String namespace = tableSchema.getNamespace();
    String tableName = tableSchema.getTable();
    try {
      if (tableSchema.isTransactionalTable()) {
        consensusCommitAdmin.createTransactionalTable(
            namespace, tableName, tableSchema.getTableMetadata(), tableSchema.getOptions());
      } else {
        admin.createTable(
            namespace, tableName, tableSchema.getTableMetadata(), tableSchema.getOptions());
      }
      LOGGER.info("Creating the table {} in the namespace {} succeeded.", tableName, namespace);
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
        LOGGER.warn("Table {} in the namespace {} doesn't exist.", tableName, namespace);
      } else {
        dropTable(namespace, tableName);
        namespaces.add(namespace);
      }
    }
    dropNamespaces(namespaces);
  }

  private void dropTable(String namespace, String tableName) throws SchemaLoaderException {
    try {
      admin.dropTable(namespace, tableName);
      LOGGER.info("Deleting the table {} in the namespace {} succeeded.", tableName, namespace);
    } catch (ExecutionException e) {
      throw new SchemaLoaderException(
          "Deleting the table " + tableName + " in the namespace " + namespace + " failed.", e);
    }
  }

  private void dropNamespaces(Set<String> namespaces) throws SchemaLoaderException {
    for (String namespace : namespaces) {
      try {
        admin.dropNamespace(namespace, true);
      } catch (ExecutionException e) {
        throw new SchemaLoaderException("Deleting the namespace " + namespace + " failed.", e);
      }
    }
  }

  private boolean tableExists(String namespace, String tableName) throws SchemaLoaderException {
    try {
      return admin.tableExists(namespace, tableName);
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

  public void createCoordinatorTable(Map<String, String> options) throws SchemaLoaderException {
    if (coordinatorTableExists()) {
      LOGGER.warn("The coordinator table already exists.");
      return;
    }
    try {
      consensusCommitAdmin.createCoordinatorTable(options);
      LOGGER.info("Creating the coordinator table succeeded.");
    } catch (ExecutionException e) {
      throw new SchemaLoaderException("Creating the coordinator table failed.", e);
    }
  }

  public void dropCoordinatorTable() throws SchemaLoaderException {
    if (!coordinatorTableExists()) {
      LOGGER.warn("The coordinator table doesn't exist.");
      return;
    }
    try {
      consensusCommitAdmin.dropCoordinatorTable();
      LOGGER.info("Deleting the coordinator table succeeded.");
    } catch (ExecutionException e) {
      throw new SchemaLoaderException("Deleting the coordinator table failed.", e);
    }
  }

  private boolean coordinatorTableExists() throws SchemaLoaderException {
    try {
      return consensusCommitAdmin.coordinatorTableExists();
    } catch (ExecutionException e) {
      throw new SchemaLoaderException("Checking the existence of the coordinator table failed.", e);
    }
  }

  public void close() {
    admin.close();
  }
}
