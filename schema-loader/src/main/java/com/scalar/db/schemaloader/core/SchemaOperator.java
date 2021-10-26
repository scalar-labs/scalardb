package com.scalar.db.schemaloader.core;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.schemaloader.schema.Table;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdmin;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaOperator.class);

  private final DistributedStorageAdmin admin;
  private final ConsensusCommitAdmin consensusCommitAdmin;
  private final boolean isStorageSpecificCommand;

  public SchemaOperator(DatabaseConfig dbConfig, boolean isStorageSpecificCommand) {
    this.isStorageSpecificCommand = isStorageSpecificCommand;
    StorageFactory storageFactory = new StorageFactory(dbConfig);
    admin = storageFactory.getAdmin();
    consensusCommitAdmin =
        new ConsensusCommitAdmin(admin, new ConsensusCommitConfig(dbConfig.getProperties()));
  }

  @VisibleForTesting
  public SchemaOperator(
      DistributedStorageAdmin admin,
      ConsensusCommitAdmin consensusCommitAdmin,
      boolean isStorageSpecificCommand) {
    this.isStorageSpecificCommand = isStorageSpecificCommand;
    this.admin = admin;
    this.consensusCommitAdmin = consensusCommitAdmin;
  }

  public void createTables(List<Table> tableList, Map<String, String> metaOptions)
      throws ExecutionException {
    boolean hasTransactionTable = false;
    for (Table table : tableList) {
      String tableNamespace = table.getNamespace();
      if (!admin.namespaceExists(tableNamespace)) {
        try {
          admin.createNamespace(table.getNamespace(), true, table.getOptions());
        } catch (ExecutionException e) {
          throw new SchemaOperatorException(
              "Creating the namespace " + table.getNamespace() + " failed.", e);
        }
      }

      if (admin.tableExists(tableNamespace, table.getTable())) {
        LOGGER.warn(
            "Table "
                + table.getTable()
                + " in the namespace "
                + table.getNamespace()
                + " already existed.");
      } else {
        try {
          if (table.isTransactionTable()) {
            hasTransactionTable = true;
            consensusCommitAdmin.createTransactionalTable(
                table.getNamespace(),
                table.getTable(),
                table.getTableMetadata(),
                table.getOptions());
          } else {
            admin.createTable(
                table.getNamespace(),
                table.getTable(),
                table.getTableMetadata(),
                table.getOptions());
          }
          LOGGER.info(
              "Creating the table "
                  + table.getTable()
                  + " in the namespace "
                  + table.getNamespace()
                  + " succeeded.");
        } catch (ExecutionException e) {
          throw new SchemaOperatorException(
              "Creating the table "
                  + table.getTable()
                  + " in the namespace "
                  + table.getNamespace()
                  + " failed.",
              e);
        }
      }
    }

    if (hasTransactionTable && isStorageSpecificCommand) {
      createCoordinatorTable(metaOptions);
    }
  }

  public void createCoordinatorTable(Map<String, String> options) throws ExecutionException {
    if (admin.namespaceExists(Coordinator.NAMESPACE)) {
      if (consensusCommitAdmin.coordinatorTableExists()) {
        LOGGER.warn("Table coordinator already existed.");
        return;
      }
    }
    try {
      consensusCommitAdmin.createCoordinatorTable(options);
      LOGGER.info("Creating the coordinator table succeeded.");
    } catch (ExecutionException e) {
      throw new SchemaOperatorException("Creating the coordinator table failed.", e);
    }
  }

  public void deleteTables(List<Table> tableList) throws ExecutionException {
    Set<String> namespaces = new HashSet<>();
    boolean hasTransactionTable = false;
    for (Table table : tableList) {
      if (table.isTransactionTable()) {
        hasTransactionTable = true;
      }
      if (admin.namespaceExists(table.getNamespace())) {
        if (!admin.tableExists(table.getNamespace(), table.getTable())) {
          LOGGER.warn(
              "Table "
                  + table.getTable()
                  + " in the namespace "
                  + table.getNamespace()
                  + " doesn't exist.");
        } else {
          try {
            admin.dropTable(table.getNamespace(), table.getTable());
            LOGGER.info(
                "Deleting the table "
                    + table.getTable()
                    + " in the namespace "
                    + table.getNamespace()
                    + " succeeded.");
            namespaces.add(table.getNamespace());
          } catch (ExecutionException e) {
            throw new SchemaOperatorException(
                "Deleting the table " + table.getTable() + " failed.", e);
          }
        }
      }
    }

    if (hasTransactionTable && isStorageSpecificCommand) {
      dropCoordinatorTable();
    }

    for (String namespace : namespaces) {
      if (!admin.namespaceExists(namespace)) {
        LOGGER.warn("Namespace " + namespace + " doesn't exist for deleting.");
      } else {
        try {
          admin.dropNamespace(namespace);
        } catch (ExecutionException e) {
          throw new SchemaOperatorException("Deleting the namespace " + namespace + " failed.", e);
        }
      }
    }
  }

  public void dropCoordinatorTable() throws ExecutionException {
    if (!admin.namespaceExists(Coordinator.NAMESPACE)) {
      LOGGER.warn("Table coordinator doesn't exist.");
    } else {
      if (!consensusCommitAdmin.coordinatorTableExists()) {
        LOGGER.warn("Table coordinator doesn't exist.");
        return;
      }
      try {
        consensusCommitAdmin.dropCoordinatorTable();
        LOGGER.info("Deleting the coordinator table succeeded.");
      } catch (ExecutionException e) {
        throw new SchemaOperatorException("Deleting the coordinator table failed.", e);
      }
    }
  }

  public void close() {
    admin.close();
  }
}
