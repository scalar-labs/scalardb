package com.scalar.db.schemaloader.core;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.schemaloader.schema.Table;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdmin;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
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

  public void createTables(List<Table> tableList, Map<String, String> metaOptions) {
    boolean hasTransactionTable = false;

    for (Table table : tableList) {
      try {
        admin.createNamespace(table.getNamespace(), true, table.getOptions());
      } catch (ExecutionException e) {
        LOGGER.warn("Create namespace " + table.getNamespace() + " failed.", e);
      }

      try {
        if (table.isTransactionTable()) {
          hasTransactionTable = true;
          consensusCommitAdmin.createTransactionalTable(
              table.getNamespace(), table.getTable(), table.getTableMetadata(), table.getOptions());
        } else {
          admin.createTable(
              table.getNamespace(), table.getTable(), table.getTableMetadata(), table.getOptions());
        }
        LOGGER.info(
            "Create table "
                + table.getTable()
                + " in namespace "
                + table.getNamespace()
                + " successfully.");
      } catch (ExecutionException e) {
        LOGGER.warn(
            "Create table "
                + table.getTable()
                + " in namespace "
                + table.getNamespace()
                + " failed.",
            e);
      }
    }

    if (hasTransactionTable && isStorageSpecificCommand) {
      createCoordinatorTable(metaOptions);
    }
  }

  public void createCoordinatorTable(Map<String, String> options) {
    try {
      consensusCommitAdmin.createCoordinatorTable(options);
      LOGGER.info("Create the coordinator table successfully.");
    } catch (ExecutionException e) {
      LOGGER.warn("Create the coordinator table failed.", e);
    }
  }

  public void deleteTables(List<Table> tableList) {
    Set<String> namespaces = new HashSet<>();
    boolean hasTransactionTable = false;
    for (Table table : tableList) {
      if (table.isTransactionTable()) {
        hasTransactionTable = true;
      }
      try {
        admin.dropTable(table.getNamespace(), table.getTable());
        LOGGER.info(
            "Deleted table "
                + table.getTable()
                + " in namespace "
                + table.getNamespace()
                + " successfully.");
        namespaces.add(table.getNamespace());
      } catch (ExecutionException e) {
        LOGGER.warn("Delete table " + table.getTable() + " failed.", e);
      }
    }
    if (hasTransactionTable && isStorageSpecificCommand) {
      dropCoordinatorTable();
    }

    for (String namespace : namespaces) {
      try {
        if (admin.getNamespaceTableNames(namespace).isEmpty()) {
          try {
            admin.dropNamespace(namespace);
          } catch (ExecutionException e) {
            LOGGER.warn("Delete namespace " + namespace + " failed.", e);
          }
        }
      } catch (ExecutionException e) {
        LOGGER.warn("Get table from namespace " + namespace + " failed.", e);
      }
    }
  }

  public void dropCoordinatorTable() {
    try {
      consensusCommitAdmin.dropCoordinatorTable();
      LOGGER.info("Delete the coordinator table successfully.");
    } catch (ExecutionException e) {
      LOGGER.warn("Delete the coordinator table failed.", e);
    }
  }

  public void close() {
    admin.close();
  }
}
