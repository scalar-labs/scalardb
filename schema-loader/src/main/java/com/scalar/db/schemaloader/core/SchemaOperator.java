package com.scalar.db.schemaloader.core;

import com.google.common.annotations.VisibleForTesting;
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
      throws SchemaOperatorException {
    boolean hasTransactionTable = false;
    for (Table table : tableList) {
      String tableNamespace = table.getNamespace();
      String tableName = table.getTable();
      boolean namespaceExists;
      try {
        namespaceExists = admin.namespaceExists(tableNamespace);
      } catch (ExecutionException e) {
        throw new SchemaOperatorException(
            "Checking the existence of the namespace " + tableNamespace + " failed.", e);
      }
      if (!namespaceExists) {
        try {
          admin.createNamespace(tableNamespace, true, table.getOptions());
        } catch (ExecutionException e) {
          throw new SchemaOperatorException(
              "Creating the namespace " + tableNamespace + " failed.", e);
        }
      }
      boolean tableExists;
      try {
        tableExists = admin.tableExists(tableNamespace, tableName);
      } catch (ExecutionException e) {
        throw new SchemaOperatorException(
            "Checking the existence of the table " + tableName + " failed.", e);
      }
      if (tableExists) {
        LOGGER.warn(
            "Table " + tableName + " in the namespace " + tableNamespace + " already existed.");
      } else {
        try {
          if (table.isTransactionTable()) {
            hasTransactionTable = true;
            consensusCommitAdmin.createTransactionalTable(
                tableNamespace, tableName, table.getTableMetadata(), table.getOptions());
          } else {
            admin.createTable(
                tableNamespace, tableName, table.getTableMetadata(), table.getOptions());
          }
          LOGGER.info(
              "Creating the table "
                  + tableName
                  + " in the namespace "
                  + tableNamespace
                  + " succeeded.");
        } catch (ExecutionException e) {
          throw new SchemaOperatorException(
              "Creating the table "
                  + tableName
                  + " in the namespace "
                  + tableNamespace
                  + " failed.",
              e);
        }
      }
    }

    if (hasTransactionTable && isStorageSpecificCommand) {
      createCoordinatorTable(metaOptions);
    }
  }

  public void createCoordinatorTable(Map<String, String> options) throws SchemaOperatorException {
    boolean coordinatorTableExists;
    try {
      coordinatorTableExists = consensusCommitAdmin.coordinatorTableExists();
    } catch (ExecutionException e) {
      throw new SchemaOperatorException(
          "Checking the existence of the table coordinator failed.", e);
    }
    if (coordinatorTableExists) {
      LOGGER.warn("Table coordinator already existed.");
      return;
    }
    try {
      consensusCommitAdmin.createCoordinatorTable(options);
      LOGGER.info("Creating the coordinator table succeeded.");
    } catch (ExecutionException e) {
      throw new SchemaOperatorException("Creating the coordinator table failed.", e);
    }
  }

  public void deleteTables(List<Table> tableList) throws SchemaOperatorException {
    Set<String> namespaces = new HashSet<>();
    boolean hasTransactionTable = false;
    for (Table table : tableList) {
      if (table.isTransactionTable()) {
        hasTransactionTable = true;
      }
      String tableNamespace = table.getNamespace();
      String tableName = table.getTable();
      boolean namespaceExists;
      try {
        namespaceExists = admin.namespaceExists(tableNamespace);
      } catch (ExecutionException e) {
        throw new SchemaOperatorException(
            "Checking the existence of the namespace " + tableNamespace + " failed.", e);
      }
      boolean tableExists;
      try {
        tableExists = admin.tableExists(tableNamespace, tableName);
      } catch (ExecutionException e) {
        throw new SchemaOperatorException(
            "Checking the existence of the table " + tableName + " failed.", e);
      }

      if (namespaceExists) {
        if (!tableExists) {
          LOGGER.warn(
              "Table " + tableName + " in the namespace " + tableNamespace + " doesn't exist.");
        } else {
          try {
            admin.dropTable(tableNamespace, tableName);
            LOGGER.info(
                "Deleting the table "
                    + tableName
                    + " in the namespace "
                    + tableNamespace
                    + " succeeded.");
            namespaces.add(tableNamespace);
          } catch (ExecutionException e) {
            throw new SchemaOperatorException("Deleting the table " + tableName + " failed.", e);
          }
        }
      }
    }

    if (hasTransactionTable && isStorageSpecificCommand) {
      dropCoordinatorTable();
    }

    for (String namespace : namespaces) {
      boolean namespaceExists;
      try {
        namespaceExists = admin.namespaceExists(namespace);
      } catch (ExecutionException e) {
        throw new SchemaOperatorException(
            "Checking the existence of the namespace " + namespace + " failed.", e);
      }
      if (!namespaceExists) {
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

  public void dropCoordinatorTable() throws SchemaOperatorException {
    boolean coordinatorTableExists;
    try {
      coordinatorTableExists = consensusCommitAdmin.coordinatorTableExists();
    } catch (ExecutionException e) {
      throw new SchemaOperatorException(
          "Checking the existence of the table coordinator failed.", e);
    }
    if (!coordinatorTableExists) {
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

  public void close() {
    admin.close();
  }
}
