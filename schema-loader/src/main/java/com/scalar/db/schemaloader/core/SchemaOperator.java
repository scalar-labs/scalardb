package com.scalar.db.schemaloader.core;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.schemaloader.schema.SchemaException;
import com.scalar.db.schemaloader.schema.SchemaParser;
import com.scalar.db.schemaloader.schema.Table;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdmin;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import java.io.IOException;
import java.nio.file.Path;
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
  private final boolean separateCoordinatorTable;

  public SchemaOperator(DatabaseConfig dbConfig, boolean separateCoordinatorTable) {
    this.separateCoordinatorTable = separateCoordinatorTable;
    StorageFactory storageFactory = new StorageFactory(dbConfig);
    admin = storageFactory.getAdmin();
    consensusCommitAdmin =
        new ConsensusCommitAdmin(admin, new ConsensusCommitConfig(dbConfig.getProperties()));
  }

  public SchemaOperator(
      DistributedStorageAdmin admin,
      ConsensusCommitAdmin consensusCommitAdmin,
      boolean separateCoordinatorTable) {
    this.separateCoordinatorTable = separateCoordinatorTable;
    this.admin = admin;
    this.consensusCommitAdmin = consensusCommitAdmin;
  }

  public void createTables(Path schemaFile, Map<String, String> metaOptions)
      throws SchemaOperatorException {
    List<Table> tableList;
    try {
      tableList = SchemaParser.parse(schemaFile, metaOptions);
    } catch (SchemaException | IOException e) {
      throw new SchemaOperatorException("Parsing the schema file failed.", e);
    }
    createTables(tableList, metaOptions);
  }

  public void createTables(String serializedSchemaJson, Map<String, String> metaOptions)
      throws SchemaOperatorException {
    List<Table> tableList;
    try {
      tableList = SchemaParser.parse(serializedSchemaJson, metaOptions);
    } catch (SchemaException e) {
      throw new SchemaOperatorException("Parsing the serialized schema json failed.", e);
    }
    createTables(tableList, metaOptions);
  }

  public void createTables(List<Table> tableList, Map<String, String> metaOptions)
      throws SchemaOperatorException {
    boolean hasTransactionTable = false;
    for (Table table : tableList) {
      String tableNamespace = table.getNamespace();
      String tableName = table.getTable();

      createNamespace(tableNamespace, table.getOptions());
      if (tableExists(tableNamespace, tableName)) {
        LOGGER.warn(
            "Table " + tableName + " in the namespace " + tableNamespace + " already exists.");
      } else {
        hasTransactionTable |= table.isTransactionTable();
        createTable(table);
      }
    }

    if (hasTransactionTable && !separateCoordinatorTable) {
      createCoordinatorTable(metaOptions);
    }
  }

  public void createCoordinatorTable(Map<String, String> options) throws SchemaOperatorException {
    if (coordinatorTableExists()) {
      LOGGER.warn("The coordinator table already exists.");
      return;
    }
    try {
      consensusCommitAdmin.createCoordinatorTable(options);
      LOGGER.info("Creating the coordinator table succeeded.");
    } catch (ExecutionException e) {
      throw new SchemaOperatorException("Creating the coordinator table failed.", e);
    }
  }

  public void deleteTables(Path schemaFile, Map<String, String> metaOptions)
      throws SchemaOperatorException {
    List<Table> tableList;
    try {
      tableList = SchemaParser.parse(schemaFile, metaOptions);
    } catch (SchemaException | IOException e) {
      throw new SchemaOperatorException("Parsing the schema file failed.", e);
    }
    deleteTables(tableList);
  }

  public void deleteTables(String serializedSchemaJson, Map<String, String> metaOptions)
      throws SchemaOperatorException {
    List<Table> tableList;
    try {
      tableList = SchemaParser.parse(serializedSchemaJson, metaOptions);
    } catch (SchemaException e) {
      throw new SchemaOperatorException("Parsing the serialized schema json failed.", e);
    }
    deleteTables(tableList);
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

      if (!tableExists(tableNamespace, tableName)) {
        LOGGER.warn(
            "Table " + tableName + " in the namespace " + tableNamespace + " doesn't exist.");
      } else {
        dropTable(tableNamespace, tableName);
        namespaces.add(tableNamespace);
      }
    }

    if (hasTransactionTable && !separateCoordinatorTable) {
      dropCoordinatorTable();
    }

    dropNamespaces(namespaces);
  }

  public void dropCoordinatorTable() throws SchemaOperatorException {
    if (!coordinatorTableExists()) {
      LOGGER.warn("The coordinator table doesn't exist.");
      return;
    }
    try {
      consensusCommitAdmin.dropCoordinatorTable();
      LOGGER.info("Deleting the coordinator table succeeded.");
    } catch (ExecutionException e) {
      throw new SchemaOperatorException("Deleting the coordinator table failed.", e);
    }
  }

  private boolean coordinatorTableExists() throws SchemaOperatorException {
    try {
      return consensusCommitAdmin.coordinatorTableExists();
    } catch (ExecutionException e) {
      throw new SchemaOperatorException(
          "Checking the existence of the coordinator table failed.", e);
    }
  }

  private boolean tableExists(String tableNamespace, String tableName)
      throws SchemaOperatorException {
    try {
      return admin.tableExists(tableNamespace, tableName);
    } catch (ExecutionException e) {
      throw new SchemaOperatorException(
          "Checking the existence of the table "
              + tableName
              + " in the namespace "
              + tableNamespace
              + " failed.",
          e);
    }
  }

  private void createNamespace(String tableNamespace, Map<String, String> options)
      throws SchemaOperatorException {
    try {
      admin.createNamespace(tableNamespace, true, options);
    } catch (ExecutionException e) {
      throw new SchemaOperatorException("Creating the namespace " + tableNamespace + " failed.", e);
    }
  }

  private void createTable(Table table) throws SchemaOperatorException {
    String tableNamespace = table.getNamespace();
    String tableName = table.getTable();
    try {
      if (table.isTransactionTable()) {
        consensusCommitAdmin.createTransactionalTable(
            tableNamespace, tableName, table.getTableMetadata(), table.getOptions());
      } else {
        admin.createTable(tableNamespace, tableName, table.getTableMetadata(), table.getOptions());
      }
      LOGGER.info(
          "Creating the table "
              + tableName
              + " in the namespace "
              + tableNamespace
              + " succeeded.");
    } catch (ExecutionException e) {
      throw new SchemaOperatorException(
          "Creating the table " + tableName + " in the namespace " + tableNamespace + " failed.",
          e);
    }
  }

  private void dropTable(String tableNamespace, String tableName) throws SchemaOperatorException {
    try {
      admin.dropTable(tableNamespace, tableName);
      LOGGER.info(
          "Deleting the table "
              + tableName
              + " in the namespace "
              + tableNamespace
              + " succeeded.");
    } catch (ExecutionException e) {
      throw new SchemaOperatorException(
          "Deleting the table " + tableName + " in the namespace " + tableNamespace + " failed.",
          e);
    }
  }

  private void dropNamespaces(Set<String> namespaces) throws SchemaOperatorException {
    for (String namespace : namespaces) {
      try {
        admin.dropNamespace(namespace, true);
      } catch (ExecutionException e) {
        throw new SchemaOperatorException("Deleting the namespace " + namespace + " failed.", e);
      }
    }
  }

  public void close() {
    admin.close();
  }
}
