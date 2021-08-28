package com.scalar.db.schemaloader.core;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.schemaloader.schema.CoordinatorSchema;
import com.scalar.db.schemaloader.schema.Table;
import com.scalar.db.service.StorageFactory;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaOperator.class);

  private final DistributedStorageAdmin admin;

  public SchemaOperator(DatabaseConfig dbConfig) {
    StorageFactory storageFactory = new StorageFactory(dbConfig);
    admin = storageFactory.getAdmin();
  }

  public void createTables(List<Table> tableList) {
    boolean hasTransactionTable = false;

    for (Table table : tableList) {
      if (table.isTransactionTable()) {
        hasTransactionTable = true;
      }
      try {
        admin.createNamespace(table.getNamespace(), true, table.getOptions());
      } catch (ExecutionException e) {
        LOGGER.warn("Create namespace " + table.getNamespace() + " failed. " + e.getCause());
      }
      try {
        admin.createTable(
            table.getNamespace(),
            table.getTable(),
            table.getTableMetadata(),
            false,
            table.getOptions());
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
                + " failed. "
                + e.getCause());
      }
    }

    if (hasTransactionTable) {
      CoordinatorSchema coordinatorSchema = new CoordinatorSchema();
      try {
        admin.createNamespace(coordinatorSchema.getNamespace(), true, null);
        admin.createTable(
            coordinatorSchema.getNamespace(),
            coordinatorSchema.getTable(),
            coordinatorSchema.getTableMetadata(),
            true,
            null);
      } catch (ExecutionException e) {
        LOGGER.warn("Failed on coordinator schema creation. " + e.getCause().getMessage());
      }
    }
  }

  public void deleteTables(List<Table> tableList) {
    List<String> namespaces = new ArrayList<>();
    for (Table table : tableList) {
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
        LOGGER.warn("Delete table " + table.getTable() + " failed. " + e.getCause());
      }
    }
    for (String namespace : namespaces) {
      try {
        if (admin.getNamespaceTableNames(namespace).isEmpty()) {
          try {
            admin.dropNamespace(namespace);
          } catch (ExecutionException e) {
            LOGGER.warn("Delete namespace " + namespace + " failed. " + e.getCause());
          }
        }
      } catch (ExecutionException e) {
        LOGGER.warn("Get table from namespace " + namespace + " failed. " + e.getCause());
      }
    }
  }
}
