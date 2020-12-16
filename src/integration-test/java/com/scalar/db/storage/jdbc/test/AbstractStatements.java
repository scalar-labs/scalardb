package com.scalar.db.storage.jdbc.test;

import com.scalar.db.storage.jdbc.metadata.TableMetadataManager;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public abstract class AbstractStatements implements Statements {

  private final StatementsStrategy strategy;

  public AbstractStatements(StatementsStrategy strategy) {
    this.strategy = strategy;
  }

  protected List<String> dataSchemas(String schemaPrefix) {
    return strategy.dataSchemas(schemaPrefix);
  }

  protected List<String> dataTables(String schemaPrefix) {
    return strategy.dataTables(schemaPrefix);
  }

  @Override
  public List<String> createMetadataSchemaStatements(String schemaPrefix) {
    return Collections.singletonList(
        "CREATE SCHEMA " + TableMetadataManager.getSchema(schemaPrefix));
  }

  @Override
  public List<String> dropMetadataSchemaStatements(String schemaPrefix) {
    return Collections.singletonList("DROP SCHEMA " + TableMetadataManager.getSchema(schemaPrefix));
  }

  @Override
  public List<String> createMetadataTableStatements(String schemaPrefix) {
    return Collections.singletonList(
        "CREATE TABLE "
            + TableMetadataManager.getTable(schemaPrefix)
            + "(namespace VARCHAR(128),"
            + "table_name VARCHAR(128),"
            + "column_name VARCHAR(128),"
            + "data_type VARCHAR(20) NOT NULL,"
            + "key_type VARCHAR(20),"
            + "clustering_order VARCHAR(10),"
            + "indexed BOOLEAN,"
            + "ordinal_position INTEGER NOT NULL,"
            + "PRIMARY KEY (namespace, table_name, column_name))");
  }

  @Override
  public List<String> insertMetadataStatements(String schemaPrefix) {
    return strategy.insertMetadataStatements(schemaPrefix);
  }

  @Override
  public List<String> dropMetadataTableStatements(String schemaPrefix) {
    return Collections.singletonList("DROP TABLE " + TableMetadataManager.getTable(schemaPrefix));
  }

  @Override
  public List<String> createDataSchemaStatements(String schemaPrefix) {
    return dataSchemas(schemaPrefix).stream()
        .map(s -> "CREATE SCHEMA " + s)
        .collect(Collectors.toList());
  }

  @Override
  public List<String> dropDataSchemaStatements(String schemaPrefix) {
    return dataSchemas(schemaPrefix).stream()
        .map(s -> "DROP SCHEMA " + s)
        .collect(Collectors.toList());
  }

  @Override
  public List<String> createDataTableStatements(String schemaPrefix) {
    return strategy.createDataTableStatements(schemaPrefix);
  }

  @Override
  public List<String> dropDataTableStatements(String schemaPrefix) {
    return dataTables(schemaPrefix).stream()
        .map(t -> "DROP TABLE " + t)
        .collect(Collectors.toList());
  }
}
