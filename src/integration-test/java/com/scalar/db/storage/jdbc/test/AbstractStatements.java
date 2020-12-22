package com.scalar.db.storage.jdbc.test;

import com.scalar.db.storage.jdbc.metadata.TableMetadataManager;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public abstract class AbstractStatements implements Statements {

  private final BaseStatements baseStatements;

  public AbstractStatements(BaseStatements baseStatements) {
    this.baseStatements = baseStatements;
  }

  protected List<String> schemas(Optional<String> schemaPrefix) {
    return baseStatements.schemas(schemaPrefix);
  }

  protected List<String> tables(Optional<String> schemaPrefix) {
    return baseStatements.tables(schemaPrefix);
  }

  @Override
  public List<String> createMetadataSchemaStatements(Optional<String> schemaPrefix) {
    return Collections.singletonList(
        "CREATE SCHEMA " + TableMetadataManager.getSchema(schemaPrefix));
  }

  @Override
  public List<String> dropMetadataSchemaStatements(Optional<String> schemaPrefix) {
    return Collections.singletonList("DROP SCHEMA " + TableMetadataManager.getSchema(schemaPrefix));
  }

  @Override
  public List<String> createMetadataTableStatements(Optional<String> schemaPrefix) {
    return Collections.singletonList(
        "CREATE TABLE "
            + TableMetadataManager.getFullTableName(schemaPrefix)
            + "(full_table_name VARCHAR(128),"
            + "column_name VARCHAR(128),"
            + "data_type VARCHAR(20) NOT NULL,"
            + "key_type VARCHAR(20),"
            + "clustering_order VARCHAR(10),"
            + "indexed BOOLEAN,"
            + "ordinal_position INTEGER NOT NULL,"
            + "PRIMARY KEY (full_table_name, column_name))");
  }

  @Override
  public List<String> insertMetadataStatements(Optional<String> schemaPrefix) {
    return baseStatements.insertMetadataStatements(schemaPrefix);
  }

  @Override
  public List<String> dropMetadataTableStatements(Optional<String> schemaPrefix) {
    return Collections.singletonList(
        "DROP TABLE " + TableMetadataManager.getFullTableName(schemaPrefix));
  }

  @Override
  public List<String> createSchemaStatements(Optional<String> schemaPrefix) {
    return schemas(schemaPrefix).stream()
        .map(s -> "CREATE SCHEMA " + s)
        .collect(Collectors.toList());
  }

  @Override
  public List<String> dropSchemaStatements(Optional<String> schemaPrefix) {
    return schemas(schemaPrefix).stream().map(s -> "DROP SCHEMA " + s).collect(Collectors.toList());
  }

  @Override
  public List<String> createTableStatements(Optional<String> schemaPrefix) {
    return baseStatements.createTableStatements(schemaPrefix);
  }

  @Override
  public List<String> dropTableStatements(Optional<String> schemaPrefix) {
    return tables(schemaPrefix).stream().map(t -> "DROP TABLE " + t).collect(Collectors.toList());
  }
}
