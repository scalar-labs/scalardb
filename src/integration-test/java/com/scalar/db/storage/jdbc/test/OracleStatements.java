package com.scalar.db.storage.jdbc.test;

import com.scalar.db.storage.jdbc.metadata.TableMetadataManager;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class OracleStatements extends AbstractStatements {
  public OracleStatements(StatementsStrategy strategy) {
    super(strategy);
  }

  private List<String> createSchema(String schema) {
    return Arrays.asList(
        "CREATE USER " + schema + " IDENTIFIED BY \"oracle\"",
        "ALTER USER " + schema + " quota unlimited on USERS");
  }

  private List<String> dropSchema(String schema) {
    return Collections.singletonList("DROP USER " + schema + " CASCADE");
  }

  @Override
  public List<String> createMetadataSchemaStatements(String schemaPrefix) {
    return createSchema(TableMetadataManager.getSchema(schemaPrefix));
  }

  @Override
  public List<String> insertMetadataStatements(String schemaPrefix) {
    return super.insertMetadataStatements(schemaPrefix).stream()
        .map(s -> s.replace("true", "1"))
        .map(s -> s.replace("false", "0"))
        .collect(Collectors.toList());
  }

  @Override
  public List<String> dropMetadataSchemaStatements(String schemaPrefix) {
    return dropSchema(TableMetadataManager.getSchema(schemaPrefix));
  }

  @Override
  public List<String> createMetadataTableStatements(String schemaPrefix) {
    return convertCreateTableStatements(super.createMetadataTableStatements(schemaPrefix));
  }

  @Override
  public List<String> createDataSchemaStatements(String schemaPrefix) {
    return dataSchemas(schemaPrefix).stream()
        .flatMap(s -> createSchema(s).stream())
        .collect(Collectors.toList());
  }

  @Override
  public List<String> dropDataSchemaStatements(String schemaPrefix) {
    return dataSchemas(schemaPrefix).stream()
        .flatMap(s -> dropSchema(s).stream())
        .collect(Collectors.toList());
  }

  @Override
  public List<String> createDataTableStatements(String schemaPrefix) {
    return convertCreateTableStatements(super.createDataTableStatements(schemaPrefix));
  }

  private List<String> convertCreateTableStatements(List<String> statements) {
    return statements.stream()
        .map(s -> s.replace("BOOLEAN", "NUMBER(1)"))
        .map(s -> s.replace("BIGINT", "NUMBER(19)"))
        .collect(Collectors.toList());
  }
}
