package com.scalar.db.storage.jdbc.test;

import com.scalar.db.storage.jdbc.metadata.TableMetadataManager;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class OracleStatements extends AbstractStatements {
  public OracleStatements(BaseStatements baseStatements) {
    super(baseStatements);
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
  public List<String> createMetadataSchemaStatements(Optional<String> namespacePrefix) {
    return createSchema(TableMetadataManager.getFullSchema(namespacePrefix));
  }

  @Override
  public List<String> insertMetadataStatements(Optional<String> namespacePrefix) {
    return super.insertMetadataStatements(namespacePrefix).stream()
        .map(s -> s.replace("true", "1"))
        .map(s -> s.replace("false", "0"))
        .collect(Collectors.toList());
  }

  @Override
  public List<String> dropMetadataSchemaStatements(Optional<String> namespacePrefix) {
    return dropSchema(TableMetadataManager.getFullSchema(namespacePrefix));
  }

  @Override
  public List<String> createMetadataTableStatements(Optional<String> namespacePrefix) {
    return convertCreateTableStatements(super.createMetadataTableStatements(namespacePrefix));
  }

  @Override
  public List<String> createSchemaStatements(Optional<String> namespacePrefix) {
    return schemas(namespacePrefix).stream()
        .flatMap(s -> createSchema(s).stream())
        .collect(Collectors.toList());
  }

  @Override
  public List<String> dropSchemaStatements(Optional<String> namespacePrefix) {
    return schemas(namespacePrefix).stream()
        .flatMap(s -> dropSchema(s).stream())
        .collect(Collectors.toList());
  }

  @Override
  public List<String> createTableStatements(Optional<String> namespacePrefix) {
    return convertCreateTableStatements(super.createTableStatements(namespacePrefix));
  }

  private List<String> convertCreateTableStatements(List<String> statements) {
    return statements.stream()
        .map(s -> s.replace("BOOLEAN", "NUMBER(1)"))
        .map(s -> s.replace("BIGINT", "NUMBER(19)"))
        .collect(Collectors.toList());
  }
}
