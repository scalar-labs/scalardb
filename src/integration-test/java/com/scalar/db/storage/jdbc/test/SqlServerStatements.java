package com.scalar.db.storage.jdbc.test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class SqlServerStatements extends AbstractStatements {
  public SqlServerStatements(BaseStatements baseStatements) {
    super(baseStatements);
  }

  @Override
  public List<String> createMetadataTableStatements(Optional<String> schemaPrefix) {
    return super.createMetadataTableStatements(schemaPrefix).stream()
        .map(s -> s.replace("BOOLEAN", "BIT"))
        .collect(Collectors.toList());
  }

  @Override
  public List<String> insertMetadataStatements(Optional<String> schemaPrefix) {
    return super.insertMetadataStatements(schemaPrefix).stream()
        .map(s -> s.replace("true", "1"))
        .map(s -> s.replace("false", "0"))
        .collect(Collectors.toList());
  }

  @Override
  public List<String> createTableStatements(Optional<String> schemaPrefix) {
    return super.createTableStatements(schemaPrefix).stream()
        .map(s -> s.replace("BOOLEAN", "BIT"))
        .collect(Collectors.toList());
  }
}
