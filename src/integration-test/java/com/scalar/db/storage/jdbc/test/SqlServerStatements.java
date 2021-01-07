package com.scalar.db.storage.jdbc.test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class SqlServerStatements extends AbstractStatements {
  public SqlServerStatements(BaseStatements baseStatements) {
    super(baseStatements);
  }

  @Override
  public List<String> createMetadataTableStatements(Optional<String> namespacePrefix) {
    return super.createMetadataTableStatements(namespacePrefix).stream()
        .map(s -> s.replace("BOOLEAN", "BIT"))
        .collect(Collectors.toList());
  }

  @Override
  public List<String> insertMetadataStatements(Optional<String> namespacePrefix) {
    return super.insertMetadataStatements(namespacePrefix).stream()
        .map(s -> s.replace("true", "1"))
        .map(s -> s.replace("false", "0"))
        .collect(Collectors.toList());
  }

  @Override
  public List<String> createTableStatements(Optional<String> namespacePrefix) {
    return super.createTableStatements(namespacePrefix).stream()
        .map(s -> s.replace("BOOLEAN", "BIT"))
        .collect(Collectors.toList());
  }
}
