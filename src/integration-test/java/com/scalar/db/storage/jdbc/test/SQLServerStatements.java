package com.scalar.db.storage.jdbc.test;

import java.util.List;
import java.util.stream.Collectors;

public class SQLServerStatements extends AbstractStatements {
  public SQLServerStatements(StatementsStrategy strategy) {
    super(strategy);
  }

  @Override
  public List<String> createMetadataTableStatements(String schemaPrefix) {
    return super.createMetadataTableStatements(schemaPrefix).stream()
        .map(s -> s.replace("BOOLEAN", "BIT"))
        .collect(Collectors.toList());
  }

  @Override
  public List<String> insertMetadataStatements(String schemaPrefix) {
    return super.insertMetadataStatements(schemaPrefix).stream()
        .map(s -> s.replace("true", "1"))
        .map(s -> s.replace("false", "0"))
        .collect(Collectors.toList());
  }

  @Override
  public List<String> createDataTableStatements(String schemaPrefix) {
    return super.createDataTableStatements(schemaPrefix).stream()
        .map(s -> s.replace("BOOLEAN", "BIT"))
        .collect(Collectors.toList());
  }
}
