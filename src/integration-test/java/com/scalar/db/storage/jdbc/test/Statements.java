package com.scalar.db.storage.jdbc.test;

import java.util.List;

public interface Statements {
  List<String> createMetadataSchemaStatements(String schemaPrefix);

  List<String> dropMetadataSchemaStatements(String schemaPrefix);

  List<String> createMetadataTableStatements(String schemaPrefix);

  List<String> insertMetadataStatements(String schemaPrefix);

  List<String> dropMetadataTableStatements(String schemaPrefix);

  List<String> createDataSchemaStatements(String schemaPrefix);

  List<String> dropDataSchemaStatements(String schemaPrefix);

  List<String> createDataTableStatements(String schemaPrefix);

  List<String> dropDataTableStatements(String schemaPrefix);
}
