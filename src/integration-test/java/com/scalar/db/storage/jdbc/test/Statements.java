package com.scalar.db.storage.jdbc.test;

import java.util.List;
import java.util.Optional;

public interface Statements {
  List<String> createMetadataSchemaStatements(Optional<String> schemaPrefix);

  List<String> dropMetadataSchemaStatements(Optional<String> schemaPrefix);

  List<String> createMetadataTableStatements(Optional<String> schemaPrefix);

  List<String> insertMetadataStatements(Optional<String> schemaPrefix);

  List<String> dropMetadataTableStatements(Optional<String> schemaPrefix);

  List<String> createSchemaStatements(Optional<String> schemaPrefix);

  List<String> dropSchemaStatements(Optional<String> schemaPrefix);

  List<String> createTableStatements(Optional<String> schemaPrefix);

  List<String> dropTableStatements(Optional<String> schemaPrefix);
}
