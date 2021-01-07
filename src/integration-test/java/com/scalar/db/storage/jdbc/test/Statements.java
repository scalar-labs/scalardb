package com.scalar.db.storage.jdbc.test;

import java.util.List;
import java.util.Optional;

public interface Statements {
  List<String> createMetadataSchemaStatements(Optional<String> namespacePrefix);

  List<String> dropMetadataSchemaStatements(Optional<String> namespacePrefix);

  List<String> createMetadataTableStatements(Optional<String> namespacePrefix);

  List<String> insertMetadataStatements(Optional<String> namespacePrefix);

  List<String> dropMetadataTableStatements(Optional<String> namespacePrefix);

  List<String> createSchemaStatements(Optional<String> namespacePrefix);

  List<String> dropSchemaStatements(Optional<String> namespacePrefix);

  List<String> createTableStatements(Optional<String> namespacePrefix);

  List<String> dropTableStatements(Optional<String> namespacePrefix);
}
