package com.scalar.db.storage.jdbc;

import java.util.Optional;

public class JdbcAdminWithTableMetadataSchemaConfigTest extends JdbcAdminTestBase {

  @Override
  Optional<String> getTableMetadataSchemaConfig() {
    return Optional.of("my_meta_ns");
  }
}
