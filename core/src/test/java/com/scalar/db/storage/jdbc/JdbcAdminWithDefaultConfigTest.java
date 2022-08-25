package com.scalar.db.storage.jdbc;

import java.util.Optional;

public class JdbcAdminWithDefaultConfigTest extends JdbcAdminTestBase {
  @Override
  Optional<String> getTableMetadataSchemaConfig() {
    return Optional.empty();
  }
}
