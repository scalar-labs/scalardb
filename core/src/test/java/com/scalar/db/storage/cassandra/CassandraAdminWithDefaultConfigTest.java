package com.scalar.db.storage.cassandra;

import java.util.Optional;

public class CassandraAdminWithDefaultConfigTest extends CassandraAdminTestBase {

  @Override
  Optional<String> getMetadataKeyspaceConfig() {
    return Optional.empty();
  }
}
