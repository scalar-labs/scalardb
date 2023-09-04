package com.scalar.db.storage.cassandra;

import java.util.Optional;

public class CassandraAdminWithMetadataKeyspaceConfigTest extends CassandraAdminTestBase {

  @Override
  Optional<String> getMetadataKeyspaceConfig() {
    return Optional.of("my_ks");
  }
}
