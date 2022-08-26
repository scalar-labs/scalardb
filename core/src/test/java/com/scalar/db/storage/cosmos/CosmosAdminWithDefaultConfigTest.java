package com.scalar.db.storage.cosmos;

import java.util.Optional;

public class CosmosAdminWithDefaultConfigTest extends CosmosAdminTestBase {
  @Override
  Optional<String> getTableMetadataDatabaseConfig() {
    return Optional.empty();
  }
}
