package com.scalar.db.storage.cosmos;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.StorageSecondaryIndexIntegrationTestBase;
import java.util.Map;
import java.util.Optional;

public class CosmosSecondaryIndexIntegrationTest extends StorageSecondaryIndexIntegrationTestBase {
  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CosmosEnv.getCosmosConfig();
  }

  @Override
  protected String getNamespace() {
    Optional<String> databasePrefix = CosmosEnv.getDatabasePrefix();
    return databasePrefix.map(prefix -> prefix + NAMESPACE).orElse(NAMESPACE);
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return CosmosEnv.getCreateOptions();
  }
}
