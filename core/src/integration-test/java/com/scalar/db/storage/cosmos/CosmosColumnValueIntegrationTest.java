package com.scalar.db.storage.cosmos;

import com.scalar.db.api.DistributedStorageColumnValueIntegrationTestBase;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class CosmosColumnValueIntegrationTest
    extends DistributedStorageColumnValueIntegrationTestBase {
  @Override
  protected Properties getProperties() {
    return CosmosEnv.getProperties();
  }

  @Override
  protected String getNamespace() {
    String namespace = super.getNamespace();
    Optional<String> databasePrefix = CosmosEnv.getDatabasePrefix();
    return databasePrefix.map(prefix -> prefix + namespace).orElse(namespace);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return CosmosEnv.getCreationOptions();
  }
}
