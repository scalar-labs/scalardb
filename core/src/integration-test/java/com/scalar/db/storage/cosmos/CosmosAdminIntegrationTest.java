package com.scalar.db.storage.cosmos;

import com.scalar.db.api.DistributedStorageAdminIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import java.util.Map;
import java.util.Properties;

public class CosmosAdminIntegrationTest extends DistributedStorageAdminIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return CosmosEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return CosmosEnv.getCreationOptions();
  }

  @Override
  protected String getSystemNamespaceName(Properties properties) {
    return new CosmosConfig(new DatabaseConfig(properties))
        .getTableMetadataDatabase()
        .orElse(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
  }
}
