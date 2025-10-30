package com.scalar.db.storage.cosmos;

import com.scalar.db.api.DistributedStorageAdminCaseSensitivityIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.util.AdminTestUtils;
import java.util.Map;
import java.util.Properties;

public class CosmosAdminCaseSensitivityIntegrationTest
    extends DistributedStorageAdminCaseSensitivityIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return CosmosEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return CosmosEnv.getCreationOptions();
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new CosmosAdminTestUtils(getProperties(testName));
  }

  @Override
  protected String getSystemNamespaceName(Properties properties) {
    return new CosmosConfig(new DatabaseConfig(properties))
        .getTableMetadataDatabase()
        .orElse(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
  }
}
