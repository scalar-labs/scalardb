package com.scalar.db.storage.cosmos;

import com.scalar.db.api.DistributedStorageAdminIntegrationTestBase;
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
}
