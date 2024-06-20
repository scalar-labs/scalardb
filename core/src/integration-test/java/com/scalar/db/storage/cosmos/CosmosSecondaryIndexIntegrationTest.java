package com.scalar.db.storage.cosmos;

import com.scalar.db.api.DistributedStorageSecondaryIndexIntegrationTestBase;
import java.util.Map;
import java.util.Properties;

public class CosmosSecondaryIndexIntegrationTest
    extends DistributedStorageSecondaryIndexIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return CosmosEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return CosmosEnv.getCreationOptions();
  }
}
