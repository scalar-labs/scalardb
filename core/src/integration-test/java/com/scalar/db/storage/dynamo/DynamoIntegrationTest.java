package com.scalar.db.storage.dynamo;

import com.scalar.db.api.DistributedStorageIntegrationTestBase;
import java.util.Map;
import java.util.Properties;

public class DynamoIntegrationTest extends DistributedStorageIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return DynamoEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
  }
}
