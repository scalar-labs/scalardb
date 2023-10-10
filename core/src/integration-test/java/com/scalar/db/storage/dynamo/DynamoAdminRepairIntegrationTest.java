package com.scalar.db.storage.dynamo;

import com.scalar.db.api.DistributedStorageAdminRepairIntegrationTestBase;
import java.util.Map;
import java.util.Properties;

public class DynamoAdminRepairIntegrationTest
    extends DistributedStorageAdminRepairIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return DynamoEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
  }

  @Override
  protected void initialize(String testName) throws Exception {
    super.initialize(testName);
    adminTestUtils = new DynamoAdminTestUtils(getProperties(testName));
  }
}
