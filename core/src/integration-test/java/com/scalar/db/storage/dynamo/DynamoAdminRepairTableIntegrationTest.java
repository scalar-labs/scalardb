package com.scalar.db.storage.dynamo;

import com.scalar.db.api.DistributedStorageAdminRepairTableIntegrationTestBase;
import java.util.Map;
import java.util.Properties;

public class DynamoAdminRepairTableIntegrationTest
    extends DistributedStorageAdminRepairTableIntegrationTestBase {
  @Override
  protected Properties getProperties() {
    return DynamoEnv.getProperties();
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return DynamoEnv.getCreateOptions();
  }
}
