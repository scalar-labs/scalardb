package com.scalar.db.storage.dynamo;

import com.scalar.db.api.DistributedStorageAdminIntegrationTestBase;
import java.util.Map;
import java.util.Properties;

public class DynamoAdminIntegrationTest extends DistributedStorageAdminIntegrationTestBase {

  @Override
  protected Properties getProperties() {
    return DynamoEnv.getProperties();
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
  }

  @Override
  protected boolean isIndexOnBooleanColumnSupported() {
    return false;
  }
}
