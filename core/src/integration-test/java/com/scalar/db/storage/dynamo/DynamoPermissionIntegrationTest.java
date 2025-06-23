package com.scalar.db.storage.dynamo;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedStoragePermissionIntegrationTestBase;
import java.util.Map;
import java.util.Properties;

public class DynamoPermissionIntegrationTest
    extends DistributedStoragePermissionIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return DynamoEnv.getPropertiesForNonEmulator(testName);
  }

  @Override
  protected Properties getPropertiesForNormalUser(String testName) {
    return DynamoEnv.getPropertiesForNonEmulator(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return ImmutableMap.of(DynamoAdmin.NO_SCALING, "false", DynamoAdmin.NO_BACKUP, "false");
  }
}
