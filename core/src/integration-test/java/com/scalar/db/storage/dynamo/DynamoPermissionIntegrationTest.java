package com.scalar.db.storage.dynamo;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedStoragePermissionIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import com.scalar.db.util.PermissionTestUtils;
import java.util.Map;
import java.util.Properties;

public class DynamoPermissionIntegrationTest
    extends DistributedStoragePermissionIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return DynamoEnv.getProperties(testName);
  }

  @Override
  protected Properties getPropertiesForNormalUser(String testName) {
    return DynamoEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return ImmutableMap.of(DynamoAdmin.NO_SCALING, "false", DynamoAdmin.NO_BACKUP, "false");
  }

  @Override
  protected PermissionTestUtils getPermissionTestUtils(String testName) {
    return new DynamoPermissionTestUtils(getProperties(testName));
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new DynamoAdminTestUtils(getProperties(testName));
  }
}
