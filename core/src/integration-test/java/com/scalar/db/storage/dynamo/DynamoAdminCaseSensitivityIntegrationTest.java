package com.scalar.db.storage.dynamo;

import com.scalar.db.api.DistributedStorageAdminCaseSensitivityIntegrationTest;
import com.scalar.db.util.AdminTestUtils;
import java.util.Map;
import java.util.Properties;

public class DynamoAdminCaseSensitivityIntegrationTest
    extends DistributedStorageAdminCaseSensitivityIntegrationTest {

  @Override
  protected Properties getProperties(String testName) {
    return DynamoEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
  }

  @Override
  protected boolean isIndexOnBooleanColumnSupported() {
    return false;
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new DynamoAdminTestUtils(getProperties(testName));
  }
}
