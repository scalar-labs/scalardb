package com.scalar.db.storage.dynamo;

import com.scalar.db.api.DistributedStorageJapaneseIntegrationTestBase;
import java.util.Map;
import java.util.Properties;

public class DynamoJapaneseIntegrationTest extends DistributedStorageJapaneseIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return DynamoEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
  }
}
