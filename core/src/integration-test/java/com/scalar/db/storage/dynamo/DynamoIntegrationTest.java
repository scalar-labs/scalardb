package com.scalar.db.storage.dynamo;

import com.scalar.db.api.DistributedStorageIntegrationTestBase;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class DynamoIntegrationTest extends DistributedStorageIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return DynamoEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
  }

  @Disabled("DynamoDB doesn't support putting a null value for a secondary index column")
  @Override
  public void put_PutGivenForIndexedColumnWithNullValue_ShouldPut() {}
}
