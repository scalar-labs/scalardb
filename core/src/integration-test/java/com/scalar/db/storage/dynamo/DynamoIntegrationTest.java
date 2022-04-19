package com.scalar.db.storage.dynamo;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.StorageIntegrationTestBase;
import java.util.Map;
import org.junit.jupiter.api.Disabled;

public class DynamoIntegrationTest extends StorageIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return DynamoEnv.getDynamoConfig();
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return DynamoEnv.getCreateOptions();
  }

  // DynamoDB doesn't support putting a null value for a secondary index column
  @Disabled
  @Override
  public void put_PutGivenForIndexedColumnWithNullValue_ShouldPut() {}
}
