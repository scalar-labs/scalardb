package com.scalar.db.storage.dynamo;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.StorageIntegrationTestBase;
import java.util.Map;
import jdk.nashorn.internal.ir.annotations.Ignore;

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
  @Ignore
  @Override
  public void put_PutGivenForIndexedColumnWithNullValue_ShouldPut() {}
}
