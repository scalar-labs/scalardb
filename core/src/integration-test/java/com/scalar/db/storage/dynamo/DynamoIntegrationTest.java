package com.scalar.db.storage.dynamo;

import com.scalar.db.api.DistributedStorageIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import java.util.Map;
import org.junit.jupiter.api.Disabled;

public class DynamoIntegrationTest extends DistributedStorageIntegrationTestBase {

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

  @Override
  @Disabled("ScanAll is not yet implemented for Dynamo DB")
  public void scan_ScanAllWithNoLimitGiven_ShouldRetrieveAllRecords() {}

  @Override
  @Disabled("ScanAll is not yet implemented for Dynamo DB")
  public void scan_ScanAllWithLimitGiven_ShouldRetrieveExpectedRecords() {}

  @Override
  @Disabled("ScanAll is not yet implemented for Dynamo DB")
  public void scan_ScanAllWithProjectionsGiven_ShouldRetrieveSpecifiedValues() {}

  @Override
  @Disabled("ScanAll is not yet implemented for Dynamo DB")
  public void scan_ScanAllWithLargeData_ShouldRetrieveExpectedValues() {}
}
