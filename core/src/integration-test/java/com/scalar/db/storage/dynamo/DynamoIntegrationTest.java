package com.scalar.db.storage.dynamo;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.StorageIntegrationTestBase;
import java.util.Map;
import org.junit.Ignore;

public class DynamoIntegrationTest extends StorageIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return DynamoEnv.getDynamoConfig();
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return DynamoEnv.getCreateOptions();
  }

  // Ignore this test for now since the DynamoDB adapter doesn't support scan with exclusive range
  @Ignore
  @Override
  public void scan_ScanWithEndInclusiveRangeGiven_ShouldRetrieveResultsOfGivenRange() {}

  // Ignore this test for now since the DynamoDB adapter doesn't support scan with exclusive range
  @Ignore
  @Override
  public void scan_ScanWithStartInclusiveRangeGiven_ShouldRetrieveResultsOfGivenRange() {}
}
