package com.scalar.db.storage.dynamo;

import com.scalar.db.api.DistributedStorageCrossPartitionScanIntegrationTestBase;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class DynamoCrossPartitionScanIntegrationTest
    extends DistributedStorageCrossPartitionScanIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return DynamoEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
  }

  @Test
  @Override
  @Disabled("Cross partition scan with ordering is not supported in DynamoDB")
  public void scan_WithOrderingForNonPrimaryColumns_ShouldReturnProperResult() {}
}
