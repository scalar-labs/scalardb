package com.scalar.db.storage.dynamo;

import com.scalar.db.api.DistributedStorageAtomicityUnitIntegrationTestBase;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class DynamoAtomicityUnitIntegrationTest
    extends DistributedStorageAtomicityUnitIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return DynamoEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
  }

  @Disabled("Transaction request cannot include multiple operations on one item in DynamoDB")
  @Override
  public void mutate_MutationsWithinRecordGiven_ShouldBehaveCorrectlyBaseOnAtomicityUnit() {}
}
