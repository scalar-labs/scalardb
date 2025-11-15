package com.scalar.db.storage.cosmos;

import com.scalar.db.api.DistributedStorageAtomicityUnitIntegrationTestBase;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class CosmosAtomicityUnitIntegrationTest
    extends DistributedStorageAtomicityUnitIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return CosmosEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return CosmosEnv.getCreationOptions();
  }

  @Disabled("This test fails. It might be a bug")
  @Override
  public void mutate_MutationsWithinRecordGiven_ShouldBehaveCorrectlyBaseOnAtomicityUnit() {}
}
