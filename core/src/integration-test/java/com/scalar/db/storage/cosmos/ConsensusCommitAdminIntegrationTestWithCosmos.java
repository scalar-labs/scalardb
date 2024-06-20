package com.scalar.db.storage.cosmos;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Map;
import java.util.Properties;

public class ConsensusCommitAdminIntegrationTestWithCosmos
    extends ConsensusCommitAdminIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return CosmosEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return CosmosEnv.getCreationOptions();
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new CosmosAdminTestUtils(getProperties(testName));
  }
}
