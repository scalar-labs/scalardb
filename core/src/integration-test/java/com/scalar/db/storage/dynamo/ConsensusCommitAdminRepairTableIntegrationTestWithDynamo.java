package com.scalar.db.storage.dynamo;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminRepairTableIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Map;
import java.util.Properties;

public class ConsensusCommitAdminRepairTableIntegrationTestWithDynamo
    extends ConsensusCommitAdminRepairTableIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return ConsensusCommitDynamoEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new DynamoAdminTestUtils(getProperties(testName));
  }
}
