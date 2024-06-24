package com.scalar.db.storage.dynamo;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminRepairIntegrationTestBase;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class ConsensusCommitAdminRepairIntegrationTestWithDynamo
    extends ConsensusCommitAdminRepairIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return DynamoEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
  }

  @Override
  protected void initialize(String testName) throws Exception {
    super.initialize(testName);
    adminTestUtils = new DynamoAdminTestUtils(getProperties(testName));
  }

  @Disabled(
      "Inconsistency check for the raw table schema and the ScalarDB metadata isn't executed in schemaless database/storage")
  @Override
  public void repairCoordinatorTables_OldSchemaCoordinatorTablesExist_ShouldFail() {}
}
