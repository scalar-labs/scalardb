package com.scalar.db.storage.cosmos;

import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTestUtils;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;

public class CosmosSchemaLoaderIntegrationTest extends SchemaLoaderIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = CosmosEnv.getProperties(testName);

    // Add testName as a coordinator schema suffix
    ConsensusCommitIntegrationTestUtils.addSuffixToCoordinatorNamespace(properties, testName);

    return properties;
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new CosmosAdminTestUtils(getProperties(testName));
  }
}
