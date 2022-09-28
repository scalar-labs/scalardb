package com.scalar.db.storage.cosmos;

import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitIntegrationTestBase;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class TwoPhaseConsensusCommitIntegrationTestWithCosmos
    extends TwoPhaseConsensusCommitIntegrationTestBase {

  @Override
  protected Properties getProps1(String testName) {
    return CosmosEnv.getProperties(testName);
  }

  @Override
  protected String getNamespaceBaseName() {
    String namespaceBaseName = super.getNamespaceBaseName();
    Optional<String> databasePrefix = CosmosEnv.getDatabasePrefix();
    return databasePrefix.map(prefix -> prefix + namespaceBaseName).orElse(namespaceBaseName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return CosmosEnv.getCreationOptions();
  }
}
