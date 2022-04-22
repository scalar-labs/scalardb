package com.scalar.db.storage.cosmos;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTestBase;
import java.util.Map;
import java.util.Optional;

public class ConsensusCommitIntegrationTestWithCosmos extends ConsensusCommitIntegrationTestBase {

  @Override
  protected DatabaseConfig getDbConfig() {
    return CosmosEnv.getCosmosConfig();
  }

  @Override
  protected String getNamespaceBaseName() {
    String namespaceBaseName = super.getNamespaceBaseName();
    Optional<String> databasePrefix = CosmosEnv.getDatabasePrefix();
    return databasePrefix.map(prefix -> prefix + namespaceBaseName).orElse(namespaceBaseName);
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return CosmosEnv.getCreateOptions();
  }
}
