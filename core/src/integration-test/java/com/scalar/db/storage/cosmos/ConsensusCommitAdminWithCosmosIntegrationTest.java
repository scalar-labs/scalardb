package com.scalar.db.storage.cosmos;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminIntegrationTestBase;
import java.util.Map;
import java.util.Optional;

public class ConsensusCommitAdminWithCosmosIntegrationTest
    extends ConsensusCommitAdminIntegrationTestBase {

  @Override
  protected DatabaseConfig getDbConfig() {
    return CosmosEnv.getCosmosConfig();
  }

  @Override
  protected String getNamespace1() {
    return getNamespace(super.getNamespace1());
  }

  @Override
  protected String getNamespace2() {
    return getNamespace(super.getNamespace2());
  }

  @Override
  protected String getNamespace3() {
    return getNamespace(super.getNamespace3());
  }

  private String getNamespace(String namespace) {
    Optional<String> databasePrefix = CosmosEnv.getDatabasePrefix();
    return databasePrefix.map(prefix -> prefix + namespace).orElse(namespace);
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return CosmosEnv.getCreateOptions();
  }
}
