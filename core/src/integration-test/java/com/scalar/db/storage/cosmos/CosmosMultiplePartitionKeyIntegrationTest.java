package com.scalar.db.storage.cosmos;

import com.scalar.db.api.DistributedStorageMultiplePartitionKeyIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import java.util.Map;
import java.util.Optional;

public class CosmosMultiplePartitionKeyIntegrationTest
    extends DistributedStorageMultiplePartitionKeyIntegrationTestBase {
  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CosmosEnv.getCosmosConfig();
  }

  @Override
  protected String getNamespaceBaseName() {
    String namespaceBaseName = super.getNamespaceBaseName();
    Optional<String> databasePrefix = CosmosEnv.getDatabasePrefix();
    return databasePrefix.map(prefix -> prefix + namespaceBaseName).orElse(namespaceBaseName);
  }

  @Override
  protected int getThreadNum() {
    return 3;
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return CosmosEnv.getCreateOptions();
  }
}
