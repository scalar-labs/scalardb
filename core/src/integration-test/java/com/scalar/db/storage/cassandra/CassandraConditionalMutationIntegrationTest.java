package com.scalar.db.storage.cassandra;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.StorageConditionalMutationIntegrationTestBase;

public class CassandraConditionalMutationIntegrationTest
    extends StorageConditionalMutationIntegrationTestBase {
  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CassandraEnv.getDatabaseConfig();
  }
}
