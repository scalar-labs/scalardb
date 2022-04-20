package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStorageSecondaryIndexIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;

public class CassandraSecondaryIndexIntegrationTest
    extends DistributedStorageSecondaryIndexIntegrationTestBase {
  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CassandraEnv.getDatabaseConfig();
  }
}
