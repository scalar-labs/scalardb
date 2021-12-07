package com.scalar.db.storage.cassandra;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.StorageSecondaryIndexIntegrationTestBase;

public class CassandraSecondaryIndexIntegrationTest
    extends StorageSecondaryIndexIntegrationTestBase {
  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CassandraEnv.getDatabaseConfig();
  }
}
