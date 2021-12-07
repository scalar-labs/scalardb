package com.scalar.db.storage.cassandra;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.StorageSingleClusteringKeyScanIntegrationTestBase;

public class CassandraSingleClusteringKeyScanIntegrationTest
    extends StorageSingleClusteringKeyScanIntegrationTestBase {
  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CassandraEnv.getDatabaseConfig();
  }
}
