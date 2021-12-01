package com.scalar.db.storage.cassandra;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.StorageMultipleClusteringKeyScanIntegrationTestBase;

public class CassandraMultipleClusteringKeyScanIntegrationTest
    extends StorageMultipleClusteringKeyScanIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CassandraEnv.getDatabaseConfig();
  }
}
