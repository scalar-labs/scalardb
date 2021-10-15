package com.scalar.db.storage.cassandra;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.StorageMultipleClusteringKeysIntegrationTestBase;

public class CassandraMultipleClusteringKeysIntegrationTest
    extends StorageMultipleClusteringKeysIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CassandraEnv.getDatabaseConfig();
  }
}
