package com.scalar.db.storage.cassandra;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.StorageMultiplePartitionKeyIntegrationTestBase;

public class CassandraMultiplePartitionKeyIntegrationTest
    extends StorageMultiplePartitionKeyIntegrationTestBase {
  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CassandraEnv.getDatabaseConfig();
  }
}
