package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStorageMultiplePartitionKeyIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;

public class CassandraMultiplePartitionKeyIntegrationTest
    extends DistributedStorageMultiplePartitionKeyIntegrationTestBase {
  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CassandraEnv.getDatabaseConfig();
  }
}
