package com.scalar.db.storage.cassandra;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.StorageAdminIntegrationTestBase;

public class CassandraAdminIntegrationTest extends StorageAdminIntegrationTestBase {
  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CassandraEnv.getDatabaseConfig();
  }
}
