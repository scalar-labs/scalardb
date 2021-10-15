package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.StorageMultipleClusteringKeysIntegrationTestBase;

public class JdbcMultipleClusteringKeysIntegrationTest
    extends StorageMultipleClusteringKeysIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return JdbcEnv.getJdbcConfig();
  }
}
