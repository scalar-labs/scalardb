package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.AdminIntegrationTestBase;

public class JdbcAdminIntegrationTest extends AdminIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return JdbcEnv.getJdbcConfig();
  }
}
