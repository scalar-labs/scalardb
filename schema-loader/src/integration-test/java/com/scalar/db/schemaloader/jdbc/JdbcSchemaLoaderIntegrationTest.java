package com.scalar.db.schemaloader.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.storage.jdbc.JdbcEnv;

public class JdbcSchemaLoaderIntegrationTest extends SchemaLoaderIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return JdbcEnv.getJdbcConfig();
  }
}
