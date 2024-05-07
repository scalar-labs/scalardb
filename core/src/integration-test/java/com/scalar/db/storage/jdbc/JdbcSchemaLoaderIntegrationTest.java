package com.scalar.db.storage.jdbc;

import com.google.common.util.concurrent.Uninterruptibles;
import com.google.errorprone.annotations.concurrent.LazyInit;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class JdbcSchemaLoaderIntegrationTest extends SchemaLoaderIntegrationTestBase {
  @LazyInit private RdbEngineStrategy rdbEngine;

  @Override
  protected Properties getProperties(String testName) {
    return JdbcEnv.getProperties(testName);
  }

  @Override
  protected void initialize(String testName) throws Exception {
    super.initialize(testName);
    Properties properties = getProperties(testName);
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new JdbcAdminTestUtils(getProperties(testName));
  }

  @Override
  protected void waitForCreationIfNecessary() {
    if (rdbEngine instanceof RdbEngineYugabyte) {
      Uninterruptibles.sleepUninterruptibly(2000, TimeUnit.MILLISECONDS);
      return;
    }
    super.waitForCreationIfNecessary();
  }
}
