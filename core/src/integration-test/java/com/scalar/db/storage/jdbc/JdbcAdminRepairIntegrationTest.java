package com.scalar.db.storage.jdbc;

import com.google.common.util.concurrent.Uninterruptibles;
import com.google.errorprone.annotations.concurrent.LazyInit;
import com.scalar.db.api.DistributedStorageAdminRepairIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class JdbcAdminRepairIntegrationTest
    extends DistributedStorageAdminRepairIntegrationTestBase {
  @LazyInit private RdbEngineStrategy rdbEngine;

  @Override
  protected Properties getProperties(String testName) {
    return JdbcEnv.getProperties(testName);
  }

  @Override
  protected void initialize(String testName) throws Exception {
    super.initialize(testName);
    Properties properties = getProperties(testName);
    adminTestUtils = new JdbcAdminTestUtils(properties);
    rdbEngine = RdbEngineFactory.create(new JdbcConfig(new DatabaseConfig(properties)));
  }

  @Override
  protected void waitForDifferentSessionDdl() {
    if (rdbEngine instanceof RdbEngineYugabyte) {
      // This is needed to avoid schema or catalog version mismatch database errors.
      Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
    }
  }
}
