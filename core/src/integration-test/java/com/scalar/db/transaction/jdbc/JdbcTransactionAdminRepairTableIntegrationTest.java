package com.scalar.db.transaction.jdbc;

import com.google.common.util.concurrent.Uninterruptibles;
import com.google.errorprone.annotations.concurrent.LazyInit;
import com.scalar.db.api.DistributedTransactionAdminRepairTableIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcAdminTestUtils;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.jdbc.JdbcEnv;
import com.scalar.db.storage.jdbc.JdbcTestUtils;
import com.scalar.db.storage.jdbc.RdbEngineFactory;
import com.scalar.db.storage.jdbc.RdbEngineStrategy;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class JdbcTransactionAdminRepairTableIntegrationTest
    extends DistributedTransactionAdminRepairTableIntegrationTestBase {
  @LazyInit private RdbEngineStrategy rdbEngine;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = new Properties();
    properties.putAll(JdbcEnv.getProperties(testName));
    properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "jdbc");
    return properties;
  }

  @Override
  protected void initialize(String testName) throws Exception {
    super.initialize(testName);
    Properties properties = getProperties(testName);
    adminTestUtils = new JdbcAdminTestUtils(properties);
    rdbEngine = RdbEngineFactory.create(new JdbcConfig(new DatabaseConfig(properties)));
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new JdbcAdminTestUtils(JdbcEnv.getProperties(testName));
  }

  @Override
  protected boolean hasCoordinatorTables() {
    return false;
  }

  @Override
  protected void waitForDifferentSessionDdl() {
    if (JdbcTestUtils.isYugabyte(rdbEngine)) {
      // This is needed to avoid schema or catalog version mismatch database errors.
      Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
      return;
    }
    super.waitForDifferentSessionDdl();
  }
}
