package com.scalar.db.storage.jdbc;

import com.google.common.util.concurrent.Uninterruptibles;
import com.google.errorprone.annotations.concurrent.LazyInit;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminRepairTableIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ConsensusCommitAdminRepairTableIntegrationTestWithJdbcDatabase
    extends ConsensusCommitAdminRepairTableIntegrationTestBase {
  @LazyInit private RdbEngineStrategy rdbEngine;

  @Override
  protected Properties getProps(String testName) {
    return JdbcEnv.getProperties(testName);
  }

  @Override
  protected void initialize(String testName) throws Exception {
    super.initialize(testName);
    Properties properties = getProperties(testName);
    rdbEngine = RdbEngineFactory.create(new JdbcConfig(new DatabaseConfig(properties)));
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new JdbcAdminTestUtils(getProperties(testName));
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
