package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;

public class ConsensusCommitAdminIntegrationTestWithJdbcDatabase
    extends ConsensusCommitAdminIntegrationTestBase {
  private RdbEngineStrategy rdbEngine;

  @Override
  protected Properties getProps(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    rdbEngine = RdbEngineFactory.create(new JdbcConfig(new DatabaseConfig(properties)));
    return properties;
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new JdbcAdminTestUtils(getProperties(testName));
  }

  @Override
  protected boolean isCreateIndexOnTextColumnEnabled() {
    // "admin.createIndex()" for TEXT column fails (the "create index" query runs
    // indefinitely) on the Db2 community edition docker version which we use for the CI.
    // However, the index creation is successful on Db2 hosted on IBM Cloud.
    // So we disable these tests until the issue with the Db2 community edition is resolved.
    return !JdbcTestUtils.isDb2(rdbEngine);
  }

  @Override
  protected boolean isIndexOnBlobColumnSupported() {
    return !JdbcTestUtils.isDb2(rdbEngine);
  }
}
