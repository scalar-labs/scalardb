package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageAdminIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;

public class JdbcAdminIntegrationTest extends DistributedStorageAdminIntegrationTestBase {
  private RdbEngineStrategy rdbEngine;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    rdbEngine = RdbEngineFactory.create(new JdbcConfig(new DatabaseConfig(properties)));
    return properties;
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new JdbcAdminTestUtils(getProperties(testName));
  }

  @Override
  protected boolean isCreateIndexOnTextAndBlobColumnsEnabled() {
    // "admin.createIndex()" for TEXT and BLOB columns fails (the "create index" query runs
    // indefinitely) on Db2 community edition version but works on Db2 hosted on IBM Cloud.
    // So we disable these tests until the issue is resolved.
    return !JdbcTestUtils.isDb2(rdbEngine);
  }
}
