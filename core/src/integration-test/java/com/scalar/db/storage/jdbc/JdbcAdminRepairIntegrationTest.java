package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageAdminRepairIntegrationTestBase;
import java.util.Properties;

public class JdbcAdminRepairIntegrationTest
    extends DistributedStorageAdminRepairIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return JdbcEnv.getProperties(testName);
  }

  @Override
  protected void initialize(String testName) throws Exception {
    super.initialize(testName);
    adminTestUtils = new JdbcAdminTestUtils(getProperties(testName));
  }
}
