package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageAdminRepairTableIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;

public class JdbcAdminRepairTableIntegrationTest
    extends DistributedStorageAdminRepairTableIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return JdbcEnv.getProperties(testName);
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new JdbcAdminTestUtils(getProperties(testName));
  }
}
