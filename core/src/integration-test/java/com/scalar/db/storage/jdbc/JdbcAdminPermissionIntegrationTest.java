package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageAdminPermissionIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import com.scalar.db.util.PermissionTestUtils;
import java.util.Properties;

public class JdbcAdminPermissionIntegrationTest
    extends DistributedStorageAdminPermissionIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return JdbcEnv.getProperties(testName);
  }

  @Override
  protected Properties getPropertiesForNormalUser(String testName) {
    return JdbcEnv.getPropertiesForNormalUser(testName);
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new JdbcAdminTestUtils(getProperties(testName));
  }

  @Override
  protected PermissionTestUtils getPermissionTestUtils(String testName) {
    return new JdbcPermissionTestUtils(getProperties(testName));
  }
}
