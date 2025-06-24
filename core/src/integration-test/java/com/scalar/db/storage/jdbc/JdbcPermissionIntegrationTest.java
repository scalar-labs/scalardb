package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStoragePermissionIntegrationTestBase;
import com.scalar.db.util.PermissionTestUtils;
import java.util.Properties;

public class JdbcPermissionIntegrationTest extends DistributedStoragePermissionIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return JdbcEnv.getProperties(testName);
  }

  @Override
  protected Properties getPropertiesForNormalUser(String testName) {
    return JdbcEnv.getPropertiesForNormalUser(testName);
  }

  @Override
  protected PermissionTestUtils getPermissionTestUtils(String testName) {
    return new JdbcPermissionTestUtils(getProperties(testName));
  }
}
