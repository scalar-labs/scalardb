package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStoragePermissionIntegrationTestBase;
import java.util.Properties;

public class JdbcPermissionIntegrationTest extends DistributedStoragePermissionIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return JdbcEnv.getProperties(testName);
  }

  @Override
  protected Properties getPropertiesForNormalUser(String testName) {
    return JdbcEnv.getProperties(testName);
  }
}
