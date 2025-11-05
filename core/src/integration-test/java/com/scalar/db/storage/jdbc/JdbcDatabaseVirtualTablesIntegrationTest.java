package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageVirtualTablesIntegrationTestBase;
import java.util.Properties;

public class JdbcDatabaseVirtualTablesIntegrationTest
    extends DistributedStorageVirtualTablesIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return JdbcEnv.getProperties(testName);
  }
}
