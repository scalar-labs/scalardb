package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageAtomicityUnitIntegrationTestBase;
import java.util.Properties;

public class JdbcDatabaseAtomicityUnitIntegrationTest
    extends DistributedStorageAtomicityUnitIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return JdbcEnv.getProperties(testName);
  }
}
