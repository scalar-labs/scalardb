package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageMutationAtomicityUnitIntegrationTestBase;
import java.util.Properties;

public class JdbcDatabaseMutationAtomicityUnitIntegrationTest
    extends DistributedStorageMutationAtomicityUnitIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return JdbcEnv.getProperties(testName);
  }
}
