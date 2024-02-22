package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageJapaneseIntegrationTestBase;
import java.util.Properties;

public class JdbcDatabaseJapaneseIntegrationTest
    extends DistributedStorageJapaneseIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return JdbcEnv.getProperties(testName);
  }
}
