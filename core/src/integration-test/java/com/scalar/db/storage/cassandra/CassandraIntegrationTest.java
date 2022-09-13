package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStorageIntegrationTestBase;
import java.util.Properties;

public class CassandraIntegrationTest extends DistributedStorageIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return CassandraEnv.getProperties(testName);
  }
}
