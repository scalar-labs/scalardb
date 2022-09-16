package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStorageSecondaryIndexIntegrationTestBase;
import java.util.Properties;

public class CassandraSecondaryIndexIntegrationTest
    extends DistributedStorageSecondaryIndexIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return CassandraEnv.getProperties(testName);
  }
}
