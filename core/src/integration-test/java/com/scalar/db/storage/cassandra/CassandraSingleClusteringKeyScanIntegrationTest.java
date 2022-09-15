package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStorageSingleClusteringKeyScanIntegrationTestBase;
import java.util.Properties;

public class CassandraSingleClusteringKeyScanIntegrationTest
    extends DistributedStorageSingleClusteringKeyScanIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return CassandraEnv.getProperties(testName);
  }
}
