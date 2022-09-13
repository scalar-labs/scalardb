package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStorageSinglePartitionKeyIntegrationTestBase;
import java.util.Properties;

public class CassandraSinglePartitionKeyIntegrationTest
    extends DistributedStorageSinglePartitionKeyIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return CassandraEnv.getProperties(testName);
  }
}
