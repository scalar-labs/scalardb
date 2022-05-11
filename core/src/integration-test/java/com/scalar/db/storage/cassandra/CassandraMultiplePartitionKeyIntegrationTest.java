package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStorageMultiplePartitionKeyIntegrationTestBase;
import java.util.Properties;

public class CassandraMultiplePartitionKeyIntegrationTest
    extends DistributedStorageMultiplePartitionKeyIntegrationTestBase {
  @Override
  protected Properties getProperties() {
    return CassandraEnv.getProperties();
  }
}
