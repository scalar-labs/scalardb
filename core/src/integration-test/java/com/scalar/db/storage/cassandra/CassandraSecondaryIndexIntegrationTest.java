package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStorageSecondaryIndexIntegrationTestBase;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class CassandraSecondaryIndexIntegrationTest
    extends DistributedStorageSecondaryIndexIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return CassandraEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return Collections.singletonMap(CassandraAdmin.REPLICATION_FACTOR, "1");
  }

  @Override
  protected boolean isTimestampTypeSupported() {
    return false;
  }
}
