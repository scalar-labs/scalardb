package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStorageConditionalMutationIntegrationTestBase;
import java.util.Properties;

public class CassandraConditionalMutationIntegrationTest
    extends DistributedStorageConditionalMutationIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return CassandraEnv.getProperties(testName);
  }

  @Override
  protected boolean isTimestampTypeSupported() {
    return false;
  }
}
