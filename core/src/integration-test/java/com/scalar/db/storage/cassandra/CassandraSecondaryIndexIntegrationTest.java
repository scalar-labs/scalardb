package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStorageSecondaryIndexIntegrationTestBase;
import com.scalar.db.io.DataType;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class CassandraSecondaryIndexIntegrationTest
    extends DistributedStorageSecondaryIndexIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return CassandraEnv.getProperties(testName);
  }

  @Override
  protected Set<DataType> getSecondaryIndexTypes() {
    return super.getSecondaryIndexTypes().stream()
        .filter(type -> type != DataType.TIMESTAMP)
        .collect(Collectors.toSet());
  }
}
