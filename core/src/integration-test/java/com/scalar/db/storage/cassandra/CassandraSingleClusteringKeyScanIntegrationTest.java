package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStorageSingleClusteringKeyScanIntegrationTestBase;
import com.scalar.db.io.DataType;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class CassandraSingleClusteringKeyScanIntegrationTest
    extends DistributedStorageSingleClusteringKeyScanIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return CassandraEnv.getProperties(testName);
  }

  @Override
  protected List<DataType> getClusteringKeyTypes() {
    return super.getClusteringKeyTypes().stream()
        .filter(type -> type != DataType.TIMESTAMP)
        .collect(Collectors.toList());
  }
}
