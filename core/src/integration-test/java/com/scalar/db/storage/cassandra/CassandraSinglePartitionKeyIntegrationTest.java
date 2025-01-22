package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStorageSinglePartitionKeyIntegrationTestBase;
import com.scalar.db.io.DataType;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class CassandraSinglePartitionKeyIntegrationTest
    extends DistributedStorageSinglePartitionKeyIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return CassandraEnv.getProperties(testName);
  }

  @Override
  protected List<DataType> getPartitionKeyTypes() {
    return super.getPartitionKeyTypes().stream()
        .filter(type -> type != DataType.TIMESTAMP)
        .collect(Collectors.toList());
  }
}
