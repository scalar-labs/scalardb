package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStorageMultipleClusteringKeyScanIntegrationTestBase;
import com.scalar.db.io.DataType;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class CassandraMultipleClusteringKeyScanIntegrationTest
    extends DistributedStorageMultipleClusteringKeyScanIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return CassandraEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return Collections.singletonMap(CassandraAdmin.REPLICATION_FACTOR, "1");
  }

  @Override
  protected List<DataType> getDataTypes() {
    return super.getDataTypes().stream()
        .filter(type -> type != DataType.TIMESTAMP)
        .collect(Collectors.toList());
  }
}
