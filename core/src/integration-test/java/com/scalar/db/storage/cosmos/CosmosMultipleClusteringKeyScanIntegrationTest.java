package com.scalar.db.storage.cosmos;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.scalar.db.api.DistributedStorageMultipleClusteringKeyScanIntegrationTestBase;
import com.scalar.db.io.DataType;
import java.util.Map;
import java.util.Properties;

public class CosmosMultipleClusteringKeyScanIntegrationTest
    extends DistributedStorageMultipleClusteringKeyScanIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return CosmosEnv.getProperties(testName);
  }

  @Override
  protected ListMultimap<DataType, DataType> getClusteringKeyTypes() {
    // Return types without BLOB because blob is not supported for clustering key for now
    ListMultimap<DataType, DataType> clusteringKeyTypes = ArrayListMultimap.create();
    for (DataType firstClusteringKeyType : DataType.values()) {
      if (firstClusteringKeyType == DataType.BLOB) {
        continue;
      }
      for (DataType secondClusteringKeyType : DataType.values()) {
        if (secondClusteringKeyType == DataType.BLOB) {
          continue;
        }
        clusteringKeyTypes.put(firstClusteringKeyType, secondClusteringKeyType);
      }
    }
    return clusteringKeyTypes;
  }

  @Override
  protected int getThreadNum() {
    return 3;
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return CosmosEnv.getCreationOptions();
  }
}
