package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageSingleClusteringKeyScanIntegrationTestBase;
import com.scalar.db.io.DataType;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ObjectStorageSingleClusteringKeyScanIntegrationTest
    extends DistributedStorageSingleClusteringKeyScanIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }

  @Override
  protected List<DataType> getClusteringKeyTypes() {
    // Return types without BLOB because blob is not supported for clustering key for now
    List<DataType> clusteringKeyTypes = new ArrayList<>();
    for (DataType dataType : DataType.values()) {
      if (dataType == DataType.BLOB) {
        continue;
      }
      clusteringKeyTypes.add(dataType);
    }
    return clusteringKeyTypes;
  }
}
