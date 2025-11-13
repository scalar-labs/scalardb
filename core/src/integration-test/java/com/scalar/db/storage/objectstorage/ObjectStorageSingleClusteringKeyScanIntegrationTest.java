package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageSingleClusteringKeyScanIntegrationTestBase;
import java.util.Properties;

public class ObjectStorageSingleClusteringKeyScanIntegrationTest
    extends DistributedStorageSingleClusteringKeyScanIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }
}
