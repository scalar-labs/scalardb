package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageMultipleClusteringKeyScanIntegrationTestBase;
import java.util.Properties;

public class ObjectStorageMultipleClusteringKeyScanIntegrationTest
    extends DistributedStorageMultipleClusteringKeyScanIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }

  @Override
  protected int getThreadNum() {
    return 3;
  }

  @Override
  protected boolean isParallelDdlSupported() {
    return false;
  }
}
