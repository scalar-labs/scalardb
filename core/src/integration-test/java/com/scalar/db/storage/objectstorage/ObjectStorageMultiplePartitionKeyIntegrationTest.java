package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageMultiplePartitionKeyIntegrationTestBase;
import java.util.Properties;

public class ObjectStorageMultiplePartitionKeyIntegrationTest
    extends DistributedStorageMultiplePartitionKeyIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }

  @Override
  protected boolean isParallelDdlSupported() {
    return false;
  }
}
