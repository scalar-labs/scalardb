package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageSinglePartitionKeyIntegrationTestBase;
import java.util.Properties;

public class ObjectStorageSinglePartitionKeyIntegrationTest
    extends DistributedStorageSinglePartitionKeyIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }
}
