package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageColumnValueIntegrationTestBase;
import java.util.Properties;

public class ObjectStorageColumnValueIntegrationTest
    extends DistributedStorageColumnValueIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }
}
