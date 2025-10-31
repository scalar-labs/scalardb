package com.scalar.db.storage.objectstorage;

import java.util.Properties;

public class BlobWrapperIntegrationTest extends ObjectStorageWrapperIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }
}
