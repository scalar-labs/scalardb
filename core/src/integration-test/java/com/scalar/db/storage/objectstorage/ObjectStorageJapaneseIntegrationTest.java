package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageJapaneseIntegrationTestBase;
import java.util.Properties;

public class ObjectStorageJapaneseIntegrationTest
    extends DistributedStorageJapaneseIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }
}
