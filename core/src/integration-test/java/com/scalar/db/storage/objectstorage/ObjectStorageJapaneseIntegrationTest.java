package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageJapaneseIntegrationTestBase;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.Properties;

public class ObjectStorageJapaneseIntegrationTest
    extends DistributedStorageJapaneseIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }

  @Override
  protected void truncateTable() throws ExecutionException {
    ObjectStorageTestUtils.executeWithRetryOnRateLimit(super::truncateTable);
  }
}
