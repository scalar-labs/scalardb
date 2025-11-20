package com.scalar.db.storage.objectstorage;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.DistributedStorageMutationAtomicityUnitIntegrationTestBase;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ObjectStorageMutationAtomicityUnitIntegrationTest
    extends DistributedStorageMutationAtomicityUnitIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return ObjectStorageEnv.getCreationOptions();
  }

  @Override
  protected void waitToAvoidRateLimiting() {
    if (ObjectStorageEnv.isCloudStorage()) {
      Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
    }
  }
}
