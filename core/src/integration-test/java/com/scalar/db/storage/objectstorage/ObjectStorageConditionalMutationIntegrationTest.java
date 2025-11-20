package com.scalar.db.storage.objectstorage;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.DistributedStorageConditionalMutationIntegrationTestBase;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ObjectStorageConditionalMutationIntegrationTest
    extends DistributedStorageConditionalMutationIntegrationTestBase {

  @Override
  protected int getThreadNum() {
    return 3;
  }

  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }

  @Override
  protected void waitToAvoidRateLimiting() {
    if (ObjectStorageEnv.isCloudStorage()) {
      Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
    }
  }
}
