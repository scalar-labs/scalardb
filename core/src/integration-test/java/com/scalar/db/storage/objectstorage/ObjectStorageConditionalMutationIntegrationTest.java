package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageConditionalMutationIntegrationTestBase;
import java.util.Properties;

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
}
