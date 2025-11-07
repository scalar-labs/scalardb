package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageConditionalMutationIntegrationTestBase;
import java.util.Properties;

public class ObjectStorageConditionalMutationIntegrationTest
    extends DistributedStorageConditionalMutationIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }
}
