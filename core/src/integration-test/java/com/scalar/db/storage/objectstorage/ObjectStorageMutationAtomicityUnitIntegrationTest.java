package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageMutationAtomicityUnitIntegrationTestBase;
import java.util.Map;
import java.util.Properties;

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
}
