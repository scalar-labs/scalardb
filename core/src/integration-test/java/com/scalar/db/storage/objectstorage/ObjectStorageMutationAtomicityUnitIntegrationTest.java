package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageAtomicityUnitIntegrationTestBase;
import java.util.Map;
import java.util.Properties;

public class ObjectStorageMutationAtomicityUnitIntegrationTest
    extends DistributedStorageAtomicityUnitIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return ObjectStorageEnv.getCreationOptions();
  }
}
