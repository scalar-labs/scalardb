package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageMutationAtomicityUnitIntegrationTestBase;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.Map;
import java.util.Properties;

public class ObjectStorageMutationAtomicityUnitIntegrationTest
    extends DistributedStorageMutationAtomicityUnitIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }

  @Override
  protected void truncateTable() throws ExecutionException {
    ObjectStorageTestUtils.executeWithRetryOnTransientError(super::truncateTable);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return ObjectStorageEnv.getCreationOptions();
  }
}
