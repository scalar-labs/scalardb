package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageAdminRepairIntegrationTestBase;
import java.util.Properties;

public class ObjectStorageAdminRepairIntegrationTest
    extends DistributedStorageAdminRepairIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }

  @Override
  protected void initialize(String testName) throws Exception {
    super.initialize(testName);
    adminTestUtils = new ObjectStorageAdminTestUtils(getProperties(testName));
  }
}
