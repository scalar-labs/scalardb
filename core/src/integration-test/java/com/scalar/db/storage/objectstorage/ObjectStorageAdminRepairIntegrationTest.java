package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageAdminRepairIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

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

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      repairTable_WhenTableAlreadyExistsWithoutIndexAndMetadataSpecifiesIndex_ShouldCreateIndex() {}
}
