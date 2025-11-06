package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageAdminRepairTableIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class ObjectStorageAdminRepairTableIntegrationTest
    extends DistributedStorageAdminRepairTableIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new ObjectStorageAdminTestUtils(getProperties(testName));
  }

  @Override
  @Disabled("Object Storage recreates missing coordinator tables")
  public void repairTable_ForNonExistingTable_ShouldThrowIllegalArgument() {}
}
