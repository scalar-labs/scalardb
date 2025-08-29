package com.scalar.db.storage.cosmos;

import com.scalar.db.api.DistributedStorageAdminIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class CosmosAdminIntegrationTest extends DistributedStorageAdminIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return CosmosEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return CosmosEnv.getCreationOptions();
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new CosmosAdminTestUtils(getProperties(testName));
  }

  @Override
  @Disabled("Cosmos DB does not support renaming columns")
  public void renameColumn_ShouldRenameColumnCorrectly() {}

  @Override
  @Disabled("Cosmos DB does not support renaming columns")
  public void renameColumn_ForNonExistingTable_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Cosmos DB does not support renaming columns")
  public void renameColumn_ForNonExistingColumn_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Cosmos DB does not support renaming columns")
  public void renameColumn_ForPrimaryOrIndexKeyColumn_ShouldThrowIllegalArgumentException() {}
}
