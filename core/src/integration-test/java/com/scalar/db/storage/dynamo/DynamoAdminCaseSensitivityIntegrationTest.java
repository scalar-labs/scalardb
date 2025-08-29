package com.scalar.db.storage.dynamo;

import com.scalar.db.api.DistributedStorageAdminCaseSensitivityIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class DynamoAdminCaseSensitivityIntegrationTest
    extends DistributedStorageAdminCaseSensitivityIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return DynamoEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
  }

  @Override
  protected boolean isIndexOnBooleanColumnSupported() {
    return false;
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new DynamoAdminTestUtils(getProperties(testName));
  }

  @Override
  @Disabled("DynamoDB does not support renaming columns")
  public void renameColumn_ShouldRenameColumnCorrectly() {}

  @Override
  @Disabled("DynamoDB does not support renaming columns")
  public void renameColumn_ForNonExistingTable_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("DynamoDB does not support renaming columns")
  public void renameColumn_ForNonExistingColumn_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("DynamoDB does not support renaming columns")
  public void renameColumn_ForPrimaryOrIndexKeyColumn_ShouldThrowIllegalArgumentException() {}
}
