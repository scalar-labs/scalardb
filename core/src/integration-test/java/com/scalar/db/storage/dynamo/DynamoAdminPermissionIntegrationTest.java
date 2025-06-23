package com.scalar.db.storage.dynamo;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedStorageAdminPermissionIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class DynamoAdminPermissionIntegrationTest
    extends DistributedStorageAdminPermissionIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return DynamoEnv.getPropertiesForNonEmulator(testName);
  }

  @Override
  protected Properties getPropertiesForNormalUser(String testName) {
    return DynamoEnv.getPropertiesForNonEmulator(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return ImmutableMap.of(DynamoAdmin.NO_SCALING, "false", DynamoAdmin.NO_BACKUP, "false");
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new DynamoAdminTestUtils(getProperties(testName));
  }

  @Test
  @Override
  @Disabled("Import-related functionality is not supported in DynamoDB")
  public void getImportTableMetadata_WithSufficientPermission_ShouldSucceed() {}

  @Test
  @Override
  @Disabled("Import-related functionality is not supported in DynamoDB")
  public void addRawColumnToTable_WithSufficientPermission_ShouldSucceed() {}

  @Test
  @Override
  @Disabled("Import-related functionality is not supported in DynamoDB")
  public void importTable_WithSufficientPermission_ShouldSucceed() {}
}
