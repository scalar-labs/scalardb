package com.scalar.db.storage.dynamo;

import static com.scalar.db.storage.dynamo.DynamoPermissionTestUtils.SLEEP_BETWEEN_TESTS_SECONDS;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.DistributedStorageAdminPermissionIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import com.scalar.db.util.PermissionTestUtils;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class DynamoAdminPermissionIntegrationTest
    extends DistributedStorageAdminPermissionIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return DynamoEnv.getProperties(testName);
  }

  @Override
  protected Properties getPropertiesForNormalUser(String testName) {
    return DynamoEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return ImmutableMap.of(DynamoAdmin.NO_SCALING, "false", DynamoAdmin.NO_BACKUP, "false");
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new DynamoAdminTestUtils(getProperties(testName));
  }

  @Override
  protected PermissionTestUtils getPermissionTestUtils(String testName) {
    return new DynamoPermissionTestUtils(getProperties(testName));
  }

  @Override
  protected void sleepBetweenTests() {
    Uninterruptibles.sleepUninterruptibly(SLEEP_BETWEEN_TESTS_SECONDS, TimeUnit.SECONDS);
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

  @Test
  @Override
  @Disabled("DynamoDB does not support dropping columns")
  public void dropColumnFromTable_WithSufficientPermission_ShouldSucceed() {}

  @Test
  @Override
  @Disabled("DynamoDB does not support renaming columns")
  public void renameColumn_WithSufficientPermission_ShouldSucceed() {}

  @Test
  @Override
  @Disabled("DynamoDB does not support renaming tables")
  public void renameTable_WithSufficientPermission_ShouldSucceed() {}
}
