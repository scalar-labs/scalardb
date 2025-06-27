package com.scalar.db.storage.dynamo;

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
  private static final int SLEEP_BETWEEN_TESTS_SECONDS = 10;
  private static final int SLEEP_BETWEEN_RETRIES_SECONDS = 3;
  private static final int MAX_RETRY_COUNT = 10;

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
  protected void waitForTableCreation() {
    try {
      AdminTestUtils utils = getAdminTestUtils(TEST_NAME);
      int retryCount = 0;
      while (retryCount < MAX_RETRY_COUNT) {
        if (utils.tableExists(NAMESPACE, TABLE)) {
          utils.close();
          return;
        }
        Uninterruptibles.sleepUninterruptibly(
            SLEEP_BETWEEN_RETRIES_SECONDS, java.util.concurrent.TimeUnit.SECONDS);
        retryCount++;
      }
      utils.close();
      throw new RuntimeException("Table was not created after " + MAX_RETRY_COUNT + " retries");
    } catch (Exception e) {
      throw new RuntimeException("Failed to wait for table creation", e);
    }
  }

  @Override
  protected void waitForTableDeletion() {
    try {
      AdminTestUtils utils = getAdminTestUtils(TEST_NAME);
      int retryCount = 0;
      while (retryCount < MAX_RETRY_COUNT) {
        if (!utils.tableExists(NAMESPACE, TABLE)) {
          utils.close();
          return;
        }
        Uninterruptibles.sleepUninterruptibly(
            SLEEP_BETWEEN_RETRIES_SECONDS, java.util.concurrent.TimeUnit.SECONDS);
        retryCount++;
      }
      utils.close();
      throw new RuntimeException("Table was not deleted after " + MAX_RETRY_COUNT + " retries");
    } catch (Exception e) {
      throw new RuntimeException("Failed to wait for table deletion", e);
    }
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
}
