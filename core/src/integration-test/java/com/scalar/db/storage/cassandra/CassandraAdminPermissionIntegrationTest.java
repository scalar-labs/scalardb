package com.scalar.db.storage.cassandra;

import static com.scalar.db.storage.cassandra.CassandraPermissionTestUtils.MAX_RETRY_COUNT;
import static com.scalar.db.storage.cassandra.CassandraPermissionTestUtils.SLEEP_BETWEEN_RETRIES_SECONDS;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.DistributedStorageAdminPermissionIntegrationTestBase;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.util.AdminTestUtils;
import com.scalar.db.util.PermissionTestUtils;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class CassandraAdminPermissionIntegrationTest
    extends DistributedStorageAdminPermissionIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return CassandraEnv.getProperties(testName);
  }

  @Override
  protected Properties getPropertiesForNormalUser(String testName) {
    return CassandraEnv.getPropertiesForNormalUser(testName);
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new CassandraAdminTestUtils(getProperties(testName));
  }

  @Override
  protected PermissionTestUtils getPermissionTestUtils(String testName) {
    return new CassandraPermissionTestUtils(getProperties(testName));
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return Collections.singletonMap(CassandraAdmin.REPLICATION_FACTOR, "1");
  }

  @Override
  protected void waitForNamespaceCreation() {
    try {
      AdminTestUtils utils = getAdminTestUtils(TEST_NAME);
      int retryCount = 0;
      while (retryCount < MAX_RETRY_COUNT) {
        if (utils.namespaceExists(NAMESPACE)) {
          utils.close();
          return;
        }
        Uninterruptibles.sleepUninterruptibly(
            SLEEP_BETWEEN_RETRIES_SECONDS, java.util.concurrent.TimeUnit.SECONDS);
        retryCount++;
      }
      utils.close();
      throw new RuntimeException("Namespace was not created after " + MAX_RETRY_COUNT + " retries");
    } catch (Exception e) {
      throw new RuntimeException("Failed to wait for namespace creation", e);
    }
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
  protected void waitForNamespaceDeletion() {
    try {
      AdminTestUtils utils = getAdminTestUtils(TEST_NAME);
      int retryCount = 0;
      while (retryCount < MAX_RETRY_COUNT) {
        if (!utils.namespaceExists(NAMESPACE)) {
          utils.close();
          return;
        }
        Uninterruptibles.sleepUninterruptibly(
            SLEEP_BETWEEN_RETRIES_SECONDS, java.util.concurrent.TimeUnit.SECONDS);
        retryCount++;
      }
      utils.close();
      throw new RuntimeException("Namespace was not deleted after " + MAX_RETRY_COUNT + " retries");
    } catch (Exception e) {
      throw new RuntimeException("Failed to wait for namespace deletion", e);
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

  @Test
  @Override
  @Disabled("Import-related functionality is not supported in Cassandra")
  public void getImportTableMetadata_WithSufficientPermission_ShouldSucceed() {}

  @Test
  @Override
  @Disabled("Import-related functionality is not supported in Cassandra")
  public void addRawColumnToTable_WithSufficientPermission_ShouldSucceed() {}

  @Test
  @Override
  @Disabled("Import-related functionality is not supported in Cassandra")
  public void importTable_WithSufficientPermission_ShouldSucceed() {}

  @Test
  @Override
  public void renameColumn_WithSufficientPermission_ShouldSucceed() throws ExecutionException {
    // Arrange
    createNamespaceByRoot();
    createTableByRoot();

    // Act Assert
    // Cassandra does not support renaming non-primary key columns
    assertThatCode(() -> adminForNormalUser.renameColumn(NAMESPACE, TABLE, COL_NAME1, NEW_COL_NAME))
        .doesNotThrowAnyException();
  }
}
