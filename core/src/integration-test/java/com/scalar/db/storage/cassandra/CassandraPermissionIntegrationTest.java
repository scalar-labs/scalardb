package com.scalar.db.storage.cassandra;

import static com.scalar.db.storage.cassandra.CassandraPermissionTestUtils.MAX_RETRY_COUNT;
import static com.scalar.db.storage.cassandra.CassandraPermissionTestUtils.SLEEP_BETWEEN_RETRIES_SECONDS;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.DistributedStoragePermissionIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import com.scalar.db.util.PermissionTestUtils;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class CassandraPermissionIntegrationTest
    extends DistributedStoragePermissionIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return CassandraEnv.getProperties(testName);
  }

  @Override
  protected Properties getPropertiesForNormalUser(String testName) {
    return CassandraEnv.getPropertiesForNormalUser(testName);
  }

  @Override
  protected PermissionTestUtils getPermissionTestUtils(String testName) {
    return new CassandraPermissionTestUtils(getProperties(testName));
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new CassandraAdminTestUtils(getProperties(testName));
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return Collections.singletonMap(CassandraAdmin.REPLICATION_FACTOR, "1");
  }

  @Override
  protected void waitForDdlCompletion() {
    try {
      AdminTestUtils utils = getAdminTestUtils(TEST_NAME);
      int retryCount = 0;
      while (retryCount < MAX_RETRY_COUNT) {
        if (utils.tableExists(NAMESPACE, TABLE)) {
          utils.close();
          return;
        }
        Uninterruptibles.sleepUninterruptibly(SLEEP_BETWEEN_RETRIES_SECONDS, TimeUnit.SECONDS);
        retryCount++;
      }
      utils.close();
      throw new RuntimeException("Table was not created after " + MAX_RETRY_COUNT + " retries");
    } catch (Exception e) {
      throw new RuntimeException("Failed to wait for table creation", e);
    }
  }
}
