package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.JdbcPermissionTestUtils.DDL_WAIT_SECONDS;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.DistributedStorageAdminPermissionIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.util.AdminTestUtils;
import com.scalar.db.util.PermissionTestUtils;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

public class JdbcAdminPermissionIntegrationTest
    extends DistributedStorageAdminPermissionIntegrationTestBase {
  private RdbEngineStrategy rdbEngine;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    rdbEngine = RdbEngineFactory.create(new JdbcConfig(new DatabaseConfig(properties)));
    return properties;
  }

  @Override
  protected Properties getPropertiesForNormalUser(String testName) {
    return JdbcEnv.getPropertiesForNormalUser(testName);
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new JdbcAdminTestUtils(getProperties(testName));
  }

  @Override
  protected PermissionTestUtils getPermissionTestUtils(String testName) {
    return new JdbcPermissionTestUtils(getProperties(testName));
  }

  @Override
  protected void waitForTableCreation() {
    waitForDdlCompletion();
  }

  @Override
  protected void waitForNamespaceCreation() {
    waitForDdlCompletion();
  }

  @Override
  protected void waitForTableDeletion() {
    waitForDdlCompletion();
  }

  @Override
  protected void waitForNamespaceDeletion() {
    waitForDdlCompletion();
  }

  @Override
  protected void sleepBetweenTests() {
    // Sleep to ensure the DDL operations executed as ACT are completed before the next setup.
    waitForDdlCompletion();
  }

  private void waitForDdlCompletion() {
    if (JdbcTestUtils.isYugabyte(rdbEngine)) {
      // This is needed to avoid schema or catalog version mismatch database errors.
      Uninterruptibles.sleepUninterruptibly(DDL_WAIT_SECONDS, TimeUnit.SECONDS);
    }
  }

  @SuppressWarnings("unused")
  private boolean isSqlite() {
    return JdbcEnv.isSqlite();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void alterColumnType_WithSufficientPermission_ShouldSucceed() throws ExecutionException {
    super.alterColumnType_WithSufficientPermission_ShouldSucceed();
  }
}
