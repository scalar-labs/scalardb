package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.JdbcPermissionTestUtils.DDL_WAIT_SECONDS;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.DistributedStorageAdminPermissionIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.util.AdminTestUtils;
import com.scalar.db.util.PermissionTestUtils;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

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
    if (JdbcTestUtils.isYugabyte(rdbEngine)) {
      // This is needed to avoid schema or catalog version mismatch database errors.
      Uninterruptibles.sleepUninterruptibly(DDL_WAIT_SECONDS, TimeUnit.SECONDS);
    }
  }

  @Override
  protected void waitForNamespaceCreation() {
    if (JdbcTestUtils.isYugabyte(rdbEngine)) {
      // This is needed to avoid schema or catalog version mismatch database errors.
      Uninterruptibles.sleepUninterruptibly(DDL_WAIT_SECONDS, TimeUnit.SECONDS);
    }
  }

  @Override
  protected void waitForTableDeletion() {
    if (JdbcTestUtils.isYugabyte(rdbEngine)) {
      // This is needed to avoid schema or catalog version mismatch database errors.
      Uninterruptibles.sleepUninterruptibly(DDL_WAIT_SECONDS, TimeUnit.SECONDS);
    }
  }

  @Override
  protected void waitForNamespaceDeletion() {
    if (JdbcTestUtils.isYugabyte(rdbEngine)) {
      // This is needed to avoid schema or catalog version mismatch database errors.
      Uninterruptibles.sleepUninterruptibly(DDL_WAIT_SECONDS, TimeUnit.SECONDS);
    }
  }

  @Override
  protected void sleepBetweenTests() {
    if (JdbcTestUtils.isYugabyte(rdbEngine)) {
      // This is needed to avoid schema or catalog version mismatch database errors.
      Uninterruptibles.sleepUninterruptibly(DDL_WAIT_SECONDS, TimeUnit.SECONDS);
    }
  }
}
