package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.MultipleClusteringKeysIntegrationTestBase;
import java.io.IOException;
import java.util.Collections;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;

public class JdbcMultipleClusteringKeysIntegrationTest
    extends MultipleClusteringKeysIntegrationTestBase {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    admin = new JdbcDatabaseAdmin(JdbcEnv.getJdbcConfig());
    storage = new JdbcDatabase(JdbcEnv.getJdbcConfig());
    createTestTables(Collections.emptyMap());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    deleteTestTables();
    admin.close();
    storage.close();
  }

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return JdbcEnv.getJdbcConfig();
  }

  @Override
  public void scan_WithClusteringKeyRangeOfValuesFloatAfter_ShouldReturnProperlyResult()
      throws IOException, ExecutionException {
    // Ignore in case of mysql, mysql rounds the float value so number of results get from scan is
    // different
    Assume.assumeTrue(
        JdbcUtils.getRdbEngine(JdbcEnv.getJdbcConfig().getContactPoints().get(0))
            != RdbEngine.MYSQL);
    super.scan_WithClusteringKeyRangeOfValuesFloatAfter_ShouldReturnProperlyResult();
  }

  @Override
  public void
      scan_WithClusteringKeyStartInclusiveRangeOfValuesFloatAfter_ShouldReturnProperlyResult()
          throws IOException, ExecutionException {
    // Ignore in case of mysql, mysql rounds the float value so number of results get from scan is
    // different
    Assume.assumeTrue(
        JdbcUtils.getRdbEngine(JdbcEnv.getJdbcConfig().getContactPoints().get(0))
            != RdbEngine.MYSQL);
    super.scan_WithClusteringKeyStartInclusiveRangeOfValuesFloatAfter_ShouldReturnProperlyResult();
  }

  @Override
  public void
      scan_WithClusteringKeyStartExclusiveRangeOfValuesFloatAfter_ShouldReturnProperlyResult()
          throws IOException, ExecutionException {
    // Ignore in case of mysql, mysql rounds the float value so number of results get from scan is
    // different
    Assume.assumeTrue(
        JdbcUtils.getRdbEngine(JdbcEnv.getJdbcConfig().getContactPoints().get(0))
            != RdbEngine.MYSQL);
    super.scan_WithClusteringKeyStartExclusiveRangeOfValuesFloatAfter_ShouldReturnProperlyResult();
  }
}
