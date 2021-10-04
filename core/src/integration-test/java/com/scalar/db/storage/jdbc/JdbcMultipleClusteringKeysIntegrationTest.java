package com.scalar.db.storage.jdbc;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.MultipleClusteringKeysIntegrationTestBase;
import com.scalar.db.storage.jdbc.test.TestEnv;
import java.io.IOException;
import java.util.Collections;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;

public class JdbcMultipleClusteringKeysIntegrationTest
    extends MultipleClusteringKeysIntegrationTestBase {

  private static TestEnv testEnv;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    testEnv = new TestEnv();
    admin = new JdbcDatabaseAdmin(testEnv.getJdbcConfig());
    distributedStorage = new JdbcDatabase(testEnv.getJdbcConfig());
    createTestTables(Collections.emptyMap());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    deleteTestTables();
    admin.close();
    distributedStorage.close();
    testEnv.close();
  }

  @Override
  public void scan_WithClusteringKeyRangeOfValuesFloatAfter_ShouldReturnProperlyResult()
      throws IOException, ExecutionException {
    // Ignore in case of mysql, mysql rounds the float value
    Assume.assumeTrue(
        JdbcUtils.getRdbEngine(testEnv.getJdbcConfig().getContactPoints().get(0))
            != RdbEngine.MYSQL);
    super.scan_WithClusteringKeyRangeOfValuesFloatAfter_ShouldReturnProperlyResult();
  }

  @Override
  public void
      scan_WithClusteringKeyStartInclusiveRangeOfValuesFloatAfter_ShouldReturnProperlyResult()
          throws IOException, ExecutionException {
    // Ignore in case of mysql, mysql rounds the float value
    Assume.assumeTrue(
        JdbcUtils.getRdbEngine(testEnv.getJdbcConfig().getContactPoints().get(0))
            != RdbEngine.MYSQL);
    super.scan_WithClusteringKeyStartInclusiveRangeOfValuesFloatAfter_ShouldReturnProperlyResult();
  }

  @Override
  public void
      scan_WithClusteringKeyStartExclusiveRangeOfValuesFloatAfter_ShouldReturnProperlyResult()
          throws IOException, ExecutionException {
    // Ignore in case of mysql, mysql rounds the float value
    Assume.assumeTrue(
        JdbcUtils.getRdbEngine(testEnv.getJdbcConfig().getContactPoints().get(0))
            != RdbEngine.MYSQL);
    super.scan_WithClusteringKeyStartExclusiveRangeOfValuesFloatAfter_ShouldReturnProperlyResult();
  }
}
