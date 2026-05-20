package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageConditionalMutationIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.util.TestUtils;
import java.util.Properties;
import java.util.Random;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.condition.DisabledIf;

/**
 * For the Spanner emulator test, see {@link
 * JdbcDatabaseConditionalMutationIntegrationTestWithSpanner}
 */
@DisabledIf("com.scalar.db.storage.jdbc.JdbcEnv#isSpannerEmulator")
public class JdbcDatabaseConditionalMutationIntegrationTest
    extends DistributedStorageConditionalMutationIntegrationTestBase {

  private RdbEngineStrategy rdbEngine;
  private JdbcAdminTestUtils jdbcAdminTestUtils;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
    if (JdbcEnv.isYugabyte()) {
      jdbcAdminTestUtils = new JdbcAdminTestUtils(properties);
    }
    return properties;
  }

  @AfterAll
  void closeJdbcAdminTestUtils() throws Exception {
    if (jdbcAdminTestUtils != null) {
      jdbcAdminTestUtils.close();
    }
  }

  @Override
  protected void truncateTable() throws ExecutionException {
    // Use DML DELETE for YugabyteDB: TRUNCATE is DDL that conflicts with table locking.
    // This only affects @BeforeEach cleanup. The actual truncateTable() API is tested in admin ITs.
    if (JdbcEnv.isYugabyte()) {
      jdbcAdminTestUtils.deleteAllRowsWithSql(getNamespace(), TABLE);
      return;
    }
    super.truncateTable();
  }

  @Override
  protected int getThreadNum() {
    if (JdbcTestUtils.isMysql(rdbEngine)) {
      // Since Deadlock error sometimes happens in MySQL, change the concurrency to 1
      return 1;
    }
    return super.getThreadNum();
  }

  @Override
  protected Column<?> getColumnWithRandomValue(
      Random random, String columnName, DataType dataType) {
    if (JdbcTestUtils.isOracle(rdbEngine)) {
      if (dataType == DataType.DOUBLE) {
        return JdbcTestUtils.getRandomOracleDoubleColumn(random, columnName);
      }
      // don't allow empty value since Oracle treats empty value as NULL
      return TestUtils.getColumnWithRandomValue(random, columnName, dataType, false);
    }
    return super.getColumnWithRandomValue(random, columnName, dataType);
  }

  @Override
  protected boolean isConditionOnBlobColumnSupported() {
    return !JdbcTestUtils.isOracle(rdbEngine);
  }
}
