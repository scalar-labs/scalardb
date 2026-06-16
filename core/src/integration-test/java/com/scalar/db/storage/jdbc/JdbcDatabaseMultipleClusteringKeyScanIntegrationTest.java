package com.scalar.db.storage.jdbc;

import com.google.errorprone.annotations.concurrent.LazyInit;
import com.scalar.db.api.DistributedStorageMultipleClusteringKeyScanIntegrationTestBase;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import java.util.Properties;
import java.util.Random;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.condition.DisabledIf;

/**
 * For the Spanner emulator test, see {@link
 * JdbcDatabaseMultipleClusteringKeyScanIntegrationTestWithSpanner}
 */
@DisabledIf("com.scalar.db.storage.jdbc.JdbcEnv#isSpannerEmulator")
public class JdbcDatabaseMultipleClusteringKeyScanIntegrationTest
    extends DistributedStorageMultipleClusteringKeyScanIntegrationTestBase {

  @LazyInit private RdbEngineStrategy rdbEngine;
  private JdbcAdminTestUtils jdbcAdminTestUtils;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
    if (JdbcEnv.isYugabyte() && jdbcAdminTestUtils == null) {
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
  protected int getThreadNum() {
    if (JdbcTestUtils.isOracle(rdbEngine) || JdbcEnv.isSpannerEmulator()) {
      return 1;
    }
    return super.getThreadNum();
  }

  @Override
  protected void truncateTable(
      DataType firstClusteringKeyType,
      Order firstClusteringOrder,
      DataType secondClusteringKeyType,
      Order secondClusteringOrder)
      throws ExecutionException {
    // Use DML DELETE for YugabyteDB: TRUNCATE is DDL that conflicts with table locking and is slow.
    // This only affects @BeforeEach cleanup. The actual truncateTable() API is tested in admin ITs.
    if (JdbcEnv.isYugabyte()) {
      jdbcAdminTestUtils.deleteAllRowsWithSql(
          getNamespaceBaseName() + firstClusteringKeyType + "_" + firstClusteringOrder,
          firstClusteringKeyType
              + "_"
              + firstClusteringOrder
              + "_"
              + secondClusteringKeyType
              + "_"
              + secondClusteringOrder);
      return;
    }
    super.truncateTable(
        firstClusteringKeyType,
        firstClusteringOrder,
        secondClusteringKeyType,
        secondClusteringOrder);
  }

  @Override
  protected boolean isParallelDdlSupported() {
    if (JdbcEnv.isYugabyte() || JdbcEnv.isSpannerEmulator()) {
      return false;
    }
    return super.isParallelDdlSupported();
  }

  // TODO: Remove this once https://github.com/yugabyte/yugabyte-db/issues/22140 is fixed and the
  //       fix is released.
  @SuppressWarnings("unused")
  private boolean isYugabyteDb() {
    return JdbcEnv.isYugabyte();
  }

  @Override
  protected Column<?> getColumnWithRandomValue(
      Random random, String columnName, DataType dataType) {
    if (JdbcTestUtils.isOracle(rdbEngine)) {
      if (dataType == DataType.DOUBLE) {
        return JdbcTestUtils.getRandomOracleDoubleValue(random, columnName);
      }
    }
    return super.getColumnWithRandomValue(random, columnName, dataType);
  }

  @Override
  protected Column<?> getColumnWithMinValue(String columnName, DataType dataType) {
    if (JdbcTestUtils.isOracle(rdbEngine)) {
      if (dataType == DataType.DOUBLE) {
        return JdbcTestUtils.getMinOracleDoubleValue(columnName);
      }
    }
    if (JdbcTestUtils.isDb2(rdbEngine)) {
      if (dataType == DataType.FLOAT) {
        return JdbcTestUtils.getMinDb2FloatValue(columnName);
      }
      if (dataType == DataType.DOUBLE) {
        return JdbcTestUtils.getMinDb2DoubleValue(columnName);
      }
    }
    return super.getColumnWithMinValue(columnName, dataType);
  }

  @Override
  protected Column<?> getColumnWithMaxValue(String columnName, DataType dataType) {
    if (JdbcTestUtils.isOracle(rdbEngine)) {
      if (dataType == DataType.DOUBLE) {
        return JdbcTestUtils.getMaxOracleDoubleValue(columnName);
      }
    }
    if (JdbcTestUtils.isSqlServer(rdbEngine)) {
      if (dataType == DataType.TEXT) {
        return JdbcTestUtils.getMaxSqlServerTextValue(columnName);
      }
    }
    return super.getColumnWithMaxValue(columnName, dataType);
  }
}
