package com.scalar.db.storage.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.concurrent.LazyInit;
import com.scalar.db.api.DistributedStorageMultipleClusteringKeyScanIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class JdbcDatabaseMultipleClusteringKeyScanIntegrationTest
    extends DistributedStorageMultipleClusteringKeyScanIntegrationTestBase {

  @LazyInit private RdbEngineStrategy<?, ?, ?, ?> rdbEngine;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
    return properties;
  }

  @Override
  protected int getThreadNum() {
    if (JdbcTestUtils.isOracle(rdbEngine)) {
      return 1;
    }
    return super.getThreadNum();
  }

  @Override
  protected boolean isParallelDdlSupported() {
    if (JdbcTestUtils.isYugabyte(rdbEngine)) {
      return false;
    }
    return super.isParallelDdlSupported();
  }

  // TODO: Remove this once https://github.com/yugabyte/yugabyte-db/issues/22140 is fixed and the
  //       fix is released.
  @SuppressWarnings("unused")
  private boolean isYugabyteDb() {
    return JdbcTestUtils.isYugabyte(rdbEngine);
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

  @Override
  protected List<DataType> getDataTypes() {
    // TIMESTAMP WITH TIME ZONE type cannot be used as a primary key in Oracle
    return JdbcTestUtils.filterDataTypes(
        super.getDataTypes(),
        rdbEngine,
        ImmutableMap.of(RdbEngineOracle.class, ImmutableList.of(DataType.TIMESTAMPTZ)));
  }
}
