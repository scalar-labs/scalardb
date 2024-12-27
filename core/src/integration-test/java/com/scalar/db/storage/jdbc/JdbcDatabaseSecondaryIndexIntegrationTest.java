package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageSecondaryIndexIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.util.TestUtils;
import java.util.Properties;
import java.util.Random;

public class JdbcDatabaseSecondaryIndexIntegrationTest
    extends DistributedStorageSecondaryIndexIntegrationTestBase {

  private RdbEngineStrategy<?, ?, ?, ?> rdbEngine;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
    return properties;
  }

  @Override
  protected Column<?> getColumnWithRandomValue(
      Random random, String columnName, DataType dataType) {
    if (JdbcTestUtils.isOracle(rdbEngine)) {
      if (dataType == DataType.DOUBLE) {
        return JdbcTestUtils.getRandomOracleDoubleValue(random, columnName);
      }
      // don't allow empty value since Oracle treats empty value as NULL
      return TestUtils.getColumnWithRandomValue(random, columnName, dataType, false);
    }
    return super.getColumnWithRandomValue(random, columnName, dataType);
  }

  @Override
  protected Column<?> getColumnWithMinValue(String columnName, DataType dataType) {
    if (JdbcTestUtils.isOracle(rdbEngine)) {
      if (dataType == DataType.DOUBLE) {
        return JdbcTestUtils.getMinOracleDoubleValue(columnName);
      }
      // don't allow empty value since Oracle treats empty value as NULL
      return TestUtils.getColumnWithMinValue(columnName, dataType, false);
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
