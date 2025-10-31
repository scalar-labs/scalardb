package com.scalar.db.storage.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.scalar.db.api.DistributedStorageSecondaryIndexIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.util.TestUtils;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

public class JdbcDatabaseSecondaryIndexIntegrationTest
    extends DistributedStorageSecondaryIndexIntegrationTestBase {

  private RdbEngineStrategy rdbEngine;

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

  @Override
  protected Set<DataType> getSecondaryIndexTypes() {
    // BLOB type cannot be used as a secondary index in Db2
    return Sets.newHashSet(
        JdbcTestUtils.filterDataTypes(
            Arrays.asList(DataType.values()),
            rdbEngine,
            ImmutableMap.of(
                RdbEngineDb2.class,
                ImmutableList.of(DataType.BLOB),
                RdbEngineOracle.class,
                ImmutableList.of(DataType.BLOB))));
  }
}
