package com.scalar.db.storage.jdbc;

import static java.util.stream.Collectors.toList;

import com.google.errorprone.annotations.concurrent.LazyInit;
import com.scalar.db.api.DistributedStorageMultiplePartitionKeyIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class JdbcDatabaseMultiplePartitionKeyIntegrationTest
    extends DistributedStorageMultiplePartitionKeyIntegrationTestBase {

  @LazyInit private RdbEngineStrategy rdbEngine;

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
    return super.getDataTypes().stream()
        .filter(
            type -> {
              if (JdbcTestUtils.isOracle(rdbEngine) && type == DataType.TIMESTAMPTZ) {
                // TIMESTAMP WITH TIME ZONE type cannot be used as a primary key in Oracle
                return false;
              } else if (JdbcTestUtils.isYugabyte(rdbEngine)
                  && (type == DataType.FLOAT || type == DataType.DOUBLE)) {
                // FLOAT and DOUBLE types cannot be used as partition key in Yugabyte
                return false;
              } else {
                return true;
              }
            })
        .collect(toList());
  }
}
