package com.scalar.db.storage.jdbc;

import static java.util.stream.Collectors.toSet;

import com.scalar.db.api.DistributedStorageSinglePartitionKeyIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

public class JdbcDatabaseSinglePartitionKeyIntegrationTest
    extends DistributedStorageSinglePartitionKeyIntegrationTestBase {

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
  protected Set<DataType> getPartitionKeyTypes() {
    return super.getPartitionKeyTypes().stream()
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
        .collect(toSet());
  }
}
