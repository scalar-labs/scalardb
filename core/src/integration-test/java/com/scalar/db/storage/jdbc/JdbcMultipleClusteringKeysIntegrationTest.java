package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import com.scalar.db.storage.StorageMultipleClusteringKeysIntegrationTestBase;
import java.util.Random;
import java.util.stream.IntStream;

public class JdbcMultipleClusteringKeysIntegrationTest
    extends StorageMultipleClusteringKeysIntegrationTestBase {

  private static final double ORACLE_DOUBLE_MAX_VALUE = 9.99999999999999E125D;
  private static final double ORACLE_DOUBLE_MIN_VALUE = -9.99999999999999E125D;

  private static RdbEngine rdbEngine;

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    JdbcConfig jdbcConfig = JdbcEnv.getJdbcConfig();
    rdbEngine = JdbcUtils.getRdbEngine(jdbcConfig.getContactPoints().get(0));
    return jdbcConfig;
  }

  @Override
  protected Value<?> getRandomValue(Random random, String columnName, DataType dataType) {
    if (rdbEngine == RdbEngine.ORACLE) {
      if (dataType == DataType.DOUBLE) {
        return new DoubleValue(columnName, nextOracleDouble(random));
      }
    }
    return super.getRandomValue(random, columnName, dataType);
  }

  private double nextOracleDouble(Random random) {
    return random
        .doubles(ORACLE_DOUBLE_MIN_VALUE, ORACLE_DOUBLE_MAX_VALUE)
        .limit(1)
        .findFirst()
        .orElse(0.0d);
  }

  @Override
  protected Value<?> getMinValue(String columnName, DataType dataType) {
    if (rdbEngine == RdbEngine.ORACLE) {
      if (dataType == DataType.DOUBLE) {
        return new DoubleValue(columnName, ORACLE_DOUBLE_MIN_VALUE);
      }
    }
    return super.getMinValue(columnName, dataType);
  }

  @Override
  protected Value<?> getMaxValue(String columnName, DataType dataType) {
    if (rdbEngine == RdbEngine.ORACLE) {
      if (dataType == DataType.DOUBLE) {
        return new DoubleValue(columnName, ORACLE_DOUBLE_MAX_VALUE);
      }
    }

    if (rdbEngine == RdbEngine.SQL_SERVER) {
      if (dataType == DataType.TEXT) {
        // Since SQL Server can't handle 0xFF character correctly, we use "ZZZ..." as the max value
        StringBuilder builder = new StringBuilder();
        IntStream.range(0, TEXT_MAX_COUNT).forEach(i -> builder.append('Z'));
        return new TextValue(columnName, builder.toString());
      }
    }
    return super.getMaxValue(columnName, dataType);
  }
}
