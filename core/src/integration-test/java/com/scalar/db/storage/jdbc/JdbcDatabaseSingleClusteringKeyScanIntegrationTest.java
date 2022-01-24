package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Value;
import com.scalar.db.storage.StorageSingleClusteringKeyScanIntegrationTestBase;
import java.util.Random;

public class JdbcDatabaseSingleClusteringKeyScanIntegrationTest
    extends StorageSingleClusteringKeyScanIntegrationTestBase {

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
        return JdbcTestUtils.getRandomOracleDoubleValue(random, columnName);
      }
    }
    return super.getRandomValue(random, columnName, dataType);
  }

  @Override
  protected Value<?> getMinValue(String columnName, DataType dataType) {
    if (rdbEngine == RdbEngine.ORACLE) {
      if (dataType == DataType.DOUBLE) {
        return JdbcTestUtils.getMinOracleDoubleValue(columnName);
      }
    }
    return super.getMinValue(columnName, dataType);
  }

  @Override
  protected Value<?> getMaxValue(String columnName, DataType dataType) {
    if (rdbEngine == RdbEngine.ORACLE) {
      if (dataType == DataType.DOUBLE) {
        return JdbcTestUtils.getMaxOracleDoubleValue(columnName);
      }
    }
    if (rdbEngine == RdbEngine.SQL_SERVER) {
      if (dataType == DataType.TEXT) {
        return JdbcTestUtils.getMaxSqlServerTextValue(columnName);
      }
    }
    return super.getMaxValue(columnName, dataType);
  }
}
