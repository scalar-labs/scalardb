package com.scalar.db.storage.jdbc;

import com.google.errorprone.annotations.concurrent.LazyInit;
import com.scalar.db.api.DistributedStorageMultipleClusteringKeyScanIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Value;
import java.util.Properties;
import java.util.Random;

public class JdbcDatabaseMultipleClusteringKeyScanIntegrationTest
    extends DistributedStorageMultipleClusteringKeyScanIntegrationTestBase {

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
    if (rdbEngine instanceof RdbEngineOracle) {
      return 1;
    }
    return super.getThreadNum();
  }

  @Override
  protected boolean isParallelDdlSupported() {
    if (rdbEngine instanceof RdbEngineYugabyte) {
      return false;
    }
    return super.isParallelDdlSupported();
  }

  @Override
  protected boolean isYugabyteDb() {
    return rdbEngine instanceof RdbEngineYugabyte;
  }

  @Override
  protected Value<?> getRandomValue(Random random, String columnName, DataType dataType) {
    if (rdbEngine instanceof RdbEngineOracle) {
      if (dataType == DataType.DOUBLE) {
        return JdbcTestUtils.getRandomOracleDoubleValue(random, columnName);
      }
    }
    return super.getRandomValue(random, columnName, dataType);
  }

  @Override
  protected Value<?> getMinValue(String columnName, DataType dataType) {
    if (rdbEngine instanceof RdbEngineOracle) {
      if (dataType == DataType.DOUBLE) {
        return JdbcTestUtils.getMinOracleDoubleValue(columnName);
      }
    }
    return super.getMinValue(columnName, dataType);
  }

  @Override
  protected Value<?> getMaxValue(String columnName, DataType dataType) {
    if (rdbEngine instanceof RdbEngineOracle) {
      if (dataType == DataType.DOUBLE) {
        return JdbcTestUtils.getMaxOracleDoubleValue(columnName);
      }
    }
    if (rdbEngine instanceof RdbEngineSqlServer) {
      if (dataType == DataType.TEXT) {
        return JdbcTestUtils.getMaxSqlServerTextValue(columnName);
      }
    }
    return super.getMaxValue(columnName, dataType);
  }
}
