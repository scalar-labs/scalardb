package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageCrossPartitionScanIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.util.ScalarDbUtils;
import java.util.Properties;
import java.util.Random;

public class JdbcDatabaseCrossPartitionScanIntegrationTest
    extends DistributedStorageCrossPartitionScanIntegrationTestBase {

  private RdbEngineStrategy rdbEngine;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
    return JdbcEnv.getProperties(testName);
  }

  @Override
  protected int getThreadNum() {
    if (rdbEngine instanceof RdbEngineOracle) {
      return 1;
    }
    return super.getThreadNum();
  }

  @Override
  protected Column<?> getRandomColumn(Random random, String columnName, DataType dataType) {
    if (rdbEngine instanceof RdbEngineOracle) {
      if (dataType == DataType.DOUBLE) {
        return ScalarDbUtils.toColumn(JdbcTestUtils.getRandomOracleDoubleValue(random, columnName));
      }
    }
    return super.getRandomColumn(random, columnName, dataType);
  }

  @Override
  protected boolean supportParallelDdl() {
    return !(rdbEngine instanceof RdbEngineYugabyte);
  }
}
