package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageConditionalMutationIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.util.TestUtils;
import java.util.Properties;
import java.util.Random;

public class JdbcDatabaseConditionalMutationIntegrationTest
    extends DistributedStorageConditionalMutationIntegrationTestBase {

  private RdbEngineStrategy<?, ?, ?, ?> rdbEngine;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
    return properties;
  }

  @Override
  protected int getThreadNum() {
    if (JdbcTestUtils.isMysql(rdbEngine)) {
      // Since Deadlock error sometimes happens in MySQL, change the concurrency to 1
      return 1;
    }
    return super.getThreadNum();
  }

  @Override
  protected Column<?> getColumnWithRandomValue(
      Random random, String columnName, DataType dataType) {
    if (JdbcTestUtils.isOracle(rdbEngine)) {
      if (dataType == DataType.DOUBLE) {
        return JdbcTestUtils.getRandomOracleDoubleColumn(random, columnName);
      }
      // don't allow empty value since Oracle treats empty value as NULL
      return TestUtils.getColumnWithRandomValue(random, columnName, dataType, false);
    }
    return super.getColumnWithRandomValue(random, columnName, dataType);
  }
}
