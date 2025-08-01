package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageWithReservedKeywordIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;

public class JdbcDatabaseWithReservedKeywordIntegrationTest
    extends DistributedStorageWithReservedKeywordIntegrationTestBase {

  private RdbEngineStrategy rdbEngine;

  @Override
  protected String getNamespace() {
    // a reserved keyword in JDBC
    return "between";
  }

  @Override
  protected String getTableName() {
    // a reserved keyword in JDBC
    return "create";
  }

  @Override
  protected String getColumnName1() {
    // a reserved keyword in JDBC
    return "from";
  }

  @Override
  protected String getColumnName2() {
    // a reserved keyword in JDBC
    return "to";
  }

  @Override
  protected String getColumnName3() {
    // a reserved keyword in JDBC
    return "values";
  }

  @Override
  protected String getColumnName4() {
    // a reserved keyword in JDBC
    return "like";
  }

  @Override
  protected String getColumnName5() {
    // a reserved keyword in JDBC
    return "order";
  }

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
    return JdbcEnv.getProperties(testName);
  }

  @Override
  protected int getLargeDataSizeInBytes() {
    if (JdbcTestUtils.isOracle(rdbEngine)) {
      // For Oracle, the max data size for BLOB is 2000 bytes
      return 2000;
    } else {
      return super.getLargeDataSizeInBytes();
    }
  }
}
