package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.StorageWithReservedKeywordIntegrationTestBase;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
public class JdbcDatabaseWithReservedKeywordIntegrationTest
    extends StorageWithReservedKeywordIntegrationTestBase {

  @Override
  protected void initialize() {
    // reserved keywords in JDBC
    NAMESPACE = "between";
    TABLE = "create";
    COL_NAME1 = "from";
    COL_NAME2 = "to";
    COL_NAME3 = "values";
    COL_NAME4 = "like";
    COL_NAME5 = "order";
  }

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return JdbcEnv.getJdbcConfig();
  }
}
