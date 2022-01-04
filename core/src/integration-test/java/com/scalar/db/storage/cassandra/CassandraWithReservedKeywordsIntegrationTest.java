package com.scalar.db.storage.cassandra;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.StorageIntegrationTestBase;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
public class CassandraWithReservedKeywordsIntegrationTest extends StorageIntegrationTestBase {
  @Override
  protected void initialize() {
    NAMESPACE = "keyspace";
    TABLE = "table";
    COL_NAME1 = "from";
    COL_NAME2 = "to";
    COL_NAME3 = "one";
    COL_NAME4 = "two";
    COL_NAME5 = "password";
  }

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CassandraEnv.getDatabaseConfig();
  }
}
