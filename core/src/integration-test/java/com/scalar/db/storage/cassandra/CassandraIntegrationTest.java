package com.scalar.db.storage.cassandra;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.StorageIntegrationTestBase;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
public class CassandraIntegrationTest extends StorageIntegrationTestBase {
  @Override
  protected void initialize() {
    NAMESPACE = "cassandra_integration_testing";
    TABLE = "test_table";
    COL_NAME1 = "c1";
    COL_NAME2 = "c2";
    COL_NAME3 = "c3";
    COL_NAME4 = "c4";
    COL_NAME5 = "c5";
  }

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CassandraEnv.getDatabaseConfig();
  }
}
