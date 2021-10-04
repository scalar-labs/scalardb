package com.scalar.db.storage.cassandra;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.MultipleClusteringKeysIntegrationTestBase;
import java.util.Collections;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class CassandraMultipleClusteringKeysIntegrationTest
    extends MultipleClusteringKeysIntegrationTestBase {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    admin = new CassandraAdmin(CassandraEnv.getDatabaseConfig());
    storage = new Cassandra(CassandraEnv.getDatabaseConfig());
    createTestTables(Collections.emptyMap());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    deleteTestTables();
    admin.close();
    storage.close();
  }

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CassandraEnv.getDatabaseConfig();
  }
}
