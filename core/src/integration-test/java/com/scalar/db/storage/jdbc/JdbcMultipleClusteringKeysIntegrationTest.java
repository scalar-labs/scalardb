package com.scalar.db.storage.jdbc;

import com.scalar.db.storage.MultipleClusteringKeysIntegrationTestBase;
import com.scalar.db.storage.jdbc.test.TestEnv;
import java.util.Collections;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class JdbcMultipleClusteringKeysIntegrationTest
    extends MultipleClusteringKeysIntegrationTestBase {

  private static TestEnv testEnv;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    testEnv = new TestEnv();
    admin = new JdbcDatabaseAdmin(testEnv.getJdbcConfig());
    distributedStorage = new JdbcDatabase(testEnv.getJdbcConfig());
    createTestTables(Collections.emptyMap());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    deleteTestTables();
    admin.close();
    distributedStorage.close();
    testEnv.close();
  }
}
