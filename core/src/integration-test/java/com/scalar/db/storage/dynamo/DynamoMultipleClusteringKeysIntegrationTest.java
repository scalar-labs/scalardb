package com.scalar.db.storage.dynamo;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.MultipleClusteringKeysIntegrationTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class DynamoMultipleClusteringKeysIntegrationTest
    extends MultipleClusteringKeysIntegrationTestBase {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    admin = new DynamoAdmin(DynamoEnv.getDynamoConfig());
    storage = new Dynamo(DynamoEnv.getDynamoConfig());
    createTestTables(DynamoEnv.getCreateOptions());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    deleteTestTables();
    admin.close();
    storage.close();
  }

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return DynamoEnv.getDynamoConfig();
  }
}
