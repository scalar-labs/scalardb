package com.scalar.db.storage.cosmos;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.MultipleClusteringKeysIntegrationTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class CosmosMultipleClusteringKeysIntegrationTest
    extends MultipleClusteringKeysIntegrationTestBase {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    admin = new CosmosAdmin(CosmosEnv.getCosmosConfig());
    storage = new Cosmos(CosmosEnv.getCosmosConfig());
    createTestTables(CosmosEnv.getCreateOptions());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    deleteTestTables();
    admin.close();
    storage.close();
  }

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CosmosEnv.getCosmosConfig();
  }
}
