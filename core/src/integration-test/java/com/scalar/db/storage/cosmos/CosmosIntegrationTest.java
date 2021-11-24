package com.scalar.db.storage.cosmos;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.StorageIntegrationTestBase;
import java.util.Map;
import java.util.Optional;
import org.junit.AfterClass;

public class CosmosIntegrationTest extends StorageIntegrationTestBase {

  @Override
  protected void tearUp() throws Exception {
    CosmosConfig cosmosConfig = CosmosEnv.getCosmosConfig();
    Cosmos cosmosStorage = new Cosmos(cosmosConfig);
    storage = cosmosStorage;
    admin = new CosmosAdmin(cosmosStorage.getCosmosClient(), cosmosConfig);
    createTable();
  }

  @AfterClass
  public static void tearDownAfterClass() throws ExecutionException {
    deleteTable();
    storage.close();
  }

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CosmosEnv.getCosmosConfig();
  }

  @Override
  protected String getNamespace() {
    Optional<String> databasePrefix = CosmosEnv.getDatabasePrefix();
    return databasePrefix.map(prefix -> prefix + NAMESPACE).orElse(NAMESPACE);
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return CosmosEnv.getCreateOptions();
  }
}
