package com.scalar.db.storage.cosmos;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;

public class CosmosClientSingleton {
  private static CosmosClient cosmosClient = null;
  private static int references = 0;

  private static void initializeCosmosClient(CosmosConfig config) {
    cosmosClient =
        new CosmosClientBuilder()
            .endpoint(config.getContactPoints().get(0))
            .key(config.getPassword().orElse(null))
            .directMode()
            .consistencyLevel(ConsistencyLevel.STRONG)
            .buildClient();
  }

  public static synchronized CosmosClient getCosmosClient(CosmosConfig config) {
    if (cosmosClient == null) {
      initializeCosmosClient(config);
    }
    references++;

    return cosmosClient;
  }

  public static synchronized void release() {
    if (references == 1) {
      cosmosClient.close();
      cosmosClient = null;
    }
    if (references > 0) {
      references--;
    }
  }
}
