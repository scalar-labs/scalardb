package com.scalar.db.storage.cosmos;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.StorageMultipleClusteringKeysIntegrationTestBase;
import java.util.Map;
import java.util.Optional;
import org.junit.AfterClass;

public class CosmosMultipleClusteringKeysIntegrationTest
    extends StorageMultipleClusteringKeysIntegrationTestBase {

  @Override
  protected void tearUp() throws ExecutionException {
    CosmosConfig cosmosConfig = CosmosEnv.getCosmosConfig();
    Cosmos cosmosStorage = new Cosmos(cosmosConfig);
    storage = cosmosStorage;
    admin = new CosmosAdmin(cosmosStorage.getCosmosClient(), cosmosConfig);

    namespaceBaseName = getNamespaceBaseName();
    clusteringKeyTypes = getClusteringKeyTypes();
    createTables();
    seed = System.currentTimeMillis();
    System.out.println("The seed used in the multiple clustering keys integration test is " + seed);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    deleteTables();
    storage.close();
  }

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CosmosEnv.getCosmosConfig();
  }

  @Override
  protected String getNamespaceBaseName() {
    Optional<String> databasePrefix = CosmosEnv.getDatabasePrefix();
    return databasePrefix.map(prefix -> prefix + NAMESPACE_BASE_NAME).orElse(NAMESPACE_BASE_NAME);
  }

  @Override
  protected ListMultimap<DataType, DataType> getClusteringKeyTypes() {
    // Return types without BLOB because blob is not supported for clustering key for now
    ListMultimap<DataType, DataType> clusteringKeyTypes = ArrayListMultimap.create();
    for (DataType firstClusteringKeyType : DataType.values()) {
      if (firstClusteringKeyType == DataType.BLOB) {
        continue;
      }
      for (DataType secondClusteringKeyType : DataType.values()) {
        if (secondClusteringKeyType == DataType.BLOB) {
          continue;
        }
        clusteringKeyTypes.put(firstClusteringKeyType, secondClusteringKeyType);
      }
    }
    return clusteringKeyTypes;
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return CosmosEnv.getCreateOptions();
  }
}
