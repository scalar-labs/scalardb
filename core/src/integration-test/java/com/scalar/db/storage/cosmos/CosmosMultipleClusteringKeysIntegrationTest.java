package com.scalar.db.storage.cosmos;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.StorageMultipleClusteringKeysIntegrationTestBase;
import java.util.Map;
import java.util.Optional;

public class CosmosMultipleClusteringKeysIntegrationTest
    extends StorageMultipleClusteringKeysIntegrationTestBase {

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
    // Return types without BLOB because blob is not supported for clustering key
    ListMultimap<DataType, DataType> clusteringKeyTypes = ArrayListMultimap.create();
    for (DataType cKeyTypeBefore : DataType.values()) {
      if (cKeyTypeBefore == DataType.BLOB) {
        continue;
      }
      for (DataType cKeyTypeAfter : DataType.values()) {
        if (cKeyTypeAfter == DataType.BLOB) {
          continue;
        }
        clusteringKeyTypes.put(cKeyTypeBefore, cKeyTypeAfter);
      }
    }
    return clusteringKeyTypes;
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return CosmosEnv.getCreateOptions();
  }
}
