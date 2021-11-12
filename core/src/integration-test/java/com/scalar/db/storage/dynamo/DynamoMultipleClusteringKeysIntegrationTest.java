package com.scalar.db.storage.dynamo;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.StorageMultipleClusteringKeysIntegrationTestBase;
import java.util.Map;

public class DynamoMultipleClusteringKeysIntegrationTest
    extends StorageMultipleClusteringKeysIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return DynamoEnv.getDynamoConfig();
  }

  @Override
  protected ListMultimap<DataType, DataType> getClusteringKeyTypes() {
    // the BLOB type is supported only for the last key
    ListMultimap<DataType, DataType> clusteringKeyTypes = ArrayListMultimap.create();
    for (DataType cKeyTypeBefore : DataType.values()) {
      if (cKeyTypeBefore == DataType.BLOB) {
        continue;
      }
      for (DataType cKeyTypeAfter : DataType.values()) {
        clusteringKeyTypes.put(cKeyTypeBefore, cKeyTypeAfter);
      }
    }
    return clusteringKeyTypes;
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return DynamoEnv.getCreateOptions();
  }
}
