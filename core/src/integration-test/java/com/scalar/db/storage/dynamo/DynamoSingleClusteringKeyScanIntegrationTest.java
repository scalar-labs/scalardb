package com.scalar.db.storage.dynamo;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Value;
import com.scalar.db.storage.StorageSingleClusteringKeyScanIntegrationTestBase;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class DynamoSingleClusteringKeyScanIntegrationTest
    extends StorageSingleClusteringKeyScanIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return DynamoEnv.getDynamoConfig();
  }

  @Override
  protected Set<DataType> getClusteringKeyTypes() {
    // Return types without BLOB because blob is not supported for clustering key for now
    Set<DataType> clusteringKeyTypes = new HashSet<>();
    for (DataType dataType : DataType.values()) {
      if (dataType == DataType.BLOB) {
        continue;
      }
      clusteringKeyTypes.add(dataType);
    }
    return clusteringKeyTypes;
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return DynamoEnv.getCreateOptions();
  }

  @Override
  protected Value<?> getRandomValue(Random random, String columnName, DataType dataType) {
    if (dataType == DataType.DOUBLE) {
      return DynamoTestUtils.getRandomDynamoDoubleValue(random, columnName);
    }
    return super.getRandomValue(random, columnName, dataType);
  }

  @Override
  protected Value<?> getMinValue(String columnName, DataType dataType) {
    if (dataType == DataType.DOUBLE) {
      return DynamoTestUtils.getMinDynamoDoubleValue(columnName);
    }
    return super.getMinValue(columnName, dataType);
  }

  @Override
  protected Value<?> getMaxValue(String columnName, DataType dataType) {
    if (dataType == DataType.DOUBLE) {
      return DynamoTestUtils.getMaxDynamoDoubleValue(columnName);
    }
    return super.getMaxValue(columnName, dataType);
  }
}
