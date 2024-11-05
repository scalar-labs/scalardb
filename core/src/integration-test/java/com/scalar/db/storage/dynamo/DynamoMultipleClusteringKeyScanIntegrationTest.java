package com.scalar.db.storage.dynamo;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.scalar.db.api.DistributedStorageMultipleClusteringKeyScanIntegrationTestBase;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Value;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class DynamoMultipleClusteringKeyScanIntegrationTest
    extends DistributedStorageMultipleClusteringKeyScanIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return DynamoEnv.getProperties(testName);
  }

  @Override
  protected ListMultimap<DataType, DataType> getClusteringKeyTypes() {
    // Return types without BLOB because blob is not supported for clustering key for now
    ListMultimap<DataType, DataType> clusteringKeyTypes = ArrayListMultimap.create();
    for (DataType firstClusteringKeyType : DataType.valuesWithoutTimesRelatedTypes()) {
      if (firstClusteringKeyType == DataType.BLOB) {
        continue;
      }
      for (DataType secondClusteringKeyType : DataType.valuesWithoutTimesRelatedTypes()) {
        if (secondClusteringKeyType == DataType.BLOB) {
          continue;
        }
        clusteringKeyTypes.put(firstClusteringKeyType, secondClusteringKeyType);
      }
    }
    return clusteringKeyTypes;
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
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
