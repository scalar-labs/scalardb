package com.scalar.db.storage.dynamo;

import com.scalar.db.api.DistributedStorageSingleClusteringKeyScanIntegrationTestBase;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Value;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

public class DynamoSingleClusteringKeyScanIntegrationTest
    extends DistributedStorageSingleClusteringKeyScanIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return DynamoEnv.getProperties(testName);
  }

  @Override
  protected Set<DataType> getClusteringKeyTypes() {
    // Return types without BLOB because blob is not supported for clustering key for now
    Set<DataType> clusteringKeyTypes = new HashSet<>();
    for (DataType dataType : DataType.valuesWithoutTimesRelatedTypes()) {
      if (dataType == DataType.BLOB) {
        continue;
      }
      clusteringKeyTypes.add(dataType);
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
