package com.scalar.db.storage.dynamo;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.scalar.db.api.DistributedStorageMultiplePartitionKeyIntegrationTestBase;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Value;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class DynamoMultiplePartitionKeyIntegrationTest
    extends DistributedStorageMultiplePartitionKeyIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return DynamoEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
  }

  @Override
  protected ListMultimap<DataType, DataType> getPartitionKeyTypes() {
    ListMultimap<DataType, DataType> clusteringKeyTypes = ArrayListMultimap.create();
    for (DataType firstClusteringKeyType : DataType.valuesWithoutTimesRelatedTypes()) {
      // BLOB type is supported only for the last value in partition key
      if (firstClusteringKeyType == DataType.BLOB) {
        continue;
      }
      for (DataType secondClusteringKeyType : DataType.valuesWithoutTimesRelatedTypes()) {
        clusteringKeyTypes.put(firstClusteringKeyType, secondClusteringKeyType);
      }
    }
    return clusteringKeyTypes;
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
