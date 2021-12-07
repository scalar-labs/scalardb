package com.scalar.db.storage.dynamo;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Value;
import com.scalar.db.storage.StorageSecondaryIndexIntegrationTestBase;
import com.scalar.db.storage.TestUtils;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class DynamoSecondaryIndexIntegrationTest extends StorageSecondaryIndexIntegrationTestBase {
  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return DynamoEnv.getDynamoConfig();
  }

  @Override
  protected Set<DataType> getSecondaryIndexTypes() {
    // Return types without BOOLEAN because boolean is not supported for secondary index for now
    Set<DataType> clusteringKeyTypes = new HashSet<>();
    for (DataType dataType : DataType.values()) {
      if (dataType == DataType.BOOLEAN) {
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
    // don't allow empty value since secondary index cannot contain empty value
    return TestUtils.getRandomValue(random, columnName, dataType, false);
  }

  @Override
  protected Value<?> getMinValue(String columnName, DataType dataType) {
    if (dataType == DataType.DOUBLE) {
      return DynamoTestUtils.getMinDynamoDoubleValue(columnName);
    }
    // don't allow empty value since secondary index cannot contain empty value
    return TestUtils.getMinValue(columnName, dataType, false);
  }

  @Override
  protected Value<?> getMaxValue(String columnName, DataType dataType) {
    if (dataType == DataType.DOUBLE) {
      return DynamoTestUtils.getMaxDynamoDoubleValue(columnName);
    }
    return super.getMaxValue(columnName, dataType);
  }
}
