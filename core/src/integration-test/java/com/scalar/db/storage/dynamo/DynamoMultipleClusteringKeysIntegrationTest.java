package com.scalar.db.storage.dynamo;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.Value;
import com.scalar.db.storage.StorageMultipleClusteringKeysIntegrationTestBase;
import java.util.Map;
import java.util.Random;

public class DynamoMultipleClusteringKeysIntegrationTest
    extends StorageMultipleClusteringKeysIntegrationTestBase {

  private static final double DYNAMO_DOUBLE_MAX_VALUE = 9.99999999999999E125D;
  private static final double DYNAMO_DOUBLE_MIN_VALUE = -9.99999999999999E125D;

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return DynamoEnv.getDynamoConfig();
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
    return DynamoEnv.getCreateOptions();
  }

  @Override
  protected Value<?> getRandomValue(Random random, String columnName, DataType dataType) {
    if (dataType == DataType.DOUBLE) {
      return new DoubleValue(columnName, nextDynamoDouble(random));
    }
    return super.getRandomValue(random, columnName, dataType);
  }

  private double nextDynamoDouble(Random random) {
    return random
        .doubles(DYNAMO_DOUBLE_MIN_VALUE, DYNAMO_DOUBLE_MAX_VALUE)
        .limit(1)
        .findFirst()
        .orElse(0.0d);
  }

  @Override
  protected Value<?> getMinValue(String columnName, DataType dataType) {
    if (dataType == DataType.DOUBLE) {
      return new DoubleValue(columnName, DYNAMO_DOUBLE_MAX_VALUE);
    }
    return super.getMinValue(columnName, dataType);
  }

  @Override
  protected Value<?> getMaxValue(String columnName, DataType dataType) {
    if (dataType == DataType.DOUBLE) {
      return new DoubleValue(columnName, DYNAMO_DOUBLE_MIN_VALUE);
    }
    return super.getMaxValue(columnName, dataType);
  }
}
