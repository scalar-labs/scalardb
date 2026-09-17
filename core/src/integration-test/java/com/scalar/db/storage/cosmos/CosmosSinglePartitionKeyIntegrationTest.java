package com.scalar.db.storage.cosmos;

import com.scalar.db.api.DistributedStorageSinglePartitionKeyIntegrationTestBase;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class CosmosSinglePartitionKeyIntegrationTest
    extends DistributedStorageSinglePartitionKeyIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return CosmosEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return CosmosEnv.getCreationOptions();
  }

  @Override
  protected Column<?> getColumnWithMaxValue(String columnName, DataType dataType) {
    if (dataType == DataType.BIGINT) {
      return BigIntColumn.of(columnName, CosmosTestUtils.BIGINT_MAX_VALUE);
    }
    return super.getColumnWithMaxValue(columnName, dataType);
  }

  @Override
  protected Column<?> getColumnWithMinValue(String columnName, DataType dataType) {
    if (dataType == DataType.BIGINT) {
      return BigIntColumn.of(columnName, CosmosTestUtils.BIGINT_MIN_VALUE);
    }
    return super.getColumnWithMinValue(columnName, dataType);
  }

  @Override
  protected Column<?> getColumnWithRandomValue(
      Random random, String columnName, DataType dataType) {
    if (dataType == DataType.BIGINT) {
      long value =
          random
              .longs(1, CosmosTestUtils.BIGINT_MIN_VALUE, CosmosTestUtils.BIGINT_MAX_VALUE + 1)
              .findFirst()
              .orElse(0);
      return BigIntColumn.of(columnName, value);
    }
    return super.getColumnWithRandomValue(random, columnName, dataType);
  }
}
