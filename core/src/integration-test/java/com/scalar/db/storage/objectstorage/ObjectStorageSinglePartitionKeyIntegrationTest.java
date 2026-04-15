package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageSinglePartitionKeyIntegrationTestBase;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.TextColumn;
import com.scalar.db.util.TestUtils;
import java.util.Properties;
import java.util.Random;
import java.util.stream.IntStream;

public class ObjectStorageSinglePartitionKeyIntegrationTest
    extends DistributedStorageSinglePartitionKeyIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }

  @Override
  protected Column<?> getColumnWithMaxValue(String columnName, DataType dataType) {
    if (dataType == DataType.BIGINT) {
      return BigIntColumn.of(columnName, ObjectStorageTestUtils.BIGINT_MAX_VALUE);
    }
    if (dataType == DataType.TEXT && ObjectStorageEnv.isCloudStorage()) {
      // Since Cloud Storage can't handle 0xFF character correctly, we use "ZZZ..." as the max value
      StringBuilder builder = new StringBuilder();
      IntStream.range(0, TestUtils.MAX_TEXT_COUNT).forEach(i -> builder.append('Z'));
      return TextColumn.of(columnName, builder.toString());
    }
    return super.getColumnWithMaxValue(columnName, dataType);
  }

  @Override
  protected Column<?> getColumnWithMinValue(String columnName, DataType dataType) {
    if (dataType == DataType.BIGINT) {
      return BigIntColumn.of(columnName, ObjectStorageTestUtils.BIGINT_MIN_VALUE);
    }
    return super.getColumnWithMinValue(columnName, dataType);
  }

  @Override
  protected Column<?> getColumnWithRandomValue(
      Random random, String columnName, DataType dataType) {
    if (dataType == DataType.BIGINT) {
      long value =
          random
              .longs(
                  1,
                  ObjectStorageTestUtils.BIGINT_MIN_VALUE,
                  ObjectStorageTestUtils.BIGINT_MAX_VALUE + 1)
              .findFirst()
              .orElse(0);
      return BigIntColumn.of(columnName, value);
    }
    return super.getColumnWithRandomValue(random, columnName, dataType);
  }
}
