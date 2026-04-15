package com.scalar.db.storage.objectstorage;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.api.DistributedStorageColumnValueIntegrationTestBase;
import com.scalar.db.api.Put;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import java.util.Properties;
import java.util.Random;
import org.junit.jupiter.api.Test;

public class ObjectStorageColumnValueIntegrationTest
    extends DistributedStorageColumnValueIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }

  @Override
  protected Column<?> getColumnWithMaxValue(String columnName, DataType dataType) {
    if (dataType == DataType.BIGINT) {
      return BigIntColumn.of(columnName, ObjectStorageTestUtils.BIGINT_MAX_VALUE);
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

  @Test
  public void put_WithBigIntValueGreaterThanUpperLimit_ShouldThrowIllegalArgumentException() {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(Key.ofInt(PARTITION_KEY, 1))
            .value(BigIntColumn.of(COL_NAME3, ObjectStorageTestUtils.BIGINT_MAX_VALUE + 1))
            .build();

    // Act Assert
    assertThatThrownBy(() -> storage.put(put)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void put_WithBigIntValueLessThanLowerLimit_ShouldThrowIllegalArgumentException() {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(Key.ofInt(PARTITION_KEY, 1))
            .value(BigIntColumn.of(COL_NAME3, ObjectStorageTestUtils.BIGINT_MIN_VALUE - 1))
            .build();

    // Act Assert
    assertThatThrownBy(() -> storage.put(put)).isInstanceOf(IllegalArgumentException.class);
  }
}
