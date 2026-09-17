package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.api.DistributedStorageColumnValueIntegrationTestBase;
import com.scalar.db.api.Put;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.Arguments;

public class CosmosColumnValueIntegrationTest
    extends DistributedStorageColumnValueIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return CosmosEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return CosmosEnv.getCreationOptions();
  }

  @Override
  protected Stream<Arguments> provideLargeBlobSizes() {
    // Cosmos has a maximum item size of 2MB. Even though the BLOB data inserted is 1.5MB, when the
    // full record is serialized to JSON with UTF8 encoding, its approximate size is 2MB.
    return Stream.of(Arguments.of(1_500_000, "1.5 MB"));
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

  @Test
  public void put_WithBigIntValueGreaterThanUpperLimit_ShouldThrowIllegalArgumentException() {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(Key.ofInt(PARTITION_KEY, 1))
            .value(BigIntColumn.of(COL_NAME3, CosmosTestUtils.BIGINT_MAX_VALUE + 1))
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
            .value(BigIntColumn.of(COL_NAME3, CosmosTestUtils.BIGINT_MIN_VALUE - 1))
            .build();

    // Act Assert
    assertThatThrownBy(() -> storage.put(put)).isInstanceOf(IllegalArgumentException.class);
  }
}
