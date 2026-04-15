package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageConditionalMutationIntegrationTestBase;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import java.util.Properties;
import java.util.Random;

public class ObjectStorageConditionalMutationIntegrationTest
    extends DistributedStorageConditionalMutationIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
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
