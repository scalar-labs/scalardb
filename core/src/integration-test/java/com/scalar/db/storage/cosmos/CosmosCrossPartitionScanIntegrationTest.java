package com.scalar.db.storage.cosmos;

import com.scalar.db.api.DistributedStorageCrossPartitionScanIntegrationTestBase;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class CosmosCrossPartitionScanIntegrationTest
    extends DistributedStorageCrossPartitionScanIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return CosmosEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return CosmosEnv.getCreationOptions();
  }

  @Override
  protected int getThreadNum() {
    return 3;
  }

  @Override
  protected boolean isParallelDdlSupported() {
    return false;
  }

  @Override
  protected Column<?> getRandomColumn(Random random, String columnName, DataType dataType) {
    if (dataType == DataType.BIGINT) {
      long value =
          random
              .longs(1, CosmosTestUtils.BIGINT_MIN_VALUE, CosmosTestUtils.BIGINT_MAX_VALUE + 1)
              .findFirst()
              .orElse(0);
      return BigIntColumn.of(columnName, value);
    }
    return super.getRandomColumn(random, columnName, dataType);
  }

  @Test
  @Override
  @Disabled("Cross partition scan with ordering is not supported in Cosmos DB")
  public void scan_WithOrderingForNonPrimaryColumns_ShouldReturnProperResult() {}
}
