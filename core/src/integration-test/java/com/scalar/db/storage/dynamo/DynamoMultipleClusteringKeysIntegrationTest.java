package com.scalar.db.storage.dynamo;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.StorageMultipleClusteringKeysIntegrationTestBase;
import java.util.Map;
import org.junit.Ignore;
import org.junit.Test;

public class DynamoMultipleClusteringKeysIntegrationTest
    extends StorageMultipleClusteringKeysIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return DynamoEnv.getDynamoConfig();
  }

  @Override
  protected ListMultimap<DataType, DataType> getClusteringKeyTypes() {
    // Return empty for now
    return ArrayListMultimap.create();
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return DynamoEnv.getCreateOptions();
  }

  // Ignore all tests for now. We will have them back after the sort key optimization

  @Ignore
  @Test
  @Override
  public void scan_WithoutClusteringKeyRange_ShouldReturnProperResult() {}

  @Ignore
  @Test
  @Override
  public void scan_WithFirstClusteringKeyRange_ShouldReturnProperResult() {}

  @Ignore
  @Test
  @Override
  public void scan_WithFirstClusteringKeyRangeWithSameValues_ShouldReturnProperResult() {}

  @Ignore
  @Test
  @Override
  public void scan_WithFirstClusteringKeyRangeWithMinAndMaxValue_ShouldReturnProperResult() {}

  @Ignore
  @Test
  @Override
  public void scan_WithFirstClusteringKeyStartRange_ShouldReturnProperResult() {}

  @Ignore
  @Test
  @Override
  public void scan_WithFirstClusteringKeyStartRangeWithMinValue_ShouldReturnProperResult() {}

  @Ignore
  @Test
  @Override
  public void scan_WithFirstClusteringKeyEndRange_ShouldReturnProperResult() {}

  @Ignore
  @Test
  @Override
  public void scan_WithFirstClusteringKeyEndRangeWithMaxValue_ShouldReturnProperResult() {}

  @Ignore
  @Test
  @Override
  public void scan_WithSecondClusteringKeyRange_ShouldReturnProperResult() {}

  @Ignore
  @Test
  @Override
  public void scan_WithSecondClusteringKeyRangeWithSameValues_ShouldReturnProperResult() {}

  @Ignore
  @Test
  @Override
  public void scan_WithSecondClusteringKeyRangeWithMinAndMaxValues_ShouldReturnProperResult() {}

  @Ignore
  @Test
  @Override
  public void scan_WithSecondClusteringKeyStartRange_ShouldReturnProperResult() {}

  @Ignore
  @Test
  @Override
  public void scan_WithSecondClusteringKeyStartRangeWithMinValue_ShouldReturnProperResult() {}

  @Ignore
  @Test
  @Override
  public void scan_WithSecondClusteringKeyEndRange_ShouldReturnProperResult() {}

  @Ignore
  @Test
  @Override
  public void scan_WithSecondClusteringKeyEndRangeWithMaxValue_ShouldReturnProperResult() {}
}
