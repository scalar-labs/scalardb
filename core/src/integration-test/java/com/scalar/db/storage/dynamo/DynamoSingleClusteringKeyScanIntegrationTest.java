package com.scalar.db.storage.dynamo;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.StorageSingleClusteringKeyScanIntegrationTestBase;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.junit.Ignore;
import org.junit.Test;

public class DynamoSingleClusteringKeyScanIntegrationTest
    extends StorageSingleClusteringKeyScanIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return DynamoEnv.getDynamoConfig();
  }

  @Override
  protected Set<DataType> getClusteringKeyTypes() {
    // Return empty for now
    return Collections.emptySet();
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
  public void scan_WithClusteringKeyRange_ShouldReturnProperResult() {}

  @Ignore
  @Test
  @Override
  public void scan_WithClusteringKeyRangeWithSameValues_ShouldReturnProperResult() {}

  @Ignore
  @Test
  @Override
  public void scan_WithClusteringKeyRangeWithMinAndMaxValue_ShouldReturnProperResult() {}

  @Ignore
  @Test
  @Override
  public void scan_WithClusteringKeyStartRange_ShouldReturnProperResult() {}

  @Ignore
  @Test
  @Override
  public void scan_WithClusteringKeyStartRangeWithMinValue_ShouldReturnProperResult() {}

  @Ignore
  @Test
  @Override
  public void scan_WithClusteringKeyEndRange_ShouldReturnProperResult() {}

  @Ignore
  @Test
  @Override
  public void scan_WithClusteringKeyEndRangeWithMaxValue_ShouldReturnProperResult() {}
}
