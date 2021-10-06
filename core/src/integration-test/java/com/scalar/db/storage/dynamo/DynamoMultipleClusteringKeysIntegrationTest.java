package com.scalar.db.storage.dynamo;

import com.google.common.collect.ImmutableList;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.StorageMultipleClusteringKeysIntegrationTestBase;
import java.util.List;
import java.util.Map;
import jdk.nashorn.internal.ir.annotations.Ignore;

public class DynamoMultipleClusteringKeysIntegrationTest
    extends StorageMultipleClusteringKeysIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return DynamoEnv.getDynamoConfig();
  }

  @Override
  protected List<DataType> getClusteringKeyTypeList() {
    // Return types without BOOLEAN because boolean is not supported for clustering key in Dynamo
    return ImmutableList.of(
        DataType.INT,
        DataType.BIGINT,
        DataType.FLOAT,
        DataType.DOUBLE,
        DataType.TEXT,
        DataType.BLOB);
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return DynamoEnv.getCreateOptions();
  }

  /*
   * Ignore boolean clustering key tests because boolean is not supported for clustering key in Dynamo
   */

  @Ignore
  @Override
  public void scan_WithoutClusteringKeysBooleanAfter_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void
      scan_OnlyWithBeforeClusteringKeyRangeOfValuesBooleanBefore_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void
      scan_OnlyWithBeforeClusteringKeyStartInclusiveRangeOfValuesBooleanBefore_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void
      scan_OnlyWithBeforeClusteringKeyStartExclusiveRangeOfValuesBooleanBefore_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void
      scan_OnlyWithBeforeClusteringKeyEndInclusiveRangeOfValuesBooleanBefore_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void
      scan_OnlyWithBeforeClusteringKeyEndExclusiveRangeOfValuesBooleanBefore_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void scan_WithClusteringKeyRangeOfValuesBooleanAfter_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void
      scan_WithClusteringKeyStartInclusiveRangeOfValuesBooleanAfter_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void
      scan_WithClusteringKeyStartExclusiveRangeOfValuesBooleanAfter_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void
      scan_WithClusteringKeyEndInclusiveRangeOfValuesBooleanAfter_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void
      scan_WithClusteringKeyEndExclusiveRangeOfValuesBooleanAfter_ShouldReturnProperlyResult() {}
}
