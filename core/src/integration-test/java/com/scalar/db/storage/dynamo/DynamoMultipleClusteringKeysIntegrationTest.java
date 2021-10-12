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
      scan_WithBeforeClusteringKeyInclusiveRangeBooleanBefore_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void
      scan_WithBeforeClusteringKeyExclusiveRangeBooleanBefore_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void
      scan_WithBeforeClusteringKeyStartInclusiveRangeBooleanBefore_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void
      scan_WithBeforeClusteringKeyStartExclusiveRangeBooleanBefore_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void
      scan_WithBeforeClusteringKeyEndInclusiveRangeBooleanBefore_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void
      scan_WithBeforeClusteringKeyEndExclusiveRangeBooleanBefore_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void scan_WithClusteringKeyInclusiveRangeBooleanAfter_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void scan_WithClusteringKeyExclusiveRangeBooleanAfter_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void scan_WithClusteringKeyStartInclusiveRangeBooleanAfter_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void scan_WithClusteringKeyStartExclusiveRangeBooleanAfter_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void scan_WithClusteringKeyEndInclusiveRangeBooleanAfter_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void scan_WithClusteringKeyEndExclusiveRangeBooleanAfter_ShouldReturnProperlyResult() {}

  /*
   * Ignore tests for scan with exclusive range because DynamoDB does not support it
   */

  @Ignore
  @Override
  public void scan_WithBeforeClusteringKeyExclusiveRangeBigIntBefore_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void scan_WithBeforeClusteringKeyExclusiveRangeBlobBefore_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void scan_WithBeforeClusteringKeyExclusiveRangeDoubleBefore_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void scan_WithBeforeClusteringKeyExclusiveRangeFloatBefore_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void scan_WithBeforeClusteringKeyExclusiveRangeIntBefore_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void scan_WithBeforeClusteringKeyExclusiveRangeTextBefore_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void scan_WithClusteringKeyExclusiveRangeBigIntAfter_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void scan_WithClusteringKeyExclusiveRangeBlobAfter_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void scan_WithClusteringKeyExclusiveRangeDoubleAfter_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void scan_WithClusteringKeyExclusiveRangeFloatAfter_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void scan_WithClusteringKeyExclusiveRangeIntAfter_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void scan_WithClusteringKeyExclusiveRangeTextAfter_ShouldReturnProperlyResult() {}
}
