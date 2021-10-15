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

  // Ignore tests for scan with exclusive range because DynamoDB does not support it
  @Ignore
  @Override
  public void scan_WithBeforeClusteringKeyExclusiveRange_ShouldReturnProperResult() {}

  @Ignore
  @Override
  public void scan_WithClusteringKeyExclusiveRange_ShouldReturnProperResult() {}
}
