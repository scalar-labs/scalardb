package com.scalar.db.storage.cosmos;

import com.google.common.collect.ImmutableList;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.StorageMultipleClusteringKeysIntegrationTestBase;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import jdk.nashorn.internal.ir.annotations.Ignore;

public class CosmosMultipleClusteringKeysIntegrationTest
    extends StorageMultipleClusteringKeysIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CosmosEnv.getCosmosConfig();
  }

  @Override
  protected String getNamespaceBaseName() {
    Optional<String> databasePrefix = CosmosEnv.getDatabasePrefix();
    return databasePrefix.map(prefix -> prefix + NAMESPACE_BASE_NAME).orElse(NAMESPACE_BASE_NAME);
  }

  @Override
  protected List<DataType> getClusteringKeyTypeList() {
    // Return types without BLOB because blob is not supported for clustering key in Cosmos
    return ImmutableList.of(
        DataType.BOOLEAN,
        DataType.INT,
        DataType.BIGINT,
        DataType.FLOAT,
        DataType.DOUBLE,
        DataType.TEXT);
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return CosmosEnv.getCreateOptions();
  }

  /*
   * Ignore blob clustering key tests because blob is not supported for clustering key in Dynamo
   */

  @Ignore
  @Override
  public void scan_WithoutClusteringKeysBlobAfter_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void
      scan_OnlyWithBeforeClusteringKeyRangeOfValuesBlobBefore_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void
      scan_OnlyWithBeforeClusteringKeyStartInclusiveRangeOfValuesBlobBefore_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void
      scan_OnlyWithBeforeClusteringKeyStartExclusiveRangeOfValuesBlobBefore_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void
      scan_OnlyWithBeforeClusteringKeyEndInclusiveRangeOfValuesBlobBefore_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void
      scan_OnlyWithBeforeClusteringKeyEndExclusiveRangeOfValuesBlobBefore_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void scan_WithClusteringKeyRangeOfValuesBlobAfter_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void
      scan_WithClusteringKeyStartInclusiveRangeOfValuesBlobAfter_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void
      scan_WithClusteringKeyStartExclusiveRangeOfValuesBlobAfter_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void
      scan_WithClusteringKeyEndInclusiveRangeOfValuesBlobAfter_ShouldReturnProperlyResult() {}

  @Ignore
  @Override
  public void
      scan_WithClusteringKeyEndExclusiveRangeOfValuesBlobAfter_ShouldReturnProperlyResult() {}
}
