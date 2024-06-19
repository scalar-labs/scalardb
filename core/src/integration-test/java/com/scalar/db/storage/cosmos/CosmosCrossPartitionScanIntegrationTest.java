package com.scalar.db.storage.cosmos;

import com.scalar.db.api.DistributedStorageCrossPartitionScanIntegrationTestBase;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class CosmosCrossPartitionScanIntegrationTest
    extends DistributedStorageCrossPartitionScanIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return CosmosEnv.getProperties(testName);
  }

  @Override
  protected String getNamespaceBaseName() {
    String namespaceBaseName = super.getNamespaceBaseName();
    Optional<String> databasePrefix = CosmosEnv.getDatabasePrefix();
    return databasePrefix.map(prefix -> prefix + namespaceBaseName).orElse(namespaceBaseName);
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

  @Test
  @Override
  @Disabled("Cross partition scan with ordering is not supported in Cosmos DB")
  public void scan_WithOrderingForNonPrimaryColumns_ShouldReturnProperResult() {}
}
