package com.scalar.db.storage.cosmos;

import com.scalar.db.api.DistributedStorageIntegrationTestBase;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class CosmosIntegrationTest extends DistributedStorageIntegrationTestBase {

  @Override
  protected Properties getProperties() {
    return CosmosEnv.getProperties();
  }

  @Override
  protected String getNamespace() {
    String namespace = super.getNamespace();
    Optional<String> databasePrefix = CosmosEnv.getDatabasePrefix();
    return databasePrefix.map(prefix -> prefix + namespace).orElse(namespace);
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return CosmosEnv.getCreateOptions();
  }

  @Override
  @Disabled("ScanAll is not yet implemented for Cosmos DB")
  public void scan_ScanAllWithNoLimitGiven_ShouldRetrieveAllRecords() {}

  @Override
  @Disabled("ScanAll is not yet implemented for Cosmos DB")
  public void scan_ScanAllWithLimitGiven_ShouldRetrieveExpectedRecords() {}

  @Override
  @Disabled("ScanAll is not yet implemented for Cosmos DB")
  public void scan_ScanAllWithProjectionsGiven_ShouldRetrieveSpecifiedValues() {}

  @Override
  @Disabled("ScanAll is not yet implemented for Cosmos DB")
  public void scan_ScanAllWithLargeData_ShouldRetrieveExpectedValues() {}
}
