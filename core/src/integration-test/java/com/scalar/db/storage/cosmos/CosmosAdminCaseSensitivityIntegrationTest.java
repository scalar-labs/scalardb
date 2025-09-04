package com.scalar.db.storage.cosmos;

import com.scalar.db.api.DistributedStorageAdminCaseSensitivityIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class CosmosAdminCaseSensitivityIntegrationTest
    extends DistributedStorageAdminCaseSensitivityIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return CosmosEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return CosmosEnv.getCreationOptions();
  }

  @Override
  protected String getSystemNamespaceName(Properties properties) {
    return new CosmosConfig(new DatabaseConfig(properties))
        .getTableMetadataDatabase()
        .orElse(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
  }

  @Disabled("Cosmos DB does not support dropping columns")
  @Override
  public void dropColumnFromTable_DropColumnForEachExistingDataType_ShouldDropColumnsCorrectly() {}

  @Disabled("Cosmos DB does not support dropping columns")
  @Override
  public void dropColumnFromTable_ForNonExistingTable_ShouldThrowIllegalArgumentException() {}

  @Disabled("Cosmos DB does not support dropping columns")
  @Override
  public void dropColumnFromTable_ForNonExistingColumn_ShouldThrowIllegalArgumentException() {}

  @Disabled("Cosmos DB does not support dropping columns")
  @Override
  public void dropColumnFromTable_ForPrimaryKeyColumn_ShouldThrowIllegalArgumentException() {}

  @Disabled("Cosmos DB does not support dropping columns")
  @Override
  public void dropColumnFromTable_ForIndexedColumn_ShouldDropColumnAndIndexCorrectly() {}

  @Disabled("Cosmos DB does not support dropping columns")
  @Override
  public void dropColumnFromTable_IfExists_ForNonExistingColumn_ShouldNotThrowAnyException() {}
}
