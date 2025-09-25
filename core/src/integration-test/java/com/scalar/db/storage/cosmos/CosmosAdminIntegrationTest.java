package com.scalar.db.storage.cosmos;

import com.scalar.db.api.DistributedStorageAdminIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class CosmosAdminIntegrationTest extends DistributedStorageAdminIntegrationTestBase {

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

  @Override
  @Disabled("Cosmos DB does not support dropping columns")
  public void dropColumnFromTable_DropColumnForEachExistingDataType_ShouldDropColumnsCorrectly() {}

  @Override
  @Disabled("Cosmos DB does not support dropping columns")
  public void dropColumnFromTable_ForNonExistingTable_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Cosmos DB does not support dropping columns")
  public void dropColumnFromTable_ForNonExistingColumn_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Cosmos DB does not support dropping columns")
  public void dropColumnFromTable_ForPrimaryKeyColumn_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Cosmos DB does not support dropping columns")
  public void dropColumnFromTable_ForIndexedColumn_ShouldDropColumnAndIndexCorrectly() {}

  @Override
  @Disabled("Cosmos DB does not support dropping columns")
  public void dropColumnFromTable_IfExists_ForNonExistingColumn_ShouldNotThrowAnyException() {}

  @Override
  @Disabled("Cosmos DB does not support renaming columns")
  public void renameColumn_ShouldRenameColumnCorrectly() {}

  @Override
  @Disabled("Cosmos DB does not support renaming columns")
  public void renameColumn_ForNonExistingTable_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Cosmos DB does not support renaming columns")
  public void renameColumn_ForNonExistingColumn_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Cosmos DB does not support renaming columns")
  public void renameColumn_ForPrimaryKeyColumn_ShouldRenameColumnCorrectly() {}

  @Override
  @Disabled("Cosmos DB does not support renaming columns")
  public void renameColumn_ForIndexKeyColumn_ShouldRenameColumnAndIndexCorrectly() {}

  @Override
  @Disabled("Cosmos DB does not support renaming tables")
  public void renameTable_ForExistingTable_ShouldRenameTableCorrectly() {}

  @Override
  @Disabled("Cosmos DB does not support renaming tables")
  public void renameTable_ForNonExistingTable_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Cosmos DB does not support renaming tables")
  public void renameTable_IfNewTableNameAlreadyExists_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Cosmos DB does not support renaming tables")
  public void renameTable_ForExistingTableWithIndexes_ShouldRenameTableAndIndexesCorrectly() {}
}
