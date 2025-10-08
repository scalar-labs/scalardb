package com.scalar.db.storage.cosmos;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminIntegrationTestBase;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class ConsensusCommitAdminIntegrationTestWithCosmos
    extends ConsensusCommitAdminIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return ConsensusCommitCosmosEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return ConsensusCommitCosmosEnv.getCreationOptions();
  }

  @Override
  protected String getSystemNamespaceName(Properties properties) {
    return new CosmosConfig(new DatabaseConfig(properties))
        .getTableMetadataDatabase()
        .orElse(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
  }

  @Override
  protected String getCoordinatorNamespaceName(String testName) {
    return new ConsensusCommitConfig(new DatabaseConfig(getProperties(testName)))
        .getCoordinatorNamespace()
        .orElse(Coordinator.NAMESPACE);
  }

  @Override
  protected boolean isGroupCommitEnabled(String testName) {
    return new ConsensusCommitConfig(new DatabaseConfig(getProperties(testName)))
        .isCoordinatorGroupCommitEnabled();
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
  public void dropColumnFromTable_IfNotExists_ForNonExistingColumn_ShouldNotThrowAnyException() {}

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
  @Disabled("Cosmos DB does not support altering column types")
  public void
      alterColumnType_AlterColumnTypeFromEachExistingDataTypeToText_ShouldAlterColumnTypesCorrectly() {}

  @Override
  @Disabled("Cosmos DB does not support altering column types")
  public void alterColumnType_WideningConversion_ShouldAlterColumnTypesCorrectly() {}

  @Override
  @Disabled("Cosmos DB does not support altering column types")
  public void alterColumnType_ForPrimaryKeyOrIndexKeyColumn_ShouldThrowIllegalArgumentException() {}

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
