package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThatCode;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminRepairIntegrationTestBase;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class ConsensusCommitAdminRepairIntegrationTestWithCosmos
    extends ConsensusCommitAdminRepairIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return CosmosEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return CosmosEnv.getCreationOptions();
  }

  @Override
  protected void initialize(String testName) throws Exception {
    super.initialize(testName);
    adminTestUtils = new CosmosAdminTestUtils(getProperties(testName));
  }

  @Test
  public void repairTable_ForTableWithoutStoredProcedure_ShouldCreateStoredProcedure()
      throws ExecutionException {
    // Arrange
    CosmosAdminTestUtils cosmosAdminTestUtils = (CosmosAdminTestUtils) adminTestUtils;
    cosmosAdminTestUtils.getTableStoredProcedure(getNamespace(), getTable()).delete();

    // Act
    admin.repairTable(getNamespace(), getTable(), TABLE_METADATA, getCreationOptions());

    // Assert
    assertThatCode(
            () -> cosmosAdminTestUtils.getTableStoredProcedure(getNamespace(), getTable()).read())
        .doesNotThrowAnyException();
  }

  @Disabled(
      "Inconsistency check for the raw table schema and the ScalarDB metadata isn't executed in schemaless database/storage")
  @Override
  public void repairCoordinatorTables_OldSchemaCoordinatorTablesExist_ShouldFail() {}
}
