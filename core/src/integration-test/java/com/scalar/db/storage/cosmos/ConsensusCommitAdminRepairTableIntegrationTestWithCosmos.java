package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThatCode;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminRepairTableIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class ConsensusCommitAdminRepairTableIntegrationTestWithCosmos
    extends ConsensusCommitAdminRepairTableIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return CosmosEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return CosmosEnv.getCreationOptions();
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new CosmosAdminTestUtils(getProperties(testName));
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
}
