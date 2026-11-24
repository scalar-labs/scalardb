package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.azure.cosmos.CosmosStoredProcedure;
import com.azure.cosmos.models.CosmosStoredProcedureProperties;
import com.scalar.db.api.DistributedStorageAdminRepairIntegrationTestBase;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class CosmosAdminRepairIntegrationTest
    extends DistributedStorageAdminRepairIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
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
    admin.repairTable(getNamespace(), getTable(), getTableMetadata(), getCreationOptions());

    // Assert
    assertThatCode(
            () -> cosmosAdminTestUtils.getTableStoredProcedure(getNamespace(), getTable()).read())
        .doesNotThrowAnyException();
  }

  @Test
  public void repairTable_ForTableWithOutdatedStoredProcedure_ShouldReplaceStoredProcedure()
      throws ExecutionException {
    // Arrange
    CosmosStoredProcedure storedProcedure =
        ((CosmosAdminTestUtils) adminTestUtils).getTableStoredProcedure(getNamespace(), getTable());
    CosmosStoredProcedureProperties properties = storedProcedure.read().getProperties();
    String body = properties.getBody();
    properties.setBody("function mutate() {}");
    storedProcedure.replace(properties);

    // Act
    admin.repairTable(getNamespace(), getTable(), getTableMetadata(), getCreationOptions());

    // Assert
    assertThat(storedProcedure.read().getProperties().getBody()).isEqualTo(body);
  }

  @Test
  public void repairTable_ForTableWithUpToDateStoredProcedure_ShouldNotReplaceStoredProcedure()
      throws ExecutionException {
    // Arrange
    CosmosStoredProcedure storedProcedure =
        ((CosmosAdminTestUtils) adminTestUtils).getTableStoredProcedure(getNamespace(), getTable());
    String eTag = storedProcedure.read().getProperties().getETag();

    // Act
    admin.repairTable(getNamespace(), getTable(), getTableMetadata(), getCreationOptions());

    // Assert
    assertThat(storedProcedure.read().getProperties().getETag()).isEqualTo(eTag);
  }
}
