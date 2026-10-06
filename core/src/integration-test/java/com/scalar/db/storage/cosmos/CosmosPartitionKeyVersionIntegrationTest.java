package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.PartitionKeyDefinition;
import com.azure.cosmos.models.PartitionKeyDefinitionVersion;
import com.azure.cosmos.models.PartitionKind;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.service.StorageFactory;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/** Integration tests for Cosmos DB partition key definition V2 on containers ScalarDB creates. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CosmosPartitionKeyVersionIntegrationTest {

  private static final String TEST_NAME = "pk_version";
  private static final String NAMESPACE = "int_test_" + TEST_NAME;
  private static final String PK = "pk";
  private static final String VALUE = "value";

  private DistributedStorageAdmin admin;
  private CosmosAdminTestUtils adminTestUtils;
  private CosmosClient cosmosClient;
  private Properties properties;

  @BeforeAll
  void beforeAll() throws Exception {
    properties = CosmosEnv.getProperties(TEST_NAME);
    StorageFactory factory = StorageFactory.create(properties);
    admin = factory.getStorageAdmin();
    adminTestUtils = new CosmosAdminTestUtils(properties);
    cosmosClient = CosmosUtils.buildCosmosClient(new CosmosConfig(new DatabaseConfig(properties)));
    resetNamespace();
  }

  private void resetNamespace() throws ExecutionException {
    dropPhysicalDatabaseIfPresent();
    if (admin.namespaceExists(NAMESPACE)) {
      try {
        admin.dropNamespace(NAMESPACE);
      } catch (Exception ignored) {
        // Physical database was already dropped; repairNamespace restores metadata.
      }
    }
    admin.repairNamespace(NAMESPACE, getCreationOptions());
  }

  private void dropPhysicalDatabaseIfPresent() {
    if (adminTestUtils.namespaceExists(NAMESPACE)) {
      adminTestUtils.dropNamespace(NAMESPACE);
    }
  }

  @AfterAll
  void afterAll() throws Exception {
    try {
      if (admin != null) {
        admin.dropNamespace(NAMESPACE);
      }
    } catch (Exception e) {
      // namespace may already be dropped per test
    }
    if (cosmosClient != null) {
      cosmosClient.close();
    }
    if (adminTestUtils != null) {
      adminTestUtils.close();
    }
    if (admin != null) {
      admin.close();
    }
  }

  private Map<String, String> getCreationOptions() {
    return CosmosEnv.getCreationOptions();
  }

  @Test
  void createTable_shouldUsePartitionKeyDefinitionV2() throws ExecutionException {
    String table = "user_table_v2";
    TableMetadata metadata = textPartitionKeyMetadata();

    try {
      admin.createTable(NAMESPACE, table, metadata, getCreationOptions());
      assertV2Container(table);

      CosmosConfig config = new CosmosConfig(new DatabaseConfig(properties));
      Optional<PartitionKeyDefinitionVersion> metadataVersion =
          readPartitionKeyVersion(
              cosmosClient, config.getMetadataDatabase(), CosmosAdmin.TABLE_METADATA_CONTAINER);
      assertThat(metadataVersion).contains(PartitionKeyDefinitionVersion.V2);
    } finally {
      dropTableQuietly(table);
    }
  }

  @Test
  void namespacesContainer_shouldUsePartitionKeyDefinitionV2() {
    CosmosConfig config = new CosmosConfig(new DatabaseConfig(properties));
    Optional<PartitionKeyDefinitionVersion> namespacesVersion =
        readPartitionKeyVersion(
            cosmosClient, config.getMetadataDatabase(), CosmosAdmin.NAMESPACES_CONTAINER);
    assertThat(namespacesVersion).contains(PartitionKeyDefinitionVersion.V2);
  }

  @Test
  void repairTable_shouldNotUpgradePartitionKeyVersionFromV1() throws ExecutionException {
    String table = "existing_v1";
    TableMetadata metadata = textPartitionKeyMetadata();

    try {
      createPhysicalV1Container(table);
      Optional<PartitionKeyDefinitionVersion> versionBefore =
          readPartitionKeyVersion(cosmosClient, NAMESPACE, table);
      assertThat(isV1OrUnset(versionBefore)).isTrue();

      admin.repairTable(NAMESPACE, table, metadata, getCreationOptions());

      Optional<PartitionKeyDefinitionVersion> versionAfter =
          readPartitionKeyVersion(cosmosClient, NAMESPACE, table);
      assertThat(versionAfter).isEqualTo(versionBefore);
      assertThat(isV1OrUnset(versionAfter)).isTrue();
    } finally {
      dropTableQuietly(table);
    }
  }

  private static TableMetadata textPartitionKeyMetadata() {
    return TableMetadata.newBuilder()
        .addColumn(PK, DataType.TEXT)
        .addColumn(VALUE, DataType.INT)
        .addPartitionKey(PK)
        .build();
  }

  private void createPhysicalV1Container(String table) {
    PartitionKeyDefinition definition =
        new PartitionKeyDefinition()
            .setKind(PartitionKind.HASH)
            .setPaths(Arrays.asList("/concatenatedPartitionKey"))
            .setVersion(PartitionKeyDefinitionVersion.V1);
    cosmosClient
        .getDatabase(NAMESPACE)
        .createContainer(new CosmosContainerProperties(table, definition));
  }

  private void assertV2Container(String table) {
    Optional<PartitionKeyDefinitionVersion> version =
        readPartitionKeyVersion(cosmosClient, NAMESPACE, table);
    assertThat(version).contains(PartitionKeyDefinitionVersion.V2);
  }

  private static Optional<PartitionKeyDefinitionVersion> readPartitionKeyVersion(
      CosmosClient client, String namespace, String table) {
    CosmosContainer container = client.getDatabase(namespace).getContainer(table);
    PartitionKeyDefinition definition =
        container.read().getProperties().getPartitionKeyDefinition();
    return Optional.ofNullable(definition.getVersion());
  }

  private static boolean isV1OrUnset(Optional<PartitionKeyDefinitionVersion> version) {
    return !version.isPresent() || version.get() == PartitionKeyDefinitionVersion.V1;
  }

  private void dropTableQuietly(String table) {
    try {
      admin.dropTable(NAMESPACE, table);
    } catch (Exception e) {
      // ignore cleanup failures
    }
  }
}
