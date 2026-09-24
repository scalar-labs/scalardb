package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.PartitionKeyDefinition;
import com.azure.cosmos.models.PartitionKeyDefinitionVersion;
import com.azure.cosmos.models.PartitionKind;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
  private static final String CK = "ck";
  private static final String VALUE = "value";

  private DistributedStorage storage;
  private DistributedStorageAdmin admin;
  private CosmosClient cosmosClient;
  private Properties properties;

  @BeforeAll
  void beforeAll() throws Exception {
    properties = CosmosEnv.getProperties(TEST_NAME);
    StorageFactory factory = StorageFactory.create(properties);
    admin = factory.getStorageAdmin();
    storage = factory.getStorage();
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
    try {
      cosmosClient.getDatabase(NAMESPACE).read();
    } catch (CosmosException e) {
      if (e.getStatusCode() == CosmosErrorCode.NOT_FOUND.get()) {
        return;
      }
      throw e;
    }
    cosmosClient.getDatabase(NAMESPACE).delete();
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
    if (admin != null) {
      admin.close();
    }
    if (storage != null) {
      storage.close();
    }
  }

  private Map<String, String> getCreationOptions() {
    return CosmosEnv.getCreationOptions();
  }

  @Test
  void createTable_shouldUsePartitionKeyDefinitionV2() throws ExecutionException {
    String table = "user_table_v2";
    TableMetadata metadata = textPartitionKeyMetadata(false);

    try {
      admin.createTable(NAMESPACE, table, metadata, getCreationOptions());
      assertV2Container(table);

      CosmosConfig config = new CosmosConfig(new DatabaseConfig(properties));
      Optional<PartitionKeyDefinitionVersion> metadataVersion =
          CosmosPartitionKeyTestUtils.readPartitionKeyVersion(
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
        CosmosPartitionKeyTestUtils.readPartitionKeyVersion(
            cosmosClient, config.getMetadataDatabase(), CosmosAdmin.NAMESPACES_CONTAINER);
    assertThat(namespacesVersion).contains(PartitionKeyDefinitionVersion.V2);
  }

  @Test
  void repairTable_shouldNotUpgradePartitionKeyVersionFromV1() throws ExecutionException {
    String table = "existing_v1";
    TableMetadata metadata = textPartitionKeyMetadata(false);

    try {
      createPhysicalV1Container(table);
      Optional<PartitionKeyDefinitionVersion> versionBefore =
          CosmosPartitionKeyTestUtils.readPartitionKeyVersion(cosmosClient, NAMESPACE, table);
      assertThat(CosmosPartitionKeyTestUtils.isV1OrUnset(versionBefore)).isTrue();

      admin.repairTable(NAMESPACE, table, metadata, getCreationOptions());

      Optional<PartitionKeyDefinitionVersion> versionAfter =
          CosmosPartitionKeyTestUtils.readPartitionKeyVersion(cosmosClient, NAMESPACE, table);
      assertThat(versionAfter).isEqualTo(versionBefore);
      assertThat(CosmosPartitionKeyTestUtils.isV1OrUnset(versionAfter)).isTrue();
    } finally {
      dropTableQuietly(table);
    }
  }

  @Test
  void putAndGet_with150BytePartitionKey_onV2Container_shouldSucceed() throws ExecutionException {
    String table = "long_pk_v2";
    String partitionKeyValue = CosmosPartitionKeyTestUtils.asciiOfLength(150);

    try {
      admin.createTable(NAMESPACE, table, textPartitionKeyMetadata(false), getCreationOptions());
      assertV2Container(table);

      putWithoutClusteringKey(table, partitionKeyValue, 99);
      Optional<Result> result = getWithoutClusteringKey(table, partitionKeyValue);
      assertThat(result).isPresent();
      assertThat(result.get().getInt(VALUE)).isEqualTo(99);
    } finally {
      dropTableQuietly(table);
    }
  }

  @Test
  void mutate_with150BytePartitionKey_onV2Container_shouldSucceed() throws ExecutionException {
    String table = "mutate_long_pk_v2";
    String partitionKeyValue = CosmosPartitionKeyTestUtils.asciiOfLength(150);

    try {
      admin.createTable(NAMESPACE, table, textPartitionKeyMetadata(true), getCreationOptions());
      assertV2Container(table);

      Put put1 =
          Put.newBuilder()
              .namespace(NAMESPACE)
              .table(table)
              .partitionKey(Key.ofText(PK, partitionKeyValue))
              .clusteringKey(Key.ofInt(CK, 1))
              .intValue(VALUE, 10)
              .build();
      Put put2 =
          Put.newBuilder()
              .namespace(NAMESPACE)
              .table(table)
              .partitionKey(Key.ofText(PK, partitionKeyValue))
              .clusteringKey(Key.ofInt(CK, 2))
              .intValue(VALUE, 20)
              .build();
      assertThatCode(() -> storage.mutate(Arrays.asList(put1, put2))).doesNotThrowAnyException();

      Put putIfNotExists =
          Put.newBuilder()
              .namespace(NAMESPACE)
              .table(table)
              .partitionKey(Key.ofText(PK, partitionKeyValue))
              .clusteringKey(Key.ofInt(CK, 3))
              .intValue(VALUE, 30)
              .condition(ConditionBuilder.putIfNotExists())
              .build();
      storage.put(putIfNotExists);

      Delete deleteIf =
          Delete.newBuilder()
              .namespace(NAMESPACE)
              .table(table)
              .partitionKey(Key.ofText(PK, partitionKeyValue))
              .clusteringKey(Key.ofInt(CK, 1))
              .condition(
                  ConditionBuilder.deleteIf(ConditionBuilder.column(VALUE).isEqualToInt(10))
                      .build())
              .build();
      assertThatCode(() -> storage.delete(deleteIf)).doesNotThrowAnyException();

      List<Result> results = scanPartition(table, partitionKeyValue);
      assertThat(results).hasSize(2);
    } finally {
      dropTableQuietly(table);
    }
  }

  private static TableMetadata textPartitionKeyMetadata(boolean withClusteringKey) {
    TableMetadata.Builder builder =
        TableMetadata.newBuilder().addColumn(PK, DataType.TEXT).addColumn(VALUE, DataType.INT);
    if (withClusteringKey) {
      builder.addColumn(CK, DataType.INT).addClusteringKey(CK, Scan.Ordering.Order.ASC);
    }
    return builder.addPartitionKey(PK).build();
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
        CosmosPartitionKeyTestUtils.readPartitionKeyVersion(cosmosClient, NAMESPACE, table);
    assertThat(version).contains(PartitionKeyDefinitionVersion.V2);
  }

  private void putWithoutClusteringKey(String table, String pkValue, int value)
      throws ExecutionException {
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(table)
            .partitionKey(Key.ofText(PK, pkValue))
            .intValue(VALUE, value)
            .build();
    storage.put(put);
  }

  private Optional<Result> getWithoutClusteringKey(String table, String pkValue)
      throws ExecutionException {
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE)
            .table(table)
            .partitionKey(Key.ofText(PK, pkValue))
            .build();
    return storage.get(get);
  }

  private List<Result> scanPartition(String table, String pkValue) throws ExecutionException {
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE)
            .table(table)
            .partitionKey(Key.ofText(PK, pkValue))
            .build();
    Scanner scanner = storage.scan(scan);
    List<Result> results = new ArrayList<>();
    Optional<Result> result;
    while ((result = scanner.one()).isPresent()) {
      results.add(result.get());
    }
    try {
      scanner.close();
    } catch (java.io.IOException e) {
      throw new ExecutionException("Failed to close scanner", e);
    }
    return results;
  }

  private void dropTableQuietly(String table) {
    try {
      admin.dropTable(NAMESPACE, table);
    } catch (Exception e) {
      // ignore cleanup failures
    }
  }
}
