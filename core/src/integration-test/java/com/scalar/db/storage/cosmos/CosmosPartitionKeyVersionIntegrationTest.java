package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.PartitionKeyDefinitionVersion;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/** Integration tests for Cosmos DB partition key definition V1 vs V2 in the ScalarDB adapter. */
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
    // namespaceExists() only checks the namespaces metadata item. A prior failed run can leave the
    // physical database/containers while that item is missing, so always drop leftovers first.
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

  // New tables use partition key definition V2 by default
  @Test
  void createTable_shouldUsePartitionKeyDefinitionV2() throws ExecutionException {
    String table = "exp1_v2_baseline";
    TableMetadata metadata = textPartitionKeyMetadata(false);

    try {
      admin.createTable(NAMESPACE, table, metadata, getCreationOptions());

      Optional<PartitionKeyDefinitionVersion> version =
          CosmosPartitionKeyTestUtils.readPartitionKeyVersion(cosmosClient, NAMESPACE, table);

      assertThat(version).contains(PartitionKeyDefinitionVersion.V2);
    } finally {
      dropTableQuietly(table);
    }
  }

  @Test
  void createTable_withLegacyPartitionKeyOption_shouldUsePartitionKeyDefinitionV1()
      throws ExecutionException {
    String table = "exp1_v1_legacy";
    TableMetadata metadata = textPartitionKeyMetadata(false);
    Map<String, String> options = v1CreationOptions();

    try {
      admin.createTable(NAMESPACE, table, metadata, options);

      Optional<PartitionKeyDefinitionVersion> version =
          CosmosPartitionKeyTestUtils.readPartitionKeyVersion(cosmosClient, NAMESPACE, table);

      assertThat(CosmosPartitionKeyTestUtils.isV1OrUnset(version)).isTrue();
    } finally {
      dropTableQuietly(table);
    }
  }

  // 101-byte partition key is accepted on a V1 container
  @Test
  void putAndGet_with101BytePartitionKey_onV1Container_shouldSucceed() throws ExecutionException {
    String table = "exp2a_101_bytes";
    String partitionKeyValue = CosmosPartitionKeyTestUtils.asciiOfLength(101);
    assertThat(CosmosPartitionKeyTestUtils.utf8ByteLength(partitionKeyValue)).isEqualTo(101);

    try {
      createV1Table(table, textPartitionKeyMetadata(false));
      putWithoutClusteringKey(table, partitionKeyValue, 1);

      Optional<Result> result = getWithoutClusteringKey(table, partitionKeyValue);
      assertThat(result).isPresent();
      assertThat(result.get().getInt(VALUE)).isEqualTo(1);
    } finally {
      dropTableQuietly(table);
    }
  }

  // 102-byte partition key is rejected on a V1 container
  @Test
  void put_with102BytePartitionKey_onV1Container_shouldBeRejected() throws ExecutionException {
    String table = "exp2b_102_bytes";
    String partitionKeyValue = CosmosPartitionKeyTestUtils.asciiOfLength(102);

    try {
      createV1Table(table, textPartitionKeyMetadata(false));

      assertThatThrownBy(() -> putWithoutClusteringKey(table, partitionKeyValue, 42))
          .isInstanceOf(IllegalArgumentException.class);
    } finally {
      dropTableQuietly(table);
    }
  }

  // V1 collision-pair keys (>101 bytes) are rejected before reaching Cosmos
  @Test
  void put_withColliding102ByteKeys_onV1Container_shouldBeRejected() throws ExecutionException {
    String table = "exp2c_v1_collision";
    String[] keys = CosmosPartitionKeyTestUtils.collisionPairAtByteBoundary(102);

    try {
      createV1Table(table, textPartitionKeyMetadata(true));

      assertThatThrownBy(() -> putWithIntClusteringKey(table, keys[0], 1, 100))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> putWithIntClusteringKey(table, keys[1], 2, 200))
          .isInstanceOf(IllegalArgumentException.class);
    } finally {
      dropTableQuietly(table);
    }
  }

  // 200-byte partition key is accepted on a V2 container
  @Test
  void putAndGet_with200BytePartitionKey_onV2Container_shouldSucceed() throws ExecutionException {
    String table = "exp2d_200_bytes";
    String partitionKeyValue = CosmosPartitionKeyTestUtils.asciiOfLength(200);

    try {
      createV2Table(table, textPartitionKeyMetadata(false));
      putWithoutClusteringKey(table, partitionKeyValue, 7);

      Optional<Result> result = getWithoutClusteringKey(table, partitionKeyValue);
      assertThat(result).isPresent();
      assertThat(result.get().getInt(VALUE)).isEqualTo(7);
    } finally {
      dropTableQuietly(table);
    }
  }

  // V2 container CRUD with a long partition key
  @Test
  void putAndGet_with150BytePartitionKey_onV2Container_shouldSucceed() throws ExecutionException {
    String table = "exp3_v2_150_bytes";
    String partitionKeyValue = CosmosPartitionKeyTestUtils.asciiOfLength(150);

    try {
      createV2Table(table, textPartitionKeyMetadata(false));
      assertV2Container(table);

      putWithoutClusteringKey(table, partitionKeyValue, 99);
      Optional<Result> result = getWithoutClusteringKey(table, partitionKeyValue);
      assertThat(result).isPresent();
      assertThat(result.get().getInt(VALUE)).isEqualTo(99);
    } finally {
      dropTableQuietly(table);
    }
  }

  // V2 keeps keys that would collide under V1 in separate partitions
  @Test
  void put_withColliding102ByteKeys_onV2Container_shouldNotSharePartition()
      throws ExecutionException {
    String table = "exp4_v2_no_collision";
    String[] keys = CosmosPartitionKeyTestUtils.collisionPairAtByteBoundary(102);

    try {
      createV2Table(table, textPartitionKeyMetadata(true));
      assertV2Container(table);

      putWithIntClusteringKey(table, keys[0], 1, 100);
      putWithIntClusteringKey(table, keys[1], 2, 200);

      List<Result> scanResults = scanPartition(table, keys[0]);
      assertThat(scanResults).hasSize(1);
      assertThat(scanResults.get(0).getInt(CK)).isEqualTo(1);
    } finally {
      dropTableQuietly(table);
    }
  }

  // Document id at the 255-character Cosmos limit (V2; a 127-byte PK is illegal on V1)
  @Test
  void put_with255CharDocumentId_shouldSucceed() throws ExecutionException {
    String table = "exp5a_id_255";
    String pkValue = CosmosPartitionKeyTestUtils.asciiOfLength(127);
    String ckValue = CosmosPartitionKeyTestUtils.asciiOfLength(127);
    assertThat(pkValue.length() + 1 + ckValue.length()).isEqualTo(255);

    try {
      createV2Table(table, textPartitionKeyMetadata(true, DataType.TEXT));
      putWithTextClusteringKey(table, pkValue, ckValue, 1);

      Optional<Result> result = getWithTextClusteringKey(table, pkValue, ckValue);
      assertThat(result).isPresent();
    } finally {
      dropTableQuietly(table);
    }
  }

  // Document id over 255 characters is rejected client-side
  @Test
  void put_with256CharDocumentId_shouldFail() throws ExecutionException {
    String table = "exp5b_id_256";
    String pkValue = CosmosPartitionKeyTestUtils.asciiOfLength(128);
    String ckValue = CosmosPartitionKeyTestUtils.asciiOfLength(127);
    assertThat(pkValue.length() + 1 + ckValue.length()).isEqualTo(256);

    try {
      createV2Table(table, textPartitionKeyMetadata(true, DataType.TEXT));

      Put put =
          Put.newBuilder()
              .namespace(NAMESPACE)
              .table(table)
              .partitionKey(Key.ofText(PK, pkValue))
              .clusteringKey(Key.ofText(CK, ckValue))
              .intValue(VALUE, 1)
              .build();

      assertThatThrownBy(() -> storage.put(put)).isInstanceOf(IllegalArgumentException.class);
    } finally {
      dropTableQuietly(table);
    }
  }

  // repairTable does not upgrade an existing V1 container to V2
  @Test
  void repairTable_shouldNotUpgradePartitionKeyVersionFromV1() throws ExecutionException {
    String table = "exp6_repair_no_upgrade";
    TableMetadata metadata = textPartitionKeyMetadata(false);

    try {
      createV1Table(table, metadata);
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

  // Stored-procedure mutations with a long partition key on V2
  @Test
  void mutate_with150BytePartitionKey_onV2Container_shouldSucceed() throws ExecutionException {
    String table = "exp7_v2_mutate";
    String partitionKeyValue = CosmosPartitionKeyTestUtils.asciiOfLength(150);

    try {
      createV2Table(table, textPartitionKeyMetadata(true));
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
    return textPartitionKeyMetadata(withClusteringKey, DataType.INT);
  }

  private static TableMetadata textPartitionKeyMetadata(
      boolean withClusteringKey, DataType clusteringKeyType) {
    TableMetadata.Builder builder =
        TableMetadata.newBuilder().addColumn(PK, DataType.TEXT).addColumn(VALUE, DataType.INT);
    if (withClusteringKey) {
      builder.addColumn(CK, clusteringKeyType).addClusteringKey(CK, Scan.Ordering.Order.ASC);
    }
    return builder.addPartitionKey(PK).build();
  }

  private Map<String, String> v1CreationOptions() {
    Map<String, String> options = new HashMap<>(getCreationOptions());
    options.put(CosmosAdmin.LARGE_PARTITION_KEY, "false");
    return options;
  }

  private void createV1Table(String table, TableMetadata metadata) throws ExecutionException {
    admin.repairTable(NAMESPACE, table, metadata, v1CreationOptions());
  }

  private void createV2Table(String table, TableMetadata metadata) throws ExecutionException {
    admin.repairTable(NAMESPACE, table, metadata, getCreationOptions());
    assertV2Container(table);
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

  private void putWithIntClusteringKey(String table, String pkValue, int ckValue, int value)
      throws ExecutionException {
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(table)
            .partitionKey(Key.ofText(PK, pkValue))
            .clusteringKey(Key.ofInt(CK, ckValue))
            .intValue(VALUE, value)
            .build();
    storage.put(put);
  }

  private void putWithTextClusteringKey(String table, String pkValue, String ckValue, int value)
      throws ExecutionException {
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(table)
            .partitionKey(Key.ofText(PK, pkValue))
            .clusteringKey(Key.ofText(CK, ckValue))
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

  private Optional<Result> getWithTextClusteringKey(String table, String pkValue, String ckValue)
      throws ExecutionException {
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE)
            .table(table)
            .partitionKey(Key.ofText(PK, pkValue))
            .clusteringKey(Key.ofText(CK, ckValue))
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
