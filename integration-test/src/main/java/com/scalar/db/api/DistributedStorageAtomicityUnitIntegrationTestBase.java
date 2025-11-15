package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DistributedStorageAtomicityUnitIntegrationTestBase {
  protected static final Logger logger =
      LoggerFactory.getLogger(DistributedStorageAtomicityUnitIntegrationTestBase.class);

  protected static final String TEST_NAME = "storage_mau";
  protected static final String NAMESPACE1 = "int_test_" + TEST_NAME + "1";
  protected static final String NAMESPACE2 = "int_test_" + TEST_NAME + "2";
  protected static final String NAMESPACE3 = "int_test_" + TEST_NAME + "3";
  protected static final String TABLE1 = "test_table1";
  protected static final String TABLE2 = "test_table2";
  protected static final String COL_NAME1 = "c1";
  protected static final String COL_NAME2 = "c2";
  protected static final String COL_NAME3 = "c3";
  protected static final String COL_NAME4 = "c4";
  protected static final TableMetadata TABLE_METADATA =
      TableMetadata.newBuilder()
          .addColumn(COL_NAME1, DataType.INT)
          .addColumn(COL_NAME2, DataType.INT)
          .addColumn(COL_NAME3, DataType.INT)
          .addColumn(COL_NAME4, DataType.INT)
          .addPartitionKey(COL_NAME1)
          .addClusteringKey(COL_NAME2)
          .build();

  protected DistributedStorage storage;
  protected DistributedStorageAdmin admin;
  protected String namespace1;
  protected String namespace2;
  protected String namespace3;

  @BeforeAll
  public void beforeAll() throws Exception {
    initialize(TEST_NAME);
    StorageFactory factory = StorageFactory.create(getProperties(TEST_NAME));
    admin = factory.getStorageAdmin();
    namespace1 = getNamespace1();
    namespace2 = getNamespace2();
    namespace3 = getNamespace3();
    createTables();
    storage = factory.getStorage();
  }

  protected void initialize(String testName) throws Exception {}

  protected abstract Properties getProperties(String testName);

  protected String getNamespace1() {
    return NAMESPACE1;
  }

  protected String getNamespace2() {
    return NAMESPACE2;
  }

  protected String getNamespace3() {
    return NAMESPACE3;
  }

  protected void createTables() throws ExecutionException {
    Map<String, String> options = getCreationOptions();
    admin.createNamespace(namespace1, true, options);
    admin.createNamespace(namespace2, true, options);
    admin.createNamespace(namespace3, true, options);
    admin.createTable(namespace1, TABLE1, TABLE_METADATA, true, options);
    admin.createTable(namespace1, TABLE2, TABLE_METADATA, true, options);
    admin.createTable(namespace2, TABLE1, TABLE_METADATA, true, options);
    admin.createTable(namespace3, TABLE1, TABLE_METADATA, true, options);
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  @BeforeEach
  public void setUp() throws Exception {
    truncateTable();
  }

  protected void truncateTable() throws ExecutionException {
    admin.truncateTable(namespace1, TABLE1);
    admin.truncateTable(namespace1, TABLE2);
    admin.truncateTable(namespace2, TABLE1);
    admin.truncateTable(namespace3, TABLE1);
  }

  @AfterAll
  public void afterAll() throws Exception {
    try {
      dropTable();
    } catch (Exception e) {
      logger.warn("Failed to drop table", e);
    }

    try {
      if (admin != null) {
        admin.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close admin", e);
    }

    try {
      if (storage != null) {
        storage.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close storage", e);
    }
  }

  protected void dropTable() throws ExecutionException {
    admin.dropTable(namespace1, TABLE1);
    admin.dropTable(namespace1, TABLE2);
    admin.dropTable(namespace2, TABLE1);
    admin.dropTable(namespace3, TABLE1);
    admin.dropNamespace(namespace1);
    admin.dropNamespace(namespace2);
    admin.dropNamespace(namespace3);
  }

  @Test
  public void mutate_MutationsWithinRecordGiven_ShouldBehaveCorrectlyBaseOnAtomicityUnit()
      throws ExecutionException {
    // Arrange
    Put put1 =
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE1)
            .partitionKey(Key.ofInt(COL_NAME1, 0))
            .clusteringKey(Key.ofInt(COL_NAME2, 1))
            .intValue(COL_NAME3, 1)
            .build();
    Put put2 =
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE1)
            .partitionKey(Key.ofInt(COL_NAME1, 0))
            .clusteringKey(Key.ofInt(COL_NAME2, 1))
            .intValue(COL_NAME4, 2)
            .build();

    // Act
    Exception exception =
        Assertions.catchException(() -> storage.mutate(Arrays.asList(put1, put2)));

    // Assert
    assertThat(exception).doesNotThrowAnyException();

    Optional<Result> result =
        storage.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE1)
                .partitionKey(Key.ofInt(COL_NAME1, 0))
                .clusteringKey(Key.ofInt(COL_NAME2, 1))
                .build());
    assertThat(result).isPresent();
    assertThat(result.get().getInt(COL_NAME1)).isEqualTo(0);
    assertThat(result.get().getInt(COL_NAME2)).isEqualTo(1);
    assertThat(result.get().getInt(COL_NAME3)).isEqualTo(1);
    assertThat(result.get().getInt(COL_NAME4)).isEqualTo(2);
  }

  @Test
  public void mutate_MutationsWithinPartitionGiven_ShouldBehaveCorrectlyBaseOnAtomicityUnit()
      throws ExecutionException {
    // Arrange
    storage.put(
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE1)
            .partitionKey(Key.ofInt(COL_NAME1, 0))
            .clusteringKey(Key.ofInt(COL_NAME2, 3))
            .intValue(COL_NAME3, 3)
            .build());

    Put put1 =
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE1)
            .partitionKey(Key.ofInt(COL_NAME1, 0))
            .clusteringKey(Key.ofInt(COL_NAME2, 1))
            .intValue(COL_NAME3, 1)
            .build();
    Put put2 =
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE1)
            .partitionKey(Key.ofInt(COL_NAME1, 0))
            .clusteringKey(Key.ofInt(COL_NAME2, 2))
            .intValue(COL_NAME3, 2)
            .build();
    Delete delete =
        Delete.newBuilder()
            .namespace(namespace1)
            .table(TABLE1)
            .partitionKey(Key.ofInt(COL_NAME1, 0))
            .clusteringKey(Key.ofInt(COL_NAME2, 3))
            .build();

    // Act
    Exception exception =
        Assertions.catchException(() -> storage.mutate(Arrays.asList(put1, put2, delete)));

    // Assert
    StorageInfo storageInfo = admin.getStorageInfo(namespace1);
    switch (storageInfo.getAtomicityUnit()) {
      case RECORD:
        assertThat(exception).isInstanceOf(IllegalArgumentException.class);
        break;
      case PARTITION:
      case TABLE:
      case NAMESPACE:
      case STORAGE:
        assertThat(exception).doesNotThrowAnyException();

        Optional<Result> result1 =
            storage.get(
                Get.newBuilder()
                    .namespace(namespace1)
                    .table(TABLE1)
                    .partitionKey(Key.ofInt(COL_NAME1, 0))
                    .clusteringKey(Key.ofInt(COL_NAME2, 1))
                    .build());
        assertThat(result1).isPresent();
        assertThat(result1.get().getInt(COL_NAME1)).isEqualTo(0);
        assertThat(result1.get().getInt(COL_NAME2)).isEqualTo(1);
        assertThat(result1.get().getInt(COL_NAME3)).isEqualTo(1);

        Optional<Result> result2 =
            storage.get(
                Get.newBuilder()
                    .namespace(namespace1)
                    .table(TABLE1)
                    .partitionKey(Key.ofInt(COL_NAME1, 0))
                    .clusteringKey(Key.ofInt(COL_NAME2, 2))
                    .build());
        assertThat(result2).isPresent();
        assertThat(result2.get().getInt(COL_NAME1)).isEqualTo(0);
        assertThat(result2.get().getInt(COL_NAME2)).isEqualTo(2);
        assertThat(result2.get().getInt(COL_NAME3)).isEqualTo(2);

        Optional<Result> result3 =
            storage.get(
                Get.newBuilder()
                    .namespace(namespace1)
                    .table(TABLE1)
                    .partitionKey(Key.ofInt(COL_NAME1, 0))
                    .clusteringKey(Key.ofInt(COL_NAME2, 3))
                    .build());
        assertThat(result3).isEmpty();
        break;
      default:
        throw new AssertionError();
    }
  }

  @Test
  public void mutate_MutationsWithinTableGiven_ShouldBehaveCorrectlyBaseOnAtomicityUnit()
      throws ExecutionException {
    // Arrange
    storage.put(
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE1)
            .partitionKey(Key.ofInt(COL_NAME1, 2))
            .clusteringKey(Key.ofInt(COL_NAME2, 3))
            .intValue(COL_NAME3, 3)
            .build());

    Put put1 =
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE1)
            .partitionKey(Key.ofInt(COL_NAME1, 0))
            .clusteringKey(Key.ofInt(COL_NAME2, 1))
            .intValue(COL_NAME3, 1)
            .build();
    Put put2 =
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE1)
            .partitionKey(Key.ofInt(COL_NAME1, 1))
            .clusteringKey(Key.ofInt(COL_NAME2, 2))
            .intValue(COL_NAME3, 2)
            .build();
    Delete delete =
        Delete.newBuilder()
            .namespace(namespace1)
            .table(TABLE1)
            .partitionKey(Key.ofInt(COL_NAME1, 2))
            .clusteringKey(Key.ofInt(COL_NAME2, 3))
            .build();

    // Act
    Exception exception =
        Assertions.catchException(() -> storage.mutate(Arrays.asList(put1, put2, delete)));

    // Assert
    StorageInfo storageInfo = admin.getStorageInfo(namespace1);
    switch (storageInfo.getAtomicityUnit()) {
      case RECORD:
      case PARTITION:
        assertThat(exception).isInstanceOf(IllegalArgumentException.class);
        break;
      case TABLE:
      case NAMESPACE:
      case STORAGE:
        assertThat(exception).doesNotThrowAnyException();

        Optional<Result> result1 =
            storage.get(
                Get.newBuilder()
                    .namespace(namespace1)
                    .table(TABLE1)
                    .partitionKey(Key.ofInt(COL_NAME1, 0))
                    .clusteringKey(Key.ofInt(COL_NAME2, 1))
                    .build());
        assertThat(result1).isPresent();
        assertThat(result1.get().getInt(COL_NAME1)).isEqualTo(0);
        assertThat(result1.get().getInt(COL_NAME2)).isEqualTo(1);
        assertThat(result1.get().getInt(COL_NAME3)).isEqualTo(1);

        Optional<Result> result2 =
            storage.get(
                Get.newBuilder()
                    .namespace(namespace1)
                    .table(TABLE1)
                    .partitionKey(Key.ofInt(COL_NAME1, 1))
                    .clusteringKey(Key.ofInt(COL_NAME2, 2))
                    .build());
        assertThat(result2).isPresent();
        assertThat(result2.get().getInt(COL_NAME1)).isEqualTo(1);
        assertThat(result2.get().getInt(COL_NAME2)).isEqualTo(2);
        assertThat(result2.get().getInt(COL_NAME3)).isEqualTo(2);

        Optional<Result> result3 =
            storage.get(
                Get.newBuilder()
                    .namespace(namespace1)
                    .table(TABLE1)
                    .partitionKey(Key.ofInt(COL_NAME1, 2))
                    .clusteringKey(Key.ofInt(COL_NAME2, 3))
                    .build());
        assertThat(result3).isEmpty();
        break;
      default:
        throw new AssertionError();
    }
  }

  @Test
  public void mutate_MutationsWithinNamespaceGiven_ShouldBehaveCorrectlyBaseOnAtomicityUnit()
      throws ExecutionException {
    // Arrange
    storage.put(
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE1)
            .partitionKey(Key.ofInt(COL_NAME1, 2))
            .clusteringKey(Key.ofInt(COL_NAME2, 3))
            .intValue(COL_NAME3, 3)
            .build());

    Put put1 =
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE1)
            .partitionKey(Key.ofInt(COL_NAME1, 0))
            .clusteringKey(Key.ofInt(COL_NAME2, 1))
            .intValue(COL_NAME3, 1)
            .build();
    Put put2 =
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE2)
            .partitionKey(Key.ofInt(COL_NAME1, 1))
            .clusteringKey(Key.ofInt(COL_NAME2, 2))
            .intValue(COL_NAME3, 2)
            .build();
    Delete delete =
        Delete.newBuilder()
            .namespace(namespace1)
            .table(TABLE1)
            .partitionKey(Key.ofInt(COL_NAME1, 2))
            .clusteringKey(Key.ofInt(COL_NAME2, 3))
            .build();

    // Act
    Exception exception =
        Assertions.catchException(() -> storage.mutate(Arrays.asList(put1, put2, delete)));

    // Assert
    StorageInfo storageInfo = admin.getStorageInfo(namespace1);
    switch (storageInfo.getAtomicityUnit()) {
      case RECORD:
      case PARTITION:
      case TABLE:
        assertThat(exception).isInstanceOf(IllegalArgumentException.class);
        break;
      case NAMESPACE:
      case STORAGE:
        assertThat(exception).doesNotThrowAnyException();

        Optional<Result> result1 =
            storage.get(
                Get.newBuilder()
                    .namespace(namespace1)
                    .table(TABLE1)
                    .partitionKey(Key.ofInt(COL_NAME1, 0))
                    .clusteringKey(Key.ofInt(COL_NAME2, 1))
                    .build());
        assertThat(result1).isPresent();
        assertThat(result1.get().getInt(COL_NAME1)).isEqualTo(0);
        assertThat(result1.get().getInt(COL_NAME2)).isEqualTo(1);
        assertThat(result1.get().getInt(COL_NAME3)).isEqualTo(1);

        Optional<Result> result2 =
            storage.get(
                Get.newBuilder()
                    .namespace(namespace1)
                    .table(TABLE2)
                    .partitionKey(Key.ofInt(COL_NAME1, 1))
                    .clusteringKey(Key.ofInt(COL_NAME2, 2))
                    .build());
        assertThat(result2).isPresent();
        assertThat(result2.get().getInt(COL_NAME1)).isEqualTo(1);
        assertThat(result2.get().getInt(COL_NAME2)).isEqualTo(2);
        assertThat(result2.get().getInt(COL_NAME3)).isEqualTo(2);

        Optional<Result> result3 =
            storage.get(
                Get.newBuilder()
                    .namespace(namespace1)
                    .table(TABLE1)
                    .partitionKey(Key.ofInt(COL_NAME1, 2))
                    .clusteringKey(Key.ofInt(COL_NAME2, 3))
                    .build());
        assertThat(result3).isEmpty();
        break;
      default:
        throw new AssertionError();
    }
  }

  @Test
  public void mutate_MutationsWithinStorageGiven_ShouldBehaveCorrectlyBaseOnAtomicityUnit()
      throws ExecutionException {
    // Arrange
    storage.put(
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE1)
            .partitionKey(Key.ofInt(COL_NAME1, 2))
            .clusteringKey(Key.ofInt(COL_NAME2, 3))
            .intValue(COL_NAME3, 3)
            .build());

    Put put1 =
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE1)
            .partitionKey(Key.ofInt(COL_NAME1, 0))
            .clusteringKey(Key.ofInt(COL_NAME2, 1))
            .intValue(COL_NAME3, 1)
            .build();
    Put put2 =
        Put.newBuilder()
            .namespace(namespace2)
            .table(TABLE1)
            .partitionKey(Key.ofInt(COL_NAME1, 1))
            .clusteringKey(Key.ofInt(COL_NAME2, 2))
            .intValue(COL_NAME3, 2)
            .build();
    Delete delete =
        Delete.newBuilder()
            .namespace(namespace1)
            .table(TABLE1)
            .partitionKey(Key.ofInt(COL_NAME1, 2))
            .clusteringKey(Key.ofInt(COL_NAME2, 3))
            .build();

    // Act
    Exception exception =
        Assertions.catchException(() -> storage.mutate(Arrays.asList(put1, put2, delete)));

    // Assert
    StorageInfo storageInfo = admin.getStorageInfo(namespace1);
    switch (storageInfo.getAtomicityUnit()) {
      case RECORD:
      case PARTITION:
      case TABLE:
      case NAMESPACE:
        assertThat(exception).isInstanceOf(IllegalArgumentException.class);
        break;
      case STORAGE:
        assertThat(exception).doesNotThrowAnyException();

        Optional<Result> result1 =
            storage.get(
                Get.newBuilder()
                    .namespace(namespace1)
                    .table(TABLE1)
                    .partitionKey(Key.ofInt(COL_NAME1, 0))
                    .clusteringKey(Key.ofInt(COL_NAME2, 1))
                    .build());
        assertThat(result1).isPresent();
        assertThat(result1.get().getInt(COL_NAME1)).isEqualTo(0);
        assertThat(result1.get().getInt(COL_NAME2)).isEqualTo(1);
        assertThat(result1.get().getInt(COL_NAME3)).isEqualTo(1);

        Optional<Result> result2 =
            storage.get(
                Get.newBuilder()
                    .namespace(namespace2)
                    .table(TABLE1)
                    .partitionKey(Key.ofInt(COL_NAME1, 1))
                    .clusteringKey(Key.ofInt(COL_NAME2, 2))
                    .build());
        assertThat(result2).isPresent();
        assertThat(result2.get().getInt(COL_NAME1)).isEqualTo(1);
        assertThat(result2.get().getInt(COL_NAME2)).isEqualTo(2);
        assertThat(result2.get().getInt(COL_NAME3)).isEqualTo(2);

        Optional<Result> result3 =
            storage.get(
                Get.newBuilder()
                    .namespace(namespace1)
                    .table(TABLE1)
                    .partitionKey(Key.ofInt(COL_NAME1, 2))
                    .clusteringKey(Key.ofInt(COL_NAME2, 3))
                    .build());
        assertThat(result3).isEmpty();
        break;
      default:
        throw new AssertionError();
    }
  }
}
