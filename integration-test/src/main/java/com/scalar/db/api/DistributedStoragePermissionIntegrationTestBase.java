package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThatCode;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DistributedStoragePermissionIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(DistributedStoragePermissionIntegrationTestBase.class);

  private static final String TEST_NAME = "storage";
  private static final String NAMESPACE = "int_test_" + TEST_NAME;
  private static final String TABLE = "test_table";
  private static final String COL_NAME1 = "c1";
  private static final String COL_NAME2 = "c2";
  private static final String COL_NAME3 = "c3";
  private static final int PARTITION_KEY_VALUE = 1;
  private static final String CLUSTERING_KEY_VALUE1 = "value1";
  private static final String CLUSTERING_KEY_VALUE2 = "value2";
  private static final int INT_COLUMN_VALUE1 = 1;
  private static final int INT_COLUMN_VALUE2 = 1;

  private DistributedStorage storageForNormalUser;
  private DistributedStorageAdmin adminForRootUser;
  private String namespace;

  @BeforeAll
  public void beforeAll() throws Exception {
    // Create storage for normal user
    Properties propertiesForNormalUser = getPropertiesForNormalUser(TEST_NAME);
    StorageFactory factoryForNormalUser = StorageFactory.create(propertiesForNormalUser);
    storageForNormalUser = factoryForNormalUser.getStorage();
    // Create admin for root user
    Properties propertiesForRootUser = getProperties(TEST_NAME);
    StorageFactory factoryForRootUser = StorageFactory.create(propertiesForRootUser);
    adminForRootUser = factoryForRootUser.getStorageAdmin();
    namespace = getNamespace();
    createTable();
    Thread.sleep(1000); // Wait for the table to be created
  }

  @BeforeEach
  public void setUp() throws Exception {
    truncateTable();
  }

  @AfterAll
  public void afterAll() throws Exception {
    try {
      dropTable();
    } catch (Exception e) {
      logger.warn("Failed to drop table", e);
    }
    try {
      storageForNormalUser.close();
      adminForRootUser.close();
    } catch (Exception e) {
      logger.warn("Failed to close storage or admin", e);
    }
  }

  @Test
  public void get_WithSufficientPermission_ShouldSucceed() {
    // Arrange
    Get get =
        Get.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(Key.ofInt(COL_NAME1, PARTITION_KEY_VALUE))
            .clusteringKey(Key.ofText(COL_NAME2, CLUSTERING_KEY_VALUE1))
            .build();
    // Act Assert
    assertThatCode(() -> storageForNormalUser.get(get)).doesNotThrowAnyException();
  }

  @Test
  public void scan_WithSufficientPermission_ShouldSucceed() {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(Key.ofInt(COL_NAME1, PARTITION_KEY_VALUE))
            .build();
    // Act Assert
    assertThatCode(() -> storageForNormalUser.scan(scan).close()).doesNotThrowAnyException();
  }

  @Test
  public void scanAll_WithSufficientPermission_ShouldSucceed() {
    // Arrange
    Scan scan = Scan.newBuilder().namespace(namespace).table(TABLE).all().build();
    // Act Assert
    assertThatCode(() -> storageForNormalUser.scan(scan).close()).doesNotThrowAnyException();
  }

  @Test
  public void put_WithoutCondition_WithSufficientPermission_ShouldSucceed() {
    // Arrange
    Put put = createPut(CLUSTERING_KEY_VALUE1, INT_COLUMN_VALUE1, null);
    // Act Assert
    assertThatCode(() -> storageForNormalUser.put(put)).doesNotThrowAnyException();
  }

  @Test
  public void put_WithPutIfNotExists_WithSufficientPermission_ShouldSucceed() {
    // Arrange
    Put putWithPutIfNotExists =
        createPut(CLUSTERING_KEY_VALUE1, INT_COLUMN_VALUE1, ConditionBuilder.putIfNotExists());
    // Act Assert
    assertThatCode(() -> storageForNormalUser.put(putWithPutIfNotExists))
        .doesNotThrowAnyException();
  }

  @Test
  public void put_WithPutIfExists_WithSufficientPermission_ShouldSucceed()
      throws ExecutionException {
    // Arrange
    Put put = createPut(CLUSTERING_KEY_VALUE1, INT_COLUMN_VALUE1, null);
    storageForNormalUser.put(put);
    Put putWithPutIfExists =
        createPut(CLUSTERING_KEY_VALUE1, INT_COLUMN_VALUE2, ConditionBuilder.putIfExists());
    // Act Assert
    assertThatCode(() -> storageForNormalUser.put(putWithPutIfExists)).doesNotThrowAnyException();
  }

  @Test
  public void put_WithPutIf_WithSufficientPermission_ShouldSucceed() throws ExecutionException {
    // Arrange
    Put put = createPut(CLUSTERING_KEY_VALUE1, INT_COLUMN_VALUE1, null);
    storageForNormalUser.put(put);
    ConditionalExpression conditionalExpression =
        ConditionBuilder.buildConditionalExpression(
            IntColumn.of(COL_NAME3, INT_COLUMN_VALUE1), ConditionalExpression.Operator.EQ);
    Put putWithPutIf =
        createPut(
            CLUSTERING_KEY_VALUE1,
            INT_COLUMN_VALUE2,
            ConditionBuilder.putIf(conditionalExpression).build());
    // Act Assert
    assertThatCode(() -> storageForNormalUser.put(putWithPutIf)).doesNotThrowAnyException();
  }

  @Test
  public void put_WithMultiplePuts_WithSufficientPermission_ShouldSucceed() {
    // Arrange
    Put put1 = createPut(CLUSTERING_KEY_VALUE1, INT_COLUMN_VALUE1, null);
    Put put2 = createPut(CLUSTERING_KEY_VALUE2, INT_COLUMN_VALUE2, null);
    // Act Assert
    assertThatCode(() -> storageForNormalUser.put(Arrays.asList(put1, put2)))
        .doesNotThrowAnyException();
  }

  @Test
  public void delete_WithSufficientPermission_ShouldSucceed() {
    // Arrange
    Delete delete = createDelete(CLUSTERING_KEY_VALUE1, null);
    // Act Assert
    assertThatCode(() -> storageForNormalUser.delete(delete)).doesNotThrowAnyException();
  }

  @Test
  public void delete_WithDeleteIfExists_WithSufficientPermission_ShouldSucceed()
      throws ExecutionException {
    // Arrange
    Put put = createPut(CLUSTERING_KEY_VALUE1, INT_COLUMN_VALUE1, null);
    storageForNormalUser.put(put);
    Delete delete = createDelete(CLUSTERING_KEY_VALUE1, ConditionBuilder.deleteIfExists());
    // Act Assert
    assertThatCode(() -> storageForNormalUser.delete(delete)).doesNotThrowAnyException();
  }

  @Test
  public void delete_WithDeleteIf_WithSufficientPermission_ShouldSucceed()
      throws ExecutionException {
    // Arrange
    Put put = createPut(CLUSTERING_KEY_VALUE1, INT_COLUMN_VALUE1, null);
    storageForNormalUser.put(put);
    ConditionalExpression conditionalExpression =
        ConditionBuilder.buildConditionalExpression(
            IntColumn.of(COL_NAME3, INT_COLUMN_VALUE1), ConditionalExpression.Operator.EQ);
    Delete delete =
        createDelete(
            CLUSTERING_KEY_VALUE1, ConditionBuilder.deleteIf(conditionalExpression).build());
    // Act Assert
    assertThatCode(() -> storageForNormalUser.delete(delete)).doesNotThrowAnyException();
  }

  @Test
  public void delete_WithMultipleDeletes_WithSufficientPermission_ShouldSucceed() {
    // Arrange
    Delete delete1 = createDelete(CLUSTERING_KEY_VALUE1, null);
    Delete delete2 = createDelete(CLUSTERING_KEY_VALUE2, null);
    // Act Assert
    assertThatCode(() -> storageForNormalUser.delete(Arrays.asList(delete1, delete2)))
        .doesNotThrowAnyException();
  }

  @Test
  public void mutate_WithSufficientPermission_ShouldSucceed() {
    // Arrange
    Put put = createPut(CLUSTERING_KEY_VALUE1, INT_COLUMN_VALUE1, null);
    Delete delete = createDelete(CLUSTERING_KEY_VALUE2, null);
    // Act Assert
    assertThatCode(() -> storageForNormalUser.mutate(Arrays.asList(put, delete)))
        .doesNotThrowAnyException();
  }

  protected abstract Properties getProperties(String testName);

  protected abstract Properties getPropertiesForNormalUser(String testName);

  protected String getNamespace() {
    return NAMESPACE;
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  private void createTable() throws ExecutionException {
    Map<String, String> options = getCreationOptions();
    adminForRootUser.createNamespace(namespace, true, options);
    adminForRootUser.createTable(
        namespace,
        TABLE,
        TableMetadata.newBuilder()
            .addColumn(COL_NAME1, DataType.INT)
            .addColumn(COL_NAME2, DataType.TEXT)
            .addColumn(COL_NAME3, DataType.INT)
            .addPartitionKey(COL_NAME1)
            .addClusteringKey(COL_NAME2)
            .build(),
        true,
        options);
  }

  private void truncateTable() throws ExecutionException {
    adminForRootUser.truncateTable(namespace, TABLE);
  }

  private void dropTable() throws ExecutionException {
    adminForRootUser.dropTable(namespace, TABLE);
    adminForRootUser.dropNamespace(namespace);
  }

  private Put createPut(String clusteringKey, int intColumnValue, MutationCondition condition) {
    PutBuilder.Buildable buildable =
        Put.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(
                Key.ofInt(
                    COL_NAME1, DistributedStoragePermissionIntegrationTestBase.PARTITION_KEY_VALUE))
            .clusteringKey(Key.ofText(COL_NAME2, clusteringKey))
            .intValue(COL_NAME3, intColumnValue);
    if (condition != null) {
      buildable.condition(condition);
    }
    return buildable.build();
  }

  private Delete createDelete(String clusteringKey, MutationCondition condition) {
    DeleteBuilder.Buildable buildable =
        Delete.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(
                Key.ofInt(
                    COL_NAME1, DistributedStoragePermissionIntegrationTestBase.PARTITION_KEY_VALUE))
            .clusteringKey(Key.ofText(COL_NAME2, clusteringKey));
    if (condition != null) {
      buildable.condition(condition);
    }
    return buildable.build();
  }
}
