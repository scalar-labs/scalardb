package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.util.AdminTestUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DistributedStorageAdminIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(DistributedStorageAdminIntegrationTestBase.class);

  private static final String TEST_NAME = "storage_admin";
  private static final String NAMESPACE1 = "int_test_" + TEST_NAME + "1";
  private static final String NAMESPACE2 = "int_test_" + TEST_NAME + "2";
  private static final String NAMESPACE3 = "int_test_" + TEST_NAME + "3";
  private static final String TABLE1 = "test_table1";
  private static final String TABLE2 = "test_table2";
  private static final String TABLE3 = "test_table3";
  private static final String TABLE4 = "test_table4";
  private static final String COL_NAME1 = "c1";
  private static final String COL_NAME2 = "c2";
  private static final String COL_NAME3 = "c3";
  private static final String COL_NAME4 = "c4";
  private static final String COL_NAME5 = "c5";
  private static final String COL_NAME6 = "c6";
  private static final String COL_NAME7 = "c7";
  private static final String COL_NAME8 = "c8";
  private static final String COL_NAME9 = "c9";
  private static final String COL_NAME10 = "c10";
  private static final String COL_NAME11 = "c11";
  private static final String COL_NAME12 = "c12";
  private static final String COL_NAME13 = "c13";
  private static final String COL_NAME14 = "c14";
  private static final String COL_NAME15 = "c15";

  private static final TableMetadata TABLE_METADATA =
      TableMetadata.newBuilder()
          .addColumn(COL_NAME1, DataType.INT)
          .addColumn(COL_NAME2, DataType.TEXT)
          .addColumn(COL_NAME3, DataType.TEXT)
          .addColumn(COL_NAME4, DataType.INT)
          .addColumn(COL_NAME5, DataType.INT)
          .addColumn(COL_NAME6, DataType.TEXT)
          .addColumn(COL_NAME7, DataType.BIGINT)
          .addColumn(COL_NAME8, DataType.FLOAT)
          .addColumn(COL_NAME9, DataType.DOUBLE)
          .addColumn(COL_NAME10, DataType.BOOLEAN)
          .addColumn(COL_NAME11, DataType.BLOB)
          .addColumn(COL_NAME12, DataType.DATE)
          .addColumn(COL_NAME13, DataType.TIME)
          .addColumn(COL_NAME14, DataType.TIMESTAMPTZ)
          .addPartitionKey(COL_NAME2)
          .addPartitionKey(COL_NAME1)
          .addClusteringKey(COL_NAME4, Scan.Ordering.Order.ASC)
          .addClusteringKey(COL_NAME3, Scan.Ordering.Order.DESC)
          .addSecondaryIndex(COL_NAME5)
          .addSecondaryIndex(COL_NAME6)
          .build();
  private StorageFactory storageFactory;
  private DistributedStorageAdmin admin;
  private String namespace1;
  private String namespace2;
  private String namespace3;
  private String systemNamespaceName;

  @BeforeAll
  public void beforeAll() throws Exception {
    initialize(TEST_NAME);
    Properties properties = getProperties(TEST_NAME);
    storageFactory = StorageFactory.create(properties);
    admin = storageFactory.getAdmin();
    namespace1 = getNamespace1();
    namespace2 = getNamespace2();
    namespace3 = getNamespace3();
    systemNamespaceName = DatabaseConfig.getSystemNamespaceName(properties);
    createTables();
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

  private void createTables() throws ExecutionException {
    Map<String, String> options = getCreationOptions();
    for (String namespace : Arrays.asList(namespace1, namespace2)) {
      admin.createNamespace(namespace, true, options);
      for (String table : Arrays.asList(TABLE1, TABLE2, TABLE3)) {
        admin.createTable(namespace, table, TABLE_METADATA, true, options);
      }
    }
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  protected abstract AdminTestUtils getAdminTestUtils(String testName);

  @AfterAll
  public void afterAll() throws Exception {
    try {
      dropTables();
    } catch (Exception e) {
      logger.warn("Failed to drop tables", e);
    }

    try {
      if (admin != null) {
        admin.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close admin", e);
    }
  }

  private void dropTables() throws ExecutionException {
    for (String namespace : Arrays.asList(namespace1, namespace2)) {
      for (String table : Arrays.asList(TABLE1, TABLE2, TABLE3)) {
        admin.dropTable(namespace, table);
      }
      admin.dropNamespace(namespace);
    }
  }

  @Test
  public void getTableMetadata_CorrectTableGiven_ShouldReturnCorrectMetadata()
      throws ExecutionException {
    // Arrange
    if (isTimestampTypeSupported()) {
      admin.addNewColumnToTable(namespace1, TABLE1, COL_NAME15, DataType.TIMESTAMP);
    }

    // Act
    TableMetadata tableMetadata = admin.getTableMetadata(namespace1, TABLE1);

    // Assert
    assertThat(tableMetadata).isNotNull();

    assertThat(tableMetadata.getPartitionKeyNames().size()).isEqualTo(2);
    Iterator<String> iterator = tableMetadata.getPartitionKeyNames().iterator();
    assertThat(iterator.next()).isEqualTo(COL_NAME2);
    assertThat(iterator.next()).isEqualTo(COL_NAME1);

    assertThat(tableMetadata.getClusteringKeyNames().size()).isEqualTo(2);
    iterator = tableMetadata.getClusteringKeyNames().iterator();
    assertThat(iterator.next()).isEqualTo(COL_NAME4);
    assertThat(iterator.next()).isEqualTo(COL_NAME3);

    if (isTimestampTypeSupported()) {
      assertThat(tableMetadata.getColumnNames().size()).isEqualTo(15);
    } else {
      assertThat(tableMetadata.getColumnNames().size()).isEqualTo(14);
    }

    assertThat(tableMetadata.getColumnNames().contains(COL_NAME1)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME2)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME3)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME4)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME5)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME6)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME7)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME8)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME9)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME10)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME11)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME12)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME13)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME14)).isTrue();
    if (isTimestampTypeSupported()) {
      assertThat(tableMetadata.getColumnNames().contains(COL_NAME15)).isTrue();
    }

    assertThat(tableMetadata.getColumnDataType(COL_NAME1)).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME2)).isEqualTo(DataType.TEXT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME3)).isEqualTo(DataType.TEXT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME4)).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME5)).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME6)).isEqualTo(DataType.TEXT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME7)).isEqualTo(DataType.BIGINT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME8)).isEqualTo(DataType.FLOAT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME9)).isEqualTo(DataType.DOUBLE);
    assertThat(tableMetadata.getColumnDataType(COL_NAME10)).isEqualTo(DataType.BOOLEAN);
    assertThat(tableMetadata.getColumnDataType(COL_NAME11)).isEqualTo(DataType.BLOB);
    assertThat(tableMetadata.getColumnDataType(COL_NAME12)).isEqualTo(DataType.DATE);
    assertThat(tableMetadata.getColumnDataType(COL_NAME13)).isEqualTo(DataType.TIME);
    assertThat(tableMetadata.getColumnDataType(COL_NAME14)).isEqualTo(DataType.TIMESTAMPTZ);
    if (isTimestampTypeSupported()) {
      assertThat(tableMetadata.getColumnDataType(COL_NAME15)).isEqualTo(DataType.TIMESTAMP);
    }

    assertThat(tableMetadata.getClusteringOrder(COL_NAME1)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME2)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME3)).isEqualTo(Scan.Ordering.Order.DESC);
    assertThat(tableMetadata.getClusteringOrder(COL_NAME4)).isEqualTo(Scan.Ordering.Order.ASC);
    assertThat(tableMetadata.getClusteringOrder(COL_NAME5)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME6)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME7)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME8)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME9)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME10)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME11)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME12)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME13)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME14)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME15)).isNull();

    assertThat(tableMetadata.getSecondaryIndexNames().size()).isEqualTo(2);
    assertThat(tableMetadata.getSecondaryIndexNames().contains(COL_NAME5)).isTrue();
    assertThat(tableMetadata.getSecondaryIndexNames().contains(COL_NAME6)).isTrue();
  }

  @Test
  public void getTableMetadata_WrongTableGiven_ShouldReturnNull() throws ExecutionException {
    // Arrange

    // Act
    TableMetadata tableMetadata = admin.getTableMetadata("wrong_ns", "wrong_table");

    // Assert
    assertThat(tableMetadata).isNull();
  }

  @Test
  public void createNamespace_ForNonExistingNamespace_ShouldCreateNamespaceProperly()
      throws ExecutionException {
    try {
      // Arrange

      // Act
      admin.createNamespace(namespace3, getCreationOptions());

      // Assert
      assertThat(admin.namespaceExists(namespace3)).isTrue();
    } finally {
      admin.dropNamespace(namespace3, true);
    }
  }

  @Test
  public void createNamespace_ForExistingNamespace_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> admin.createNamespace(namespace1, getCreationOptions()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void createNamespace_IfNotExists_ForExistingNamespace_ShouldNotThrowAnyException() {
    // Arrange

    // Act Assert
    assertThatCode(() -> admin.createNamespace(namespace1, true, getCreationOptions()))
        .doesNotThrowAnyException();
  }

  @Test
  public void dropNamespace_ForNonExistingNamespace_ShouldDropNamespaceProperly()
      throws ExecutionException {
    try {
      // Arrange
      admin.createNamespace(namespace3, getCreationOptions());

      // Act
      admin.dropNamespace(namespace3);

      // Assert
      assertThat(admin.namespaceExists(namespace3)).isFalse();
    } finally {
      admin.dropNamespace(namespace3, true);
    }
  }

  @Test
  public void dropNamespace_ForNonExistingNamespace_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> admin.dropNamespace(namespace3))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void dropNamespace_ForNonEmptyNamespace_ShouldThrowIllegalArgumentException()
      throws ExecutionException {
    try {
      // Arrange
      admin.createNamespace(namespace3, getCreationOptions());
      admin.createTable(namespace3, TABLE1, TABLE_METADATA, getCreationOptions());

      // Act Assert
      assertThatThrownBy(() -> admin.dropNamespace(namespace3))
          .isInstanceOf(IllegalArgumentException.class);
    } finally {
      admin.dropTable(namespace3, TABLE1, true);
      admin.dropNamespace(namespace3, true);
    }
  }

  @Test
  public void dropNamespace_IfExists_ForNonExistingNamespace_ShouldNotThrowAnyException() {
    // Arrange

    // Act Assert
    assertThatCode(() -> admin.dropNamespace(namespace3, true)).doesNotThrowAnyException();
  }

  @Test
  public void createTable_ForNonExistingTable_ShouldCreateTableProperly()
      throws ExecutionException {
    try {
      // Arrange
      Map<String, String> options = getCreationOptions();

      // Act
      admin.createTable(namespace1, TABLE4, TABLE_METADATA, options);

      // Assert
      assertThat(admin.tableExists(namespace1, TABLE4)).isTrue();
    } finally {
      admin.dropTable(namespace1, TABLE4, true);
    }
  }

  @Test
  public void createTable_ForExistingTable_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(
            () -> admin.createTable(namespace1, TABLE1, TABLE_METADATA, getCreationOptions()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void createTable_ForNonExistingNamespace_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(
            () -> admin.createTable(namespace3, TABLE1, TABLE_METADATA, getCreationOptions()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void createTable_IfNotExists_ForExistingNamespace_ShouldNotThrowAnyException() {
    // Arrange

    // Act Assert
    assertThatCode(
            () -> admin.createTable(namespace1, TABLE1, TABLE_METADATA, true, getCreationOptions()))
        .doesNotThrowAnyException();
  }

  @Test
  public void dropTable_ForExistingTable_ShouldDropTableProperly() throws ExecutionException {
    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      admin.createTable(namespace1, TABLE4, TABLE_METADATA, options);

      // Act
      admin.dropTable(namespace1, TABLE4);

      // Assert
      assertThat(admin.tableExists(namespace1, TABLE4)).isFalse();
    } finally {
      admin.dropTable(namespace1, TABLE4, true);
    }
  }

  @Test
  public void dropTable_ForNonExistingTable_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> admin.dropTable(namespace1, TABLE4))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void dropTable_IfExists_ForNonExistingTable_ShouldNotThrowAnyException() {
    // Arrange

    // Act Assert
    assertThatCode(() -> admin.dropTable(namespace1, TABLE4, true)).doesNotThrowAnyException();
  }

  @Test
  public void truncateTable_ShouldTruncateProperly() throws ExecutionException, IOException {
    DistributedStorage storage = null;
    try {
      // Arrange
      Key partitionKey = new Key(COL_NAME2, "aaa", COL_NAME1, 1);
      Key clusteringKey = new Key(COL_NAME4, 2, COL_NAME3, "bbb");
      storage = storageFactory.getStorage();
      storage.put(
          new Put(partitionKey, clusteringKey)
              .withValue(COL_NAME5, 3)
              .withValue(COL_NAME6, "ccc")
              .withValue(COL_NAME7, 4L)
              .withValue(COL_NAME8, 1.0f)
              .withValue(COL_NAME9, 1.0d)
              .withValue(COL_NAME10, true)
              .withValue(COL_NAME11, "ddd".getBytes(StandardCharsets.UTF_8))
              .forNamespace(namespace1)
              .forTable(TABLE1));

      // Act
      admin.truncateTable(namespace1, TABLE1);

      // Assert
      Scanner scanner =
          storage.scan(new Scan(partitionKey).forNamespace(namespace1).forTable(TABLE1));
      assertThat(scanner.all()).isEmpty();
      scanner.close();
    } finally {
      if (storage != null) {
        storage.close();
      }
    }
  }

  @Test
  public void truncateTable_ForNonExistingTable_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> admin.truncateTable(namespace1, TABLE4))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void getNamespaceTableNames_ShouldReturnCorrectTables() throws ExecutionException {
    // Arrange

    // Act
    Set<String> actual = admin.getNamespaceTableNames(namespace1);

    // Assert
    assertThat(actual).isEqualTo(new HashSet<>(Arrays.asList(TABLE1, TABLE2, TABLE3)));
  }

  @Test
  public void namespaceExists_ShouldReturnCorrectResults() throws ExecutionException {
    // Arrange

    // Act Assert
    assertThat(admin.namespaceExists(namespace1)).isTrue();
    assertThat(admin.namespaceExists(namespace2)).isTrue();
    assertThat(admin.namespaceExists(namespace3)).isFalse();
  }

  @Test
  public void tableExists_ShouldReturnCorrectResults() throws ExecutionException {
    // Arrange

    // Act Assert
    assertThat(admin.tableExists(namespace1, TABLE1)).isTrue();
    assertThat(admin.tableExists(namespace1, TABLE2)).isTrue();
    assertThat(admin.tableExists(namespace1, TABLE3)).isTrue();
    assertThat(admin.tableExists(namespace1, TABLE4)).isFalse();
  }

  @Test
  public void createIndex_ForAllDataTypesWithExistingData_ShouldCreateIndexesCorrectly()
      throws ExecutionException {
    DistributedStorage storage = null;
    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata.Builder metadataBuilder =
          TableMetadata.newBuilder()
              .addColumn(COL_NAME1, DataType.INT)
              .addColumn(COL_NAME2, DataType.INT)
              .addColumn(COL_NAME3, DataType.TEXT)
              .addColumn(COL_NAME4, DataType.BIGINT)
              .addColumn(COL_NAME5, DataType.FLOAT)
              .addColumn(COL_NAME6, DataType.DOUBLE)
              .addColumn(COL_NAME7, DataType.BOOLEAN)
              .addColumn(COL_NAME8, DataType.BLOB)
              .addColumn(COL_NAME9, DataType.TEXT)
              .addColumn(COL_NAME10, DataType.DATE)
              .addColumn(COL_NAME11, DataType.TIME)
              .addColumn(COL_NAME12, DataType.TIMESTAMPTZ)
              .addPartitionKey(COL_NAME1)
              .addSecondaryIndex(COL_NAME9);
      if (isTimestampTypeSupported()) {
        metadataBuilder = metadataBuilder.addColumn(COL_NAME13, DataType.TIMESTAMP);
      }
      TableMetadata metadata = metadataBuilder.build();
      admin.createTable(namespace1, TABLE4, metadata, options);
      storage = storageFactory.getStorage();
      PutBuilder.Buildable put =
          Put.newBuilder()
              .namespace(namespace1)
              .table(TABLE4)
              .partitionKey(Key.ofInt(COL_NAME1, 1))
              .intValue(COL_NAME2, 2)
              .textValue(COL_NAME3, "3")
              .bigIntValue(COL_NAME4, 4)
              .floatValue(COL_NAME5, 5)
              .doubleValue(COL_NAME6, 6)
              .booleanValue(COL_NAME7, true)
              .blobValue(COL_NAME8, "8".getBytes(StandardCharsets.UTF_8))
              .textValue(COL_NAME9, "9")
              .dateValue(COL_NAME10, LocalDate.of(2020, 6, 2))
              .timeValue(COL_NAME11, LocalTime.of(12, 2, 6, 123_456_000))
              .timestampTZValue(
                  COL_NAME12,
                  LocalDateTime.of(LocalDate.of(2020, 6, 2), LocalTime.of(12, 2, 6, 123_000_000))
                      .toInstant(ZoneOffset.UTC));
      if (isTimestampTypeSupported()) {
        put.timestampValue(
            COL_NAME13,
            LocalDateTime.of(LocalDate.of(2020, 6, 2), LocalTime.of(12, 2, 6, 123_000_000)));
      }
      storage.put(put.build());

      // Act
      admin.createIndex(namespace1, TABLE4, COL_NAME2, options);
      admin.createIndex(namespace1, TABLE4, COL_NAME4, options);
      admin.createIndex(namespace1, TABLE4, COL_NAME5, options);
      admin.createIndex(namespace1, TABLE4, COL_NAME6, options);
      if (isIndexOnBooleanColumnSupported()) {
        admin.createIndex(namespace1, TABLE4, COL_NAME7, options);
      }
      admin.createIndex(namespace1, TABLE4, COL_NAME10, options);
      admin.createIndex(namespace1, TABLE4, COL_NAME11, options);
      admin.createIndex(namespace1, TABLE4, COL_NAME12, options);
      if (isTimestampTypeSupported()) {
        admin.createIndex(namespace1, TABLE4, COL_NAME13, options);
      }
      if (isCreateIndexOnTextAndBlobColumnsEnabled()) {
        admin.createIndex(namespace1, TABLE4, COL_NAME3, options);
        admin.createIndex(namespace1, TABLE4, COL_NAME8, options);
      }

      // Assert
      assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME2)).isTrue();
      assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME4)).isTrue();
      assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME5)).isTrue();
      assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME6)).isTrue();
      if (isIndexOnBooleanColumnSupported()) {
        assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME7)).isTrue();
      }
      assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME9)).isTrue();
      assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME10)).isTrue();
      assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME11)).isTrue();
      assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME12)).isTrue();
      if (isTimestampTypeSupported()) {
        assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME13)).isTrue();
      }
      if (isCreateIndexOnTextAndBlobColumnsEnabled()) {
        assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME3)).isTrue();
        assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME8)).isTrue();
      }

      Set<String> actualSecondaryIndexNames =
          admin.getTableMetadata(namespace1, TABLE4).getSecondaryIndexNames();
      assertThat(actualSecondaryIndexNames)
          .contains(COL_NAME2, COL_NAME4, COL_NAME5, COL_NAME9, COL_NAME10, COL_NAME11, COL_NAME12);
      int indexCount = 8;
      if (isIndexOnBooleanColumnSupported()) {
        assertThat(actualSecondaryIndexNames).contains(COL_NAME7);
        indexCount++;
      }
      if (isTimestampTypeSupported()) {
        assertThat(actualSecondaryIndexNames).contains(COL_NAME13);
        indexCount++;
      }
      if (isCreateIndexOnTextAndBlobColumnsEnabled()) {
        assertThat(actualSecondaryIndexNames).contains(COL_NAME3, COL_NAME8);
        indexCount += 2;
      }
      assertThat(actualSecondaryIndexNames).hasSize(indexCount);

    } finally {
      admin.dropTable(namespace1, TABLE4, true);
      if (storage != null) {
        storage.close();
      }
    }
  }

  @Test
  public void createIndex_ForNonExistingTable_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(
            () ->
                admin.createIndex(
                    namespace1, "non-existing_table", COL_NAME2, getCreationOptions()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void createIndex_ForNonExistingColumn_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(
            () ->
                admin.createIndex(namespace1, TABLE1, "non-existing_column", getCreationOptions()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void createIndex_ForAlreadyExistingIndex_ShouldThrowIllegalArgumentException()
      throws ExecutionException {
    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata metadata =
          TableMetadata.newBuilder()
              .addColumn(COL_NAME1, DataType.INT)
              .addColumn(COL_NAME2, DataType.INT)
              .addPartitionKey(COL_NAME1)
              .addSecondaryIndex(COL_NAME2)
              .build();
      admin.createTable(namespace1, TABLE4, metadata, options);

      // Act Assert
      assertThatThrownBy(
              () -> admin.createIndex(namespace1, TABLE4, COL_NAME2, getCreationOptions()))
          .isInstanceOf(IllegalArgumentException.class);
    } finally {
      admin.dropTable(namespace1, TABLE4, true);
    }
  }

  @Test
  public void createIndex_IfNotExists_ForAlreadyExistingIndex_ShouldNotThrowAnyException()
      throws ExecutionException {
    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata metadata =
          TableMetadata.newBuilder()
              .addColumn(COL_NAME1, DataType.INT)
              .addColumn(COL_NAME2, DataType.INT)
              .addPartitionKey(COL_NAME1)
              .addSecondaryIndex(COL_NAME2)
              .build();
      admin.createTable(namespace1, TABLE4, metadata, options);

      // Act Assert
      assertThatCode(
              () -> admin.createIndex(namespace1, TABLE4, COL_NAME2, true, getCreationOptions()))
          .doesNotThrowAnyException();
    } finally {
      admin.dropTable(namespace1, TABLE4, true);
    }
  }

  @Test
  public void dropIndex_ForAllDataTypesWithExistingData_ShouldDropIndexCorrectly()
      throws ExecutionException {
    DistributedStorage storage = null;
    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata.Builder metadataBuilder =
          TableMetadata.newBuilder()
              .addColumn(COL_NAME1, DataType.INT)
              .addColumn(COL_NAME2, DataType.INT)
              .addColumn(COL_NAME3, DataType.TEXT)
              .addColumn(COL_NAME4, DataType.BIGINT)
              .addColumn(COL_NAME5, DataType.FLOAT)
              .addColumn(COL_NAME6, DataType.DOUBLE)
              .addColumn(COL_NAME7, DataType.BOOLEAN)
              .addColumn(COL_NAME8, DataType.BLOB)
              .addColumn(COL_NAME9, DataType.TEXT)
              .addColumn(COL_NAME10, DataType.DATE)
              .addColumn(COL_NAME11, DataType.TIME)
              .addColumn(COL_NAME12, DataType.TIMESTAMPTZ)
              .addPartitionKey(COL_NAME1)
              .addSecondaryIndex(COL_NAME2)
              .addSecondaryIndex(COL_NAME3)
              .addSecondaryIndex(COL_NAME4)
              .addSecondaryIndex(COL_NAME5)
              .addSecondaryIndex(COL_NAME6)
              .addSecondaryIndex(COL_NAME8)
              .addSecondaryIndex(COL_NAME9)
              .addSecondaryIndex(COL_NAME10)
              .addSecondaryIndex(COL_NAME11)
              .addSecondaryIndex(COL_NAME12);
      if (isIndexOnBooleanColumnSupported()) {
        metadataBuilder.addSecondaryIndex(COL_NAME7).build();
      }
      if (isTimestampTypeSupported()) {
        metadataBuilder.addColumn(COL_NAME13, DataType.TIMESTAMP);
        metadataBuilder.addSecondaryIndex(COL_NAME13);
      }
      admin.createTable(namespace1, TABLE4, metadataBuilder.build(), options);
      storage = storageFactory.getStorage();
      PutBuilder.Buildable put =
          Put.newBuilder()
              .namespace(namespace1)
              .table(TABLE4)
              .partitionKey(Key.ofInt(COL_NAME1, 1))
              .intValue(COL_NAME2, 2)
              .textValue(COL_NAME3, "3")
              .bigIntValue(COL_NAME4, 4)
              .floatValue(COL_NAME5, 5)
              .doubleValue(COL_NAME6, 6)
              .booleanValue(COL_NAME7, true)
              .blobValue(COL_NAME8, "8".getBytes(StandardCharsets.UTF_8))
              .textValue(COL_NAME9, "9")
              .dateValue(COL_NAME10, LocalDate.of(2020, 6, 2))
              .timeValue(COL_NAME11, LocalTime.of(12, 2, 6, 123_456_000))
              .timestampTZValue(
                  COL_NAME12,
                  LocalDateTime.of(LocalDate.of(2020, 6, 2), LocalTime.of(12, 2, 6, 123_000_000))
                      .toInstant(ZoneOffset.UTC));
      if (isTimestampTypeSupported()) {
        put.timestampValue(
            COL_NAME13,
            LocalDateTime.of(LocalDate.of(2020, 6, 2), LocalTime.of(12, 2, 6, 123_000_000)));
      }
      storage.put(put.build());

      // Act
      admin.dropIndex(namespace1, TABLE4, COL_NAME2);
      admin.dropIndex(namespace1, TABLE4, COL_NAME3);
      admin.dropIndex(namespace1, TABLE4, COL_NAME4);
      admin.dropIndex(namespace1, TABLE4, COL_NAME5);
      admin.dropIndex(namespace1, TABLE4, COL_NAME6);
      if (isIndexOnBooleanColumnSupported()) {
        admin.dropIndex(namespace1, TABLE4, COL_NAME7);
      }
      admin.dropIndex(namespace1, TABLE4, COL_NAME8);
      admin.dropIndex(namespace1, TABLE4, COL_NAME10);
      admin.dropIndex(namespace1, TABLE4, COL_NAME11);
      admin.dropIndex(namespace1, TABLE4, COL_NAME12);
      if (isTimestampTypeSupported()) {
        admin.dropIndex(namespace1, TABLE4, COL_NAME13);
      }

      // Assert
      assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME2)).isFalse();
      assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME3)).isFalse();
      assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME4)).isFalse();
      assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME5)).isFalse();
      assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME6)).isFalse();
      assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME7)).isFalse();
      assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME8)).isFalse();
      assertThat(admin.getTableMetadata(namespace1, TABLE4).getSecondaryIndexNames())
          .containsOnly(COL_NAME9);
      assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME10)).isFalse();
      assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME11)).isFalse();
      assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME12)).isFalse();
      if (isTimestampTypeSupported()) {
        assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME13)).isFalse();
      }
    } finally {
      admin.dropTable(namespace1, TABLE4, true);
      if (storage != null) {
        storage.close();
      }
    }
  }

  @Test
  public void dropIndex_ForNonExistingTable_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> admin.dropIndex(namespace1, "non-existing-table", COL_NAME2))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void dropIndex_ForNonExistingIndex_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> admin.dropIndex(namespace1, TABLE1, COL_NAME2))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void dropIndex_IfExists_ForNonExistingIndex_ShouldNotThrowAnyException() {
    // Arrange

    // Act Assert
    assertThatCode(() -> admin.dropIndex(namespace1, TABLE1, COL_NAME2, true))
        .doesNotThrowAnyException();
  }

  @Test
  public void addNewColumnToTable_AddColumnForEachExistingDataType_ShouldAddNewColumnsCorrectly()
      throws ExecutionException {
    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata currentTableMetadata =
          TableMetadata.newBuilder()
              .addPartitionKey("pk1")
              .addColumn("pk1", DataType.TEXT)
              .addColumn("c1", DataType.TEXT)
              .build();
      admin.createTable(namespace1, TABLE4, currentTableMetadata, options);

      // Act
      admin.addNewColumnToTable(namespace1, TABLE4, "c2", DataType.TEXT);
      admin.addNewColumnToTable(namespace1, TABLE4, "c3", DataType.DOUBLE);
      admin.addNewColumnToTable(namespace1, TABLE4, "c4", DataType.INT);
      admin.addNewColumnToTable(namespace1, TABLE4, "c5", DataType.BIGINT);
      admin.addNewColumnToTable(namespace1, TABLE4, "c6", DataType.BLOB);
      admin.addNewColumnToTable(namespace1, TABLE4, "c7", DataType.BOOLEAN);
      admin.addNewColumnToTable(namespace1, TABLE4, "c8", DataType.FLOAT);

      // Assert
      TableMetadata expectedTableMetadata =
          TableMetadata.newBuilder(currentTableMetadata)
              .addColumn("c2", DataType.TEXT)
              .addColumn("c3", DataType.DOUBLE)
              .addColumn("c4", DataType.INT)
              .addColumn("c5", DataType.BIGINT)
              .addColumn("c6", DataType.BLOB)
              .addColumn("c7", DataType.BOOLEAN)
              .addColumn("c8", DataType.FLOAT)
              .build();
      assertThat(admin.getTableMetadata(namespace1, TABLE4)).isEqualTo(expectedTableMetadata);
    } finally {
      admin.dropTable(namespace1, TABLE4, true);
    }
  }

  @Test
  public void getNamespaceNames_ShouldReturnCreatedNamespaces() throws ExecutionException {
    // Arrange

    // Act
    Set<String> namespaces = admin.getNamespaceNames();

    // Assert
    assertThat(namespaces).containsOnly(namespace1, namespace2, systemNamespaceName);
  }

  @Test
  public void addNewColumnToTable_ForNonExistingTable_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(
            () -> admin.addNewColumnToTable(namespace1, TABLE4, COL_NAME2, DataType.TEXT))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void addNewColumnToTable_ForAlreadyExistingColumn_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(
            () -> admin.addNewColumnToTable(namespace1, TABLE1, COL_NAME2, DataType.TEXT))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      upgrade_WhenMetadataTableExistsButNotNamespacesTable_ShouldCreateNamespacesTableAndImportExistingNamespaces()
          throws Exception {
    AdminTestUtils adminTestUtils = getAdminTestUtils(TEST_NAME);
    try {
      // Arrange
      adminTestUtils.dropNamespacesTable();

      // Act
      admin.upgrade(getCreationOptions());

      // Assert
      assertThat(admin.getNamespaceNames())
          .containsOnly(namespace1, namespace2, systemNamespaceName);
    } finally {
      adminTestUtils.close();
    }
  }

  protected boolean isIndexOnBooleanColumnSupported() {
    return true;
  }

  protected boolean isTimestampTypeSupported() {
    return true;
  }

  protected boolean isCreateIndexOnTextAndBlobColumnsEnabled() {
    return true;
  }
}
