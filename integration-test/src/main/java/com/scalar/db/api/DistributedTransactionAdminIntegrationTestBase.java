package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;
import com.scalar.db.util.AdminTestUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DistributedTransactionAdminIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(DistributedTransactionAdminIntegrationTestBase.class);

  protected static final String NAMESPACE_BASE_NAME = "int_test_";
  protected static final String TABLE1 = "test_table1";
  protected static final String TABLE2 = "test_table2";
  protected static final String TABLE3 = "test_table3";
  protected static final String TABLE4 = "test_table4";
  protected static final String COL_NAME1 = "c1";
  protected static final String COL_NAME2 = "c2";
  protected static final String COL_NAME3 = "c3";
  protected static final String COL_NAME4 = "c4";
  protected static final String COL_NAME5 = "c5";
  protected static final String COL_NAME6 = "c6";
  protected static final String COL_NAME7 = "c7";
  protected static final String COL_NAME8 = "c8";
  protected static final String COL_NAME9 = "c9";
  protected static final String COL_NAME10 = "c10";
  protected static final String COL_NAME11 = "c11";
  private static final String COL_NAME12 = "c12";
  private static final String COL_NAME13 = "c13";
  private static final String COL_NAME14 = "c14";
  private static final String COL_NAME15 = "c15";

  protected static final TableMetadata TABLE_METADATA =
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
  protected TransactionFactory transactionFactory;
  protected DistributedTransactionAdmin admin;
  protected String namespace1;
  protected String namespace2;
  protected String namespace3;
  protected String systemNamespaceName;

  @BeforeAll
  public void beforeAll() throws Exception {
    String testName = getTestName();
    initialize(testName);
    Properties properties = getProperties(testName);
    transactionFactory = TransactionFactory.create(properties);
    admin = transactionFactory.getTransactionAdmin();
    namespace1 = getNamespaceBaseName() + testName + "1";
    namespace2 = getNamespaceBaseName() + testName + "2";
    namespace3 = getNamespaceBaseName() + testName + "3";
    systemNamespaceName = DatabaseConfig.getSystemNamespaceName(properties);
    createTables();
  }

  protected abstract String getTestName();

  protected void initialize(String testName) throws Exception {}

  protected abstract Properties getProperties(String testName);

  protected String getNamespaceBaseName() {
    return NAMESPACE_BASE_NAME;
  }

  private void createTables() throws ExecutionException {
    Map<String, String> options = getCreationOptions();
    for (String namespace : Arrays.asList(namespace1, namespace2)) {
      admin.createNamespace(namespace, true, options);
      for (String table : Arrays.asList(TABLE1, TABLE2, TABLE3)) {
        admin.createTable(namespace, table, TABLE_METADATA, true, options);
      }
    }
    admin.createCoordinatorTables(true, options);
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
    admin.dropCoordinatorTables();
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
  public void truncateTable_ShouldTruncateProperly()
      throws ExecutionException, TransactionException {
    // Use a separate table name to avoid hitting the stale cache, which can cause test failure when
    // executing DMLs
    String table = "table_for_truncate";

    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      admin.createTable(namespace1, table, TABLE_METADATA, true, options);
      Key partitionKey = Key.of(COL_NAME2, "aaa", COL_NAME1, 1);
      Key clusteringKey = Key.of(COL_NAME4, 2, COL_NAME3, "bbb");
      transactionalInsert(
          Insert.newBuilder()
              .namespace(namespace1)
              .table(table)
              .partitionKey(partitionKey)
              .clusteringKey(clusteringKey)
              .intValue(COL_NAME5, 3)
              .textValue(COL_NAME6, "ccc")
              .bigIntValue(COL_NAME7, 4L)
              .floatValue(COL_NAME8, 1.0f)
              .doubleValue(COL_NAME9, 1.0d)
              .booleanValue(COL_NAME10, true)
              .blobValue(COL_NAME11, "ddd".getBytes(StandardCharsets.UTF_8))
              .build());

      // Act
      admin.truncateTable(namespace1, table);

      // Assert
      List<Result> results =
          transactionalScan(
              Scan.newBuilder()
                  .namespace(namespace1)
                  .table(table)
                  .partitionKey(partitionKey)
                  .build());
      assertThat(results).isEmpty();
    } finally {
      admin.dropTable(namespace1, table, true);
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
      throws Exception {
    // Use a separate table name to avoid hitting the stale cache, which can cause test failure when
    // executing DMLs
    String table = "table_for_create_index";

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
      admin.createTable(namespace1, table, metadata, options);
      InsertBuilder.Buildable insert =
          Insert.newBuilder()
              .namespace(namespace1)
              .table(table)
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
        insert.timestampValue(
            COL_NAME13,
            LocalDateTime.of(LocalDate.of(2020, 6, 2), LocalTime.of(12, 2, 6, 123_000_000)));
      }
      transactionalInsert(insert.build());

      // Act
      admin.createIndex(namespace1, table, COL_NAME2, options);
      if (isCreateIndexOnTextColumnEnabled()) {
        admin.createIndex(namespace1, table, COL_NAME3, options);
      }
      admin.createIndex(namespace1, table, COL_NAME4, options);
      admin.createIndex(namespace1, table, COL_NAME5, options);
      admin.createIndex(namespace1, table, COL_NAME6, options);
      if (isIndexOnBooleanColumnSupported()) {
        admin.createIndex(namespace1, table, COL_NAME7, options);
      }
      if (isIndexOnBlobColumnSupported()) {
        admin.createIndex(namespace1, table, COL_NAME8, options);
      }
      admin.createIndex(namespace1, table, COL_NAME10, options);
      admin.createIndex(namespace1, table, COL_NAME11, options);
      admin.createIndex(namespace1, table, COL_NAME12, options);
      if (isTimestampTypeSupported()) {
        admin.createIndex(namespace1, table, COL_NAME13, options);
      }

      // Assert
      assertThat(admin.indexExists(namespace1, table, COL_NAME2)).isTrue();
      if (isCreateIndexOnTextColumnEnabled()) {
        assertThat(admin.indexExists(namespace1, table, COL_NAME3)).isTrue();
      }
      assertThat(admin.indexExists(namespace1, table, COL_NAME4)).isTrue();
      assertThat(admin.indexExists(namespace1, table, COL_NAME5)).isTrue();
      assertThat(admin.indexExists(namespace1, table, COL_NAME6)).isTrue();
      if (isIndexOnBooleanColumnSupported()) {
        assertThat(admin.indexExists(namespace1, table, COL_NAME7)).isTrue();
      }
      if (isIndexOnBlobColumnSupported()) {
        assertThat(admin.indexExists(namespace1, table, COL_NAME8)).isTrue();
      }
      assertThat(admin.indexExists(namespace1, table, COL_NAME9)).isTrue();
      assertThat(admin.indexExists(namespace1, table, COL_NAME10)).isTrue();
      assertThat(admin.indexExists(namespace1, table, COL_NAME11)).isTrue();
      assertThat(admin.indexExists(namespace1, table, COL_NAME12)).isTrue();
      if (isTimestampTypeSupported()) {
        assertThat(admin.indexExists(namespace1, table, COL_NAME13)).isTrue();
      }

      Set<String> actualSecondaryIndexNames =
          admin.getTableMetadata(namespace1, table).getSecondaryIndexNames();
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
      if (isCreateIndexOnTextColumnEnabled()) {
        assertThat(actualSecondaryIndexNames).contains(COL_NAME3);
        indexCount += 1;
      }
      if (isIndexOnBlobColumnSupported()) {
        assertThat(actualSecondaryIndexNames).contains(COL_NAME8);
        indexCount += 1;
      }
      assertThat(actualSecondaryIndexNames).hasSize(indexCount);
    } finally {
      admin.dropTable(namespace1, table, true);
    }
  }

  @Test
  public void createIndex_ForNonExistingTable_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> admin.createIndex(namespace1, TABLE4, COL_NAME2, getCreationOptions()))
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
      throws Exception {
    // Use a separate table name to avoid hitting the stale cache, which can cause test failure when
    // executing DMLs
    String table = "table_for_drop_index";

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
              .addSecondaryIndex(COL_NAME9)
              .addSecondaryIndex(COL_NAME9)
              .addSecondaryIndex(COL_NAME10)
              .addSecondaryIndex(COL_NAME11)
              .addSecondaryIndex(COL_NAME12);
      if (isIndexOnBooleanColumnSupported()) {
        metadataBuilder = metadataBuilder.addSecondaryIndex(COL_NAME7);
      }
      if (isIndexOnBlobColumnSupported()) {
        metadataBuilder = metadataBuilder.addSecondaryIndex(COL_NAME8);
      }
      if (isTimestampTypeSupported()) {
        metadataBuilder.addColumn(COL_NAME13, DataType.TIMESTAMP);
        metadataBuilder.addSecondaryIndex(COL_NAME13);
      }
      admin.createTable(namespace1, table, metadataBuilder.build(), options);

      InsertBuilder.Buildable insert =
          Insert.newBuilder()
              .namespace(namespace1)
              .table(table)
              .partitionKey(Key.ofInt(COL_NAME1, 1))
              .intValue(COL_NAME2, 2)
              .textValue(COL_NAME3, "3")
              .bigIntValue(COL_NAME4, 4)
              .floatValue(COL_NAME5, 5)
              .doubleValue(COL_NAME6, 6)
              .booleanValue(COL_NAME7, true)
              .blobValue(COL_NAME8, "8".getBytes(StandardCharsets.UTF_8))
              .textValue(COL_NAME9, "9")
              .timeValue(COL_NAME11, LocalTime.of(12, 2, 6, 123_456_000))
              .timestampTZValue(
                  COL_NAME12,
                  LocalDateTime.of(LocalDate.of(2020, 6, 2), LocalTime.of(12, 2, 6, 123_000_000))
                      .toInstant(ZoneOffset.UTC));
      if (isTimestampTypeSupported()) {
        insert.timestampValue(
            COL_NAME13,
            LocalDateTime.of(LocalDate.of(2020, 6, 2), LocalTime.of(12, 2, 6, 123_000_000)));
      }
      transactionalInsert(insert.build());

      // Act
      admin.dropIndex(namespace1, table, COL_NAME2);
      admin.dropIndex(namespace1, table, COL_NAME3);
      admin.dropIndex(namespace1, table, COL_NAME4);
      admin.dropIndex(namespace1, table, COL_NAME5);
      admin.dropIndex(namespace1, table, COL_NAME6);
      if (isIndexOnBooleanColumnSupported()) {
        admin.dropIndex(namespace1, table, COL_NAME7);
      }
      if (isIndexOnBlobColumnSupported()) {
        admin.dropIndex(namespace1, table, COL_NAME8);
      }
      admin.dropIndex(namespace1, table, COL_NAME10);
      admin.dropIndex(namespace1, table, COL_NAME11);
      admin.dropIndex(namespace1, table, COL_NAME12);
      if (isTimestampTypeSupported()) {
        admin.dropIndex(namespace1, table, COL_NAME13);
      }

      // Assert
      assertThat(admin.indexExists(namespace1, table, COL_NAME2)).isFalse();
      assertThat(admin.indexExists(namespace1, table, COL_NAME3)).isFalse();
      assertThat(admin.indexExists(namespace1, table, COL_NAME4)).isFalse();
      assertThat(admin.indexExists(namespace1, table, COL_NAME5)).isFalse();
      assertThat(admin.indexExists(namespace1, table, COL_NAME6)).isFalse();
      assertThat(admin.indexExists(namespace1, table, COL_NAME7)).isFalse();
      assertThat(admin.indexExists(namespace1, table, COL_NAME8)).isFalse();
      assertThat(admin.getTableMetadata(namespace1, table).getSecondaryIndexNames())
          .containsOnly(COL_NAME9);
      assertThat(admin.indexExists(namespace1, table, COL_NAME10)).isFalse();
      assertThat(admin.indexExists(namespace1, table, COL_NAME11)).isFalse();
      assertThat(admin.indexExists(namespace1, table, COL_NAME12)).isFalse();
      if (isTimestampTypeSupported()) {
        assertThat(admin.indexExists(namespace1, table, COL_NAME13)).isFalse();
      }
    } finally {
      admin.dropTable(namespace1, table, true);
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
      addNewColumnToTable_IfNotExists_ForAlreadyExistingColumn_ShouldNotThrowAnyException() {
    // Arrange

    // Act Assert
    assertThatCode(
            () ->
                admin.addNewColumnToTable(
                    namespace1, TABLE1, COL_NAME7, DataType.TEXT, false, true))
        .doesNotThrowAnyException();
  }

  @Test
  public void dropColumnFromTable_DropColumnForEachExistingDataType_ShouldDropColumnsCorrectly()
      throws ExecutionException {
    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata.Builder currentTableMetadataBuilder =
          TableMetadata.newBuilder()
              .addColumn("c1", DataType.INT)
              .addColumn("c2", DataType.INT)
              .addColumn("c3", DataType.INT)
              .addColumn("c4", DataType.BIGINT)
              .addColumn("c5", DataType.FLOAT)
              .addColumn("c6", DataType.DOUBLE)
              .addColumn("c7", DataType.TEXT)
              .addColumn("c8", DataType.BLOB)
              .addColumn("c9", DataType.DATE)
              .addColumn("c10", DataType.TIME)
              .addPartitionKey("c1")
              .addClusteringKey("c2", Scan.Ordering.Order.ASC);
      if (isTimestampTypeSupported()) {
        currentTableMetadataBuilder
            .addColumn("c11", DataType.TIMESTAMP)
            .addColumn("c12", DataType.TIMESTAMPTZ);
      }
      TableMetadata currentTableMetadata = currentTableMetadataBuilder.build();
      admin.createTable(namespace1, TABLE4, currentTableMetadata, options);

      // Act
      admin.dropColumnFromTable(namespace1, TABLE4, "c3");
      admin.dropColumnFromTable(namespace1, TABLE4, "c4");
      admin.dropColumnFromTable(namespace1, TABLE4, "c5");
      admin.dropColumnFromTable(namespace1, TABLE4, "c6");
      admin.dropColumnFromTable(namespace1, TABLE4, "c7");
      admin.dropColumnFromTable(namespace1, TABLE4, "c8");
      admin.dropColumnFromTable(namespace1, TABLE4, "c9");
      admin.dropColumnFromTable(namespace1, TABLE4, "c10");
      if (isTimestampTypeSupported()) {
        admin.dropColumnFromTable(namespace1, TABLE4, "c11");
        admin.dropColumnFromTable(namespace1, TABLE4, "c12");
      }

      // Assert
      TableMetadata.Builder expectedTableMetadataBuilder =
          TableMetadata.newBuilder(currentTableMetadata)
              .removeColumn("c3")
              .removeColumn("c4")
              .removeColumn("c5")
              .removeColumn("c6")
              .removeColumn("c7")
              .removeColumn("c8")
              .removeColumn("c9")
              .removeColumn("c10")
              .removeColumn("c11")
              .removeColumn("c12");
      TableMetadata expectedTableMetadata = expectedTableMetadataBuilder.build();
      assertThat(admin.getTableMetadata(namespace1, TABLE4)).isEqualTo(expectedTableMetadata);
    } finally {
      admin.dropTable(namespace1, TABLE4, true);
    }
  }

  @Test
  public void dropColumnFromTable_ForNonExistingTable_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> admin.dropColumnFromTable(namespace1, TABLE4, COL_NAME2))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void dropColumnFromTable_ForNonExistingColumn_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> admin.dropColumnFromTable(namespace1, TABLE1, "nonExistingColumn"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void dropColumnFromTable_ForPrimaryKeyColumn_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> admin.dropColumnFromTable(namespace1, TABLE1, "c1"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> admin.dropColumnFromTable(namespace1, TABLE1, "c3"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void dropColumnFromTable_ForIndexedColumn_ShouldDropColumnAndIndexCorrectly()
      throws ExecutionException {
    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata currentTableMetadata =
          TableMetadata.newBuilder()
              .addColumn("c1", DataType.INT)
              .addColumn("c2", DataType.INT)
              .addColumn("c3", DataType.TEXT)
              .addPartitionKey("c1")
              .addSecondaryIndex("c2")
              .build();
      admin.createTable(namespace1, TABLE4, currentTableMetadata, options);

      // Act
      admin.dropColumnFromTable(namespace1, TABLE4, "c2");

      // Assert
      TableMetadata expectedTableMetadata =
          TableMetadata.newBuilder()
              .addColumn("c1", DataType.INT)
              .addColumn("c3", DataType.TEXT)
              .addPartitionKey("c1")
              .build();
      assertThat(admin.getTableMetadata(namespace1, TABLE4)).isEqualTo(expectedTableMetadata);
      assertThat(admin.indexExists(namespace1, TABLE4, "c2")).isFalse();
    } finally {
      admin.dropTable(namespace1, TABLE4, true);
    }
  }

  @Test
  public void dropColumnFromTable_IfNotExists_ForNonExistingColumn_ShouldNotThrowAnyException() {
    // Arrange

    // Act Assert
    assertThatCode(() -> admin.dropColumnFromTable(namespace1, TABLE1, "nonExistingColumn", true))
        .doesNotThrowAnyException();
  }

  @Test
  public void renameColumn_ShouldRenameColumnCorrectly() throws ExecutionException {
    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata currentTableMetadata =
          TableMetadata.newBuilder()
              .addColumn("c1", DataType.INT)
              .addColumn("c2", DataType.INT)
              .addPartitionKey("c1")
              .build();
      admin.createTable(namespace1, TABLE4, currentTableMetadata, options);

      // Act
      admin.renameColumn(namespace1, TABLE4, "c2", "c3");

      // Assert
      TableMetadata expectedTableMetadata =
          TableMetadata.newBuilder()
              .addColumn("c1", DataType.INT)
              .addColumn("c3", DataType.INT)
              .addPartitionKey("c1")
              .build();
      assertThat(admin.getTableMetadata(namespace1, TABLE4)).isEqualTo(expectedTableMetadata);
    } finally {
      admin.dropTable(namespace1, TABLE4, true);
    }
  }

  @Test
  public void renameColumn_ForNonExistingTable_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> admin.renameColumn(namespace1, TABLE4, "c2", "c3"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void renameColumn_ForNonExistingColumn_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> admin.renameColumn(namespace1, TABLE1, "nonExistingColumn", "c3"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void renameColumn_ForPrimaryKeyColumn_ShouldRenameColumnCorrectly()
      throws ExecutionException {
    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata currentTableMetadata =
          TableMetadata.newBuilder()
              .addColumn("c1", DataType.INT)
              .addColumn("c2", DataType.INT)
              .addColumn("c3", DataType.TEXT)
              .addColumn("c4", DataType.INT)
              .addColumn("c5", DataType.INT)
              .addPartitionKey("c1")
              .addPartitionKey("c2")
              .addClusteringKey("c3")
              .addClusteringKey("c4")
              .build();
      admin.createTable(namespace1, TABLE4, currentTableMetadata, options);

      // Act
      admin.renameColumn(namespace1, TABLE4, "c1", "c6");
      admin.renameColumn(namespace1, TABLE4, "c3", "c7");

      // Assert
      TableMetadata expectedTableMetadata =
          TableMetadata.newBuilder()
              .addColumn("c6", DataType.INT)
              .addColumn("c2", DataType.INT)
              .addColumn("c7", DataType.TEXT)
              .addColumn("c4", DataType.INT)
              .addColumn("c5", DataType.INT)
              .addPartitionKey("c6")
              .addPartitionKey("c2")
              .addClusteringKey("c7")
              .addClusteringKey("c4")
              .build();
      assertThat(admin.getTableMetadata(namespace1, TABLE4)).isEqualTo(expectedTableMetadata);
    } finally {
      admin.dropTable(namespace1, TABLE4, true);
    }
  }

  @Test
  public void renameColumn_ForIndexKeyColumn_ShouldRenameColumnAndIndexCorrectly()
      throws ExecutionException {
    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata currentTableMetadata =
          TableMetadata.newBuilder()
              .addColumn("c1", DataType.INT)
              .addColumn("c2", DataType.INT)
              .addColumn("c3", DataType.TEXT)
              .addPartitionKey("c1")
              .addClusteringKey("c2")
              .addSecondaryIndex("c3")
              .build();
      admin.createTable(namespace1, TABLE4, currentTableMetadata, options);

      // Act
      admin.renameColumn(namespace1, TABLE4, "c3", "c4");

      // Assert
      TableMetadata expectedTableMetadata =
          TableMetadata.newBuilder()
              .addColumn("c1", DataType.INT)
              .addColumn("c2", DataType.INT)
              .addColumn("c4", DataType.TEXT)
              .addPartitionKey("c1")
              .addClusteringKey("c2")
              .addSecondaryIndex("c4")
              .build();
      assertThat(admin.getTableMetadata(namespace1, TABLE4)).isEqualTo(expectedTableMetadata);
      assertThat(admin.indexExists(namespace1, TABLE4, "c3")).isFalse();
      assertThat(admin.indexExists(namespace1, TABLE4, "c4")).isTrue();
    } finally {
      admin.dropTable(namespace1, TABLE4, true);
    }
  }

  @Test
  public void
      alterColumnType_AlterColumnTypeFromEachExistingDataTypeToText_ShouldAlterColumnTypesCorrectly()
          throws ExecutionException, IOException, TransactionException {
    // Use a separate table name to avoid hitting the stale cache, which can cause test failure when
    // executing DMLs
    String table = "table_for_alter_1";

    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata.Builder currentTableMetadataBuilder =
          TableMetadata.newBuilder()
              .addColumn("c1", DataType.INT)
              .addColumn("c2", DataType.INT)
              .addColumn("c3", DataType.INT)
              .addColumn("c4", DataType.BIGINT)
              .addColumn("c5", DataType.FLOAT)
              .addColumn("c6", DataType.DOUBLE)
              .addColumn("c7", DataType.TEXT)
              .addColumn("c8", DataType.BLOB)
              .addColumn("c9", DataType.DATE)
              .addColumn("c10", DataType.TIME)
              .addPartitionKey("c1")
              .addClusteringKey("c2", Scan.Ordering.Order.ASC);
      if (isTimestampTypeSupported()) {
        currentTableMetadataBuilder
            .addColumn("c11", DataType.TIMESTAMP)
            .addColumn("c12", DataType.TIMESTAMPTZ);
      }
      TableMetadata currentTableMetadata = currentTableMetadataBuilder.build();
      admin.createTable(namespace1, table, currentTableMetadata, options);
      InsertBuilder.Buildable insert =
          Insert.newBuilder()
              .namespace(namespace1)
              .table(table)
              .partitionKey(Key.ofInt("c1", 1))
              .clusteringKey(Key.ofInt("c2", 2))
              .intValue("c3", 1)
              .bigIntValue("c4", 2L)
              .floatValue("c5", 3.0f)
              .doubleValue("c6", 4.0d)
              .textValue("c7", "5")
              .blobValue("c8", "6".getBytes(StandardCharsets.UTF_8))
              .dateValue("c9", LocalDate.now(ZoneId.of("UTC")))
              .timeValue("c10", LocalTime.now(ZoneId.of("UTC")));
      if (isTimestampTypeSupported()) {
        insert.timestampValue("c11", LocalDateTime.now(ZoneOffset.UTC));
        insert.timestampTZValue("c12", Instant.now());
      }
      transactionalInsert(insert.build());

      // Act
      admin.alterColumnType(namespace1, table, "c3", DataType.TEXT);
      admin.alterColumnType(namespace1, table, "c4", DataType.TEXT);
      admin.alterColumnType(namespace1, table, "c5", DataType.TEXT);
      admin.alterColumnType(namespace1, table, "c6", DataType.TEXT);
      admin.alterColumnType(namespace1, table, "c7", DataType.TEXT);
      admin.alterColumnType(namespace1, table, "c8", DataType.TEXT);
      admin.alterColumnType(namespace1, table, "c9", DataType.TEXT);
      admin.alterColumnType(namespace1, table, "c10", DataType.TEXT);
      if (isTimestampTypeSupported()) {
        admin.alterColumnType(namespace1, table, "c11", DataType.TEXT);
        admin.alterColumnType(namespace1, table, "c12", DataType.TEXT);
      }

      // Assert
      TableMetadata.Builder expectedTableMetadataBuilder =
          TableMetadata.newBuilder()
              .addColumn("c1", DataType.INT)
              .addColumn("c2", DataType.INT)
              .addColumn("c3", DataType.TEXT)
              .addColumn("c4", DataType.TEXT)
              .addColumn("c5", DataType.TEXT)
              .addColumn("c6", DataType.TEXT)
              .addColumn("c7", DataType.TEXT)
              .addColumn("c8", DataType.TEXT)
              .addColumn("c9", DataType.TEXT)
              .addColumn("c10", DataType.TEXT)
              .addPartitionKey("c1")
              .addClusteringKey("c2", Scan.Ordering.Order.ASC);
      if (isTimestampTypeSupported()) {
        expectedTableMetadataBuilder
            .addColumn("c11", DataType.TEXT)
            .addColumn("c12", DataType.TEXT);
      }
      TableMetadata expectedTableMetadata = expectedTableMetadataBuilder.build();
      assertThat(admin.getTableMetadata(namespace1, table)).isEqualTo(expectedTableMetadata);
    } finally {
      admin.dropTable(namespace1, table, true);
    }
  }

  @Test
  public void alterColumnType_WideningConversion_ShouldAlterColumnTypesCorrectly()
      throws ExecutionException, IOException, TransactionException {
    // Use a separate table name to avoid hitting the stale cache, which can cause test failure when
    // executing DMLs
    String table = "table_for_alter_2";

    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata.Builder currentTableMetadataBuilder =
          TableMetadata.newBuilder()
              .addColumn("c1", DataType.INT)
              .addColumn("c2", DataType.INT)
              .addColumn("c3", DataType.INT)
              .addColumn("c4", DataType.FLOAT)
              .addPartitionKey("c1")
              .addClusteringKey("c2", Scan.Ordering.Order.ASC);
      TableMetadata currentTableMetadata = currentTableMetadataBuilder.build();
      admin.createTable(namespace1, table, currentTableMetadata, options);
      int expectedColumn3Value = 1;
      float expectedColumn4Value = 4.0f;

      InsertBuilder.Buildable insert =
          Insert.newBuilder()
              .namespace(namespace1)
              .table(table)
              .partitionKey(Key.ofInt("c1", 1))
              .clusteringKey(Key.ofInt("c2", 2))
              .intValue("c3", expectedColumn3Value)
              .floatValue("c4", expectedColumn4Value);
      transactionalInsert(insert.build());

      // Act
      admin.alterColumnType(namespace1, table, "c3", DataType.BIGINT);
      admin.alterColumnType(namespace1, table, "c4", DataType.DOUBLE);

      // Wait for cache expiry
      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

      // Assert
      TableMetadata.Builder expectedTableMetadataBuilder =
          TableMetadata.newBuilder()
              .addColumn("c1", DataType.INT)
              .addColumn("c2", DataType.INT)
              .addColumn("c3", DataType.BIGINT)
              .addColumn("c4", DataType.DOUBLE)
              .addPartitionKey("c1")
              .addClusteringKey("c2", Scan.Ordering.Order.ASC);
      TableMetadata expectedTableMetadata = expectedTableMetadataBuilder.build();
      assertThat(admin.getTableMetadata(namespace1, table)).isEqualTo(expectedTableMetadata);
      Scan scan =
          Scan.newBuilder()
              .namespace(namespace1)
              .table(table)
              .partitionKey(Key.ofInt("c1", 1))
              .build();
      List<Result> results = transactionalScan(scan);
      assertThat(results).hasSize(1);
      Result result = results.get(0);
      assertThat(result.getBigInt("c3")).isEqualTo(expectedColumn3Value);
      assertThat(result.getDouble("c4")).isEqualTo(expectedColumn4Value);
    } finally {
      admin.dropTable(namespace1, table, true);
    }
  }

  @Test
  public void alterColumnType_ForPrimaryKeyOrIndexKeyColumn_ShouldThrowIllegalArgumentException()
      throws ExecutionException {
    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata currentTableMetadata =
          TableMetadata.newBuilder()
              .addColumn("c1", DataType.INT)
              .addColumn("c2", DataType.INT)
              .addColumn("c3", DataType.INT)
              .addPartitionKey("c1")
              .addClusteringKey("c2")
              .addSecondaryIndex("c3")
              .build();
      admin.createTable(namespace1, TABLE4, currentTableMetadata, options);

      // Act Assert
      assertThatThrownBy(() -> admin.alterColumnType(namespace1, TABLE4, "c1", DataType.TEXT))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> admin.alterColumnType(namespace1, TABLE4, "c2", DataType.TEXT))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> admin.alterColumnType(namespace1, TABLE4, "c3", DataType.TEXT))
          .isInstanceOf(IllegalArgumentException.class);
    } finally {
      admin.dropTable(namespace1, TABLE4, true);
    }
  }

  @Test
  public void renameTable_ForExistingTable_ShouldRenameTableCorrectly() throws ExecutionException {
    String newTableName = "new" + TABLE4;
    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata tableMetadata =
          TableMetadata.newBuilder()
              .addColumn("c1", DataType.INT)
              .addColumn("c2", DataType.INT)
              .addPartitionKey("c1")
              .build();
      admin.createTable(namespace1, TABLE4, tableMetadata, options);

      // Act
      admin.renameTable(namespace1, TABLE4, newTableName);

      // Assert
      assertThat(admin.tableExists(namespace1, TABLE4)).isFalse();
      assertThat(admin.tableExists(namespace1, newTableName)).isTrue();
      assertThat(admin.getTableMetadata(namespace1, newTableName)).isEqualTo(tableMetadata);
    } finally {
      admin.dropTable(namespace1, TABLE4, true);
      admin.dropTable(namespace1, newTableName, true);
    }
  }

  @Test
  public void renameTable_ForNonExistingTable_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> admin.renameTable(namespace1, TABLE4, "newTableName"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void renameTable_IfNewTableNameAlreadyExists_ShouldThrowIllegalArgumentException()
      throws ExecutionException {
    String newTableName = "new" + TABLE4;
    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata tableMetadata =
          TableMetadata.newBuilder()
              .addColumn("c1", DataType.INT)
              .addColumn("c2", DataType.INT)
              .addPartitionKey("c1")
              .build();
      admin.createTable(namespace1, TABLE4, tableMetadata, options);
      admin.createTable(namespace1, newTableName, tableMetadata, options);

      // Act Assert
      assertThatThrownBy(() -> admin.renameTable(namespace1, TABLE4, newTableName))
          .isInstanceOf(IllegalArgumentException.class);
    } finally {
      admin.dropTable(namespace1, TABLE4, true);
      admin.dropTable(namespace1, newTableName, true);
    }
  }

  @Test
  public void renameTable_ForExistingTableWithIndexes_ShouldRenameTableAndIndexesCorrectly()
      throws ExecutionException {
    String newTableName = "new" + TABLE4;
    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata tableMetadata =
          TableMetadata.newBuilder()
              .addColumn("c1", DataType.INT)
              .addColumn("c2", DataType.INT)
              .addColumn("c3", DataType.INT)
              .addPartitionKey("c1")
              .addSecondaryIndex("c2")
              .addSecondaryIndex("c3")
              .build();
      admin.createTable(namespace1, TABLE4, tableMetadata, options);

      // Act
      admin.renameTable(namespace1, TABLE4, newTableName);

      // Assert
      assertThat(admin.tableExists(namespace1, TABLE4)).isFalse();
      assertThat(admin.tableExists(namespace1, newTableName)).isTrue();
      assertThat(admin.getTableMetadata(namespace1, newTableName)).isEqualTo(tableMetadata);
      assertThat(admin.indexExists(namespace1, newTableName, "c2")).isTrue();
      assertThat(admin.indexExists(namespace1, newTableName, "c3")).isTrue();
      assertThatCode(() -> admin.dropIndex(namespace1, newTableName, "c2"))
          .doesNotThrowAnyException();
      assertThatCode(() -> admin.dropIndex(namespace1, newTableName, "c3"))
          .doesNotThrowAnyException();
    } finally {
      admin.dropTable(namespace1, TABLE4, true);
      admin.dropTable(namespace1, newTableName, true);
    }
  }

  @Test
  public void renameTable_IfOnlyOneTableExists_ShouldRenameTableCorrectly()
      throws ExecutionException {
    String newTableName = "new" + TABLE4;
    try {
      // Arrange
      admin.createNamespace(namespace3);
      Map<String, String> options = getCreationOptions();
      TableMetadata tableMetadata =
          TableMetadata.newBuilder()
              .addColumn("c1", DataType.INT)
              .addColumn("c2", DataType.INT)
              .addPartitionKey("c1")
              .build();
      admin.createTable(namespace3, TABLE4, tableMetadata, options);

      // Act
      admin.renameTable(namespace3, TABLE4, newTableName);

      // Assert
      assertThat(admin.tableExists(namespace3, TABLE4)).isFalse();
      assertThat(admin.tableExists(namespace3, newTableName)).isTrue();
      assertThat(admin.getTableMetadata(namespace3, newTableName)).isEqualTo(tableMetadata);
    } finally {
      admin.dropTable(namespace3, TABLE4, true);
      admin.dropTable(namespace3, newTableName, true);
      admin.dropNamespace(namespace3, true);
    }
  }

  @Test
  public void createCoordinatorTables_ShouldCreateCoordinatorTablesCorrectly()
      throws ExecutionException {
    // Arrange
    admin.dropCoordinatorTables();

    // Act
    admin.createCoordinatorTables(getCreationOptions());

    // Assert
    assertThat(admin.coordinatorTablesExist()).isTrue();
  }

  @Test
  public void
      createCoordinatorTables_CoordinatorTablesAlreadyExist_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> admin.createCoordinatorTables(getCreationOptions()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      createCoordinatorTables_IfNotExist_CoordinatorTablesAlreadyExist_ShouldNotThrowAnyException() {
    // Arrange

    // Act Assert
    assertThatCode(() -> admin.createCoordinatorTables(true, getCreationOptions()))
        .doesNotThrowAnyException();
  }

  @Test
  public void dropCoordinatorTables_ShouldDropCoordinatorTablesCorrectly()
      throws ExecutionException {
    try {
      // Arrange

      // Act
      admin.dropCoordinatorTables();

      // Assert
      assertThat(admin.coordinatorTablesExist()).isFalse();
    } finally {
      admin.createCoordinatorTables(true, getCreationOptions());
    }
  }

  @Test
  public void
      dropCoordinatorTables_CoordinatorTablesDoNotExist_ShouldThrowIllegalArgumentException()
          throws ExecutionException {
    try {
      // Arrange
      admin.dropCoordinatorTables();

      // Act Assert
      assertThatThrownBy(() -> admin.dropCoordinatorTables())
          .isInstanceOf(IllegalArgumentException.class);
    } finally {
      admin.createCoordinatorTables(true, getCreationOptions());
    }
  }

  @Test
  public void dropCoordinatorTables_IfExist_CoordinatorTablesDoNotExist_ShouldNotThrowAnyException()
      throws ExecutionException {
    try {
      // Arrange
      admin.dropCoordinatorTables();

      // Act Assert
      assertThatCode(() -> admin.dropCoordinatorTables(true)).doesNotThrowAnyException();
    } finally {
      admin.createCoordinatorTables(true, getCreationOptions());
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
  public void
      upgrade_WhenMetadataTableExistsButNotNamespacesTable_ShouldCreateNamespacesTableAndImportExistingNamespaces()
          throws Exception {
    AdminTestUtils adminTestUtils = getAdminTestUtils(getTestName());
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

  protected boolean isIndexOnBlobColumnSupported() {
    return true;
  }

  protected boolean isTimestampTypeSupported() {
    return true;
  }

  protected boolean isCreateIndexOnTextColumnEnabled() {
    return true;
  }

  protected abstract void transactionalInsert(Insert insert) throws TransactionException;

  protected abstract List<Result> transactionalScan(Scan scan) throws TransactionException;
}
