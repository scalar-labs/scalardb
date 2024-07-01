package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
          .addPartitionKey(COL_NAME2)
          .addPartitionKey(COL_NAME1)
          .addClusteringKey(COL_NAME4, Scan.Ordering.Order.ASC)
          .addClusteringKey(COL_NAME3, Scan.Ordering.Order.DESC)
          .addSecondaryIndex(COL_NAME5)
          .addSecondaryIndex(COL_NAME6)
          .build();
  protected TransactionFactory transactionFactory;
  protected DistributedTransactionAdmin admin;
  protected String systemNamespaceName;
  protected String namespace1;
  protected String namespace2;
  protected String namespace3;

  @BeforeAll
  public void beforeAll() throws Exception {
    String testName = getTestName();
    initialize(testName);
    Properties properties = getProperties(testName);
    transactionFactory = TransactionFactory.create(properties);
    admin = transactionFactory.getTransactionAdmin();
    systemNamespaceName = getSystemNamespaceName(properties);
    namespace1 = getNamespaceBaseName() + testName + "1";
    namespace2 = getNamespaceBaseName() + testName + "2";
    namespace3 = getNamespaceBaseName() + testName + "3";
    createTables();
  }

  protected abstract String getTestName();

  protected void initialize(String testName) throws Exception {}

  protected abstract Properties getProperties(String testName);

  protected String getNamespaceBaseName() {
    return NAMESPACE_BASE_NAME;
  }

  protected abstract String getSystemNamespaceName(Properties properties);

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

    assertThat(tableMetadata.getColumnNames().size()).isEqualTo(11);
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
      admin.createNamespace(namespace3);

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
    assertThatThrownBy(() -> admin.createNamespace(namespace1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void createNamespace_IfNotExists_ForExistingNamespace_ShouldNotThrowAnyException() {
    // Arrange

    // Act Assert
    assertThatCode(() -> admin.createNamespace(namespace1, true)).doesNotThrowAnyException();
  }

  @Test
  public void dropNamespace_ForNonExistingNamespace_ShouldDropNamespaceProperly()
      throws ExecutionException {
    try {
      // Arrange
      admin.createNamespace(namespace3);

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
      admin.createNamespace(namespace3);
      admin.createTable(namespace3, TABLE1, TABLE_METADATA);

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
    assertThatThrownBy(() -> admin.createTable(namespace1, TABLE1, TABLE_METADATA))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void createTable_ForNonExistingNamespace_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> admin.createTable(namespace3, TABLE1, TABLE_METADATA))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void createTable_IfNotExists_ForExistingNamespace_ShouldNotThrowAnyException() {
    // Arrange

    // Act Assert
    assertThatCode(() -> admin.createTable(namespace1, TABLE1, TABLE_METADATA, true))
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
    DistributedTransactionManager manager = null;
    try {
      // Arrange
      Key partitionKey = new Key(COL_NAME2, "aaa", COL_NAME1, 1);
      Key clusteringKey = new Key(COL_NAME4, 2, COL_NAME3, "bbb");
      manager = transactionFactory.getTransactionManager();
      manager.put(
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
      List<Result> results =
          manager.scan(new Scan(partitionKey).forNamespace(namespace1).forTable(TABLE1));
      assertThat(results).isEmpty();
    } finally {
      if (manager != null) {
        manager.close();
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
      throws Exception {
    DistributedTransactionManager transactionManager = null;
    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata metadata =
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
              .addPartitionKey(COL_NAME1)
              .addSecondaryIndex(COL_NAME9)
              .build();
      admin.createTable(namespace1, TABLE4, metadata, options);
      transactionManager = transactionFactory.getTransactionManager();
      transactionManager.put(
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
              .build());

      // Act
      admin.createIndex(namespace1, TABLE4, COL_NAME2, options);
      admin.createIndex(namespace1, TABLE4, COL_NAME3, options);
      admin.createIndex(namespace1, TABLE4, COL_NAME4, options);
      admin.createIndex(namespace1, TABLE4, COL_NAME5, options);
      admin.createIndex(namespace1, TABLE4, COL_NAME6, options);
      if (isIndexOnBooleanColumnSupported()) {
        admin.createIndex(namespace1, TABLE4, COL_NAME7, options);
      }
      admin.createIndex(namespace1, TABLE4, COL_NAME8, options);

      // Assert
      assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME2)).isTrue();
      assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME3)).isTrue();
      assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME4)).isTrue();
      assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME5)).isTrue();
      assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME6)).isTrue();
      if (isIndexOnBooleanColumnSupported()) {
        assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME7)).isTrue();
      }
      assertThat(admin.indexExists(namespace1, TABLE4, COL_NAME8)).isTrue();
      if (isIndexOnBooleanColumnSupported()) {
        assertThat(admin.getTableMetadata(namespace1, TABLE4).getSecondaryIndexNames())
            .containsOnly(
                COL_NAME2, COL_NAME3, COL_NAME4, COL_NAME5, COL_NAME6, COL_NAME7, COL_NAME8,
                COL_NAME9);
      } else {
        assertThat(admin.getTableMetadata(namespace1, TABLE4).getSecondaryIndexNames())
            .containsOnly(
                COL_NAME2, COL_NAME3, COL_NAME4, COL_NAME5, COL_NAME6, COL_NAME8, COL_NAME9);
      }

    } finally {
      admin.dropTable(namespace1, TABLE4, true);
      if (transactionManager != null) {
        transactionManager.close();
      }
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
    DistributedTransactionManager transactionManager = null;
    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata metadata =
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
              .addPartitionKey(COL_NAME1)
              .addSecondaryIndex(COL_NAME2)
              .addSecondaryIndex(COL_NAME3)
              .addSecondaryIndex(COL_NAME4)
              .addSecondaryIndex(COL_NAME5)
              .addSecondaryIndex(COL_NAME6)
              .addSecondaryIndex(COL_NAME8)
              .addSecondaryIndex(COL_NAME9)
              .addSecondaryIndex(COL_NAME9)
              .build();
      if (isIndexOnBooleanColumnSupported()) {
        metadata = TableMetadata.newBuilder(metadata).addSecondaryIndex(COL_NAME7).build();
      }
      admin.createTable(namespace1, TABLE4, metadata, options);
      transactionManager = transactionFactory.getTransactionManager();
      transactionManager.put(
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
              .build());

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
    } finally {
      admin.dropTable(namespace1, TABLE4, true);
      if (transactionManager != null) {
        transactionManager.close();
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

  protected boolean isIndexOnBooleanColumnSupported() {
    return true;
  }
}
