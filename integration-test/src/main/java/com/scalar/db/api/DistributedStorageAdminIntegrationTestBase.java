package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.util.AdminTestUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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

  private StorageFactory storageFactory;
  private DistributedStorageAdmin admin;
  private String systemNamespaceName;
  private String namespace1;
  private String namespace2;
  private String namespace3;

  @BeforeAll
  public void beforeAll() throws Exception {
    initialize(getTestName());
    Properties properties = getProperties(getTestName());
    storageFactory = StorageFactory.create(properties);
    admin = storageFactory.getAdmin();
    systemNamespaceName = getSystemNamespaceName(properties);
    namespace1 = getNamespace1();
    namespace2 = getNamespace2();
    namespace3 = getNamespace3();
    createTables();
  }

  protected void initialize(String testName) throws Exception {}

  protected abstract Properties getProperties(String testName);

  protected abstract String getSystemNamespaceName(Properties properties);

  protected String getTestName() {
    return TEST_NAME;
  }

  protected String getNamespace1() {
    return NAMESPACE1;
  }

  protected String getNamespace2() {
    return NAMESPACE2;
  }

  protected String getNamespace3() {
    return NAMESPACE3;
  }

  protected String getTable1() {
    return TABLE1;
  }

  protected String getTable2() {
    return TABLE2;
  }

  protected String getTable3() {
    return TABLE3;
  }

  protected String getTable4() {
    return TABLE4;
  }

  protected String getColumnName1() {
    return COL_NAME1;
  }

  protected String getColumnName2() {
    return COL_NAME2;
  }

  protected String getColumnName3() {
    return COL_NAME3;
  }

  protected String getColumnName4() {
    return COL_NAME4;
  }

  protected String getColumnName5() {
    return COL_NAME5;
  }

  protected String getColumnName6() {
    return COL_NAME6;
  }

  protected String getColumnName7() {
    return COL_NAME7;
  }

  protected String getColumnName8() {
    return COL_NAME8;
  }

  protected String getColumnName9() {
    return COL_NAME9;
  }

  protected String getColumnName10() {
    return COL_NAME10;
  }

  protected String getColumnName11() {
    return COL_NAME11;
  }

  protected TableMetadata getTableMetadata() {
    return TableMetadata.newBuilder()
        .addColumn(getColumnName1(), DataType.INT)
        .addColumn(getColumnName2(), DataType.TEXT)
        .addColumn(getColumnName3(), DataType.TEXT)
        .addColumn(getColumnName4(), DataType.INT)
        .addColumn(getColumnName5(), DataType.INT)
        .addColumn(getColumnName6(), DataType.TEXT)
        .addColumn(getColumnName7(), DataType.BIGINT)
        .addColumn(getColumnName8(), DataType.FLOAT)
        .addColumn(getColumnName9(), DataType.DOUBLE)
        .addColumn(getColumnName10(), DataType.BOOLEAN)
        .addColumn(getColumnName11(), DataType.BLOB)
        .addPartitionKey(getColumnName2())
        .addPartitionKey(getColumnName1())
        .addClusteringKey(getColumnName4(), Scan.Ordering.Order.ASC)
        .addClusteringKey(getColumnName3(), Scan.Ordering.Order.DESC)
        .addSecondaryIndex(getColumnName5())
        .addSecondaryIndex(getColumnName6())
        .build();
  }

  private void createTables() throws ExecutionException {
    Map<String, String> options = getCreationOptions();
    for (String namespace : Arrays.asList(namespace1, namespace2)) {
      admin.createNamespace(namespace, true, options);
      for (String table : Arrays.asList(getTable1(), getTable2(), getTable3())) {
        admin.createTable(namespace, table, getTableMetadata(), true, options);
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
      for (String table : Arrays.asList(getTable1(), getTable2(), getTable3())) {
        admin.dropTable(namespace, table);
      }
      admin.dropNamespace(namespace);
    }
  }

  @Test
  public void getTableMetadata_CorrectTableGiven_ShouldReturnCorrectMetadata()
      throws ExecutionException {
    // Arrange

    // Act
    TableMetadata tableMetadata = admin.getTableMetadata(namespace1, getTable1());

    // Assert
    assertThat(tableMetadata).isNotNull();

    assertThat(tableMetadata.getPartitionKeyNames().size()).isEqualTo(2);
    Iterator<String> iterator = tableMetadata.getPartitionKeyNames().iterator();
    assertThat(iterator.next()).isEqualTo(getColumnName2());
    assertThat(iterator.next()).isEqualTo(getColumnName1());

    assertThat(tableMetadata.getClusteringKeyNames().size()).isEqualTo(2);
    iterator = tableMetadata.getClusteringKeyNames().iterator();
    assertThat(iterator.next()).isEqualTo(getColumnName4());
    assertThat(iterator.next()).isEqualTo(getColumnName3());

    assertThat(tableMetadata.getColumnNames().size()).isEqualTo(11);
    assertThat(tableMetadata.getColumnNames().contains(getColumnName1())).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(getColumnName2())).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(getColumnName3())).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(getColumnName4())).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(getColumnName5())).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(getColumnName6())).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(getColumnName7())).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(getColumnName8())).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(getColumnName9())).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(getColumnName10())).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(getColumnName11())).isTrue();

    assertThat(tableMetadata.getColumnDataType(getColumnName1())).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType(getColumnName2())).isEqualTo(DataType.TEXT);
    assertThat(tableMetadata.getColumnDataType(getColumnName3())).isEqualTo(DataType.TEXT);
    assertThat(tableMetadata.getColumnDataType(getColumnName4())).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType(getColumnName5())).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType(getColumnName6())).isEqualTo(DataType.TEXT);
    assertThat(tableMetadata.getColumnDataType(getColumnName7())).isEqualTo(DataType.BIGINT);
    assertThat(tableMetadata.getColumnDataType(getColumnName8())).isEqualTo(DataType.FLOAT);
    assertThat(tableMetadata.getColumnDataType(getColumnName9())).isEqualTo(DataType.DOUBLE);
    assertThat(tableMetadata.getColumnDataType(getColumnName10())).isEqualTo(DataType.BOOLEAN);
    assertThat(tableMetadata.getColumnDataType(getColumnName11())).isEqualTo(DataType.BLOB);

    assertThat(tableMetadata.getClusteringOrder(getColumnName1())).isNull();
    assertThat(tableMetadata.getClusteringOrder(getColumnName2())).isNull();
    assertThat(tableMetadata.getClusteringOrder(getColumnName3()))
        .isEqualTo(Scan.Ordering.Order.DESC);
    assertThat(tableMetadata.getClusteringOrder(getColumnName4()))
        .isEqualTo(Scan.Ordering.Order.ASC);
    assertThat(tableMetadata.getClusteringOrder(getColumnName5())).isNull();
    assertThat(tableMetadata.getClusteringOrder(getColumnName6())).isNull();
    assertThat(tableMetadata.getClusteringOrder(getColumnName7())).isNull();
    assertThat(tableMetadata.getClusteringOrder(getColumnName8())).isNull();
    assertThat(tableMetadata.getClusteringOrder(getColumnName9())).isNull();
    assertThat(tableMetadata.getClusteringOrder(getColumnName10())).isNull();
    assertThat(tableMetadata.getClusteringOrder(getColumnName11())).isNull();

    assertThat(tableMetadata.getSecondaryIndexNames().size()).isEqualTo(2);
    assertThat(tableMetadata.getSecondaryIndexNames().contains(getColumnName5())).isTrue();
    assertThat(tableMetadata.getSecondaryIndexNames().contains(getColumnName6())).isTrue();
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
      admin.createTable(namespace3, getTable1(), getTableMetadata());

      // Act Assert
      assertThatThrownBy(() -> admin.dropNamespace(namespace3))
          .isInstanceOf(IllegalArgumentException.class);
    } finally {
      admin.dropTable(namespace3, getTable1(), true);
      admin.dropNamespace(namespace3, true);
    }
  }

  @Test
  public void
      dropNamespace_ForNamespaceWithNonScalarDBManagedTables_ShouldThrowIllegalArgumentException()
          throws Exception {
    AdminTestUtils adminTestUtils = getAdminTestUtils(getTestName());
    String nonManagedTable = "non_managed_table";
    try {
      // Arrange
      admin.createNamespace(namespace3, getCreationOptions());
      admin.createTable(namespace3, nonManagedTable, getTableMetadata(), getCreationOptions());
      adminTestUtils.deleteMetadata(namespace3, nonManagedTable);

      // Act Assert
      assertThatThrownBy(() -> admin.dropNamespace(namespace3))
          .isInstanceOf(IllegalArgumentException.class);
    } finally {
      adminTestUtils.dropTable(namespace3, nonManagedTable);
      admin.dropNamespace(namespace3, true);

      adminTestUtils.close();
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
      admin.createTable(namespace1, getTable4(), getTableMetadata(), options);

      // Assert
      assertThat(admin.tableExists(namespace1, getTable4())).isTrue();
    } finally {
      admin.dropTable(namespace1, getTable4(), true);
    }
  }

  @Test
  public void createTable_ForExistingTable_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> admin.createTable(namespace1, getTable1(), getTableMetadata()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void createTable_ForNonExistingNamespace_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> admin.createTable(namespace3, getTable1(), getTableMetadata()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void createTable_IfNotExists_ForExistingNamespace_ShouldNotThrowAnyException() {
    // Arrange

    // Act Assert
    assertThatCode(() -> admin.createTable(namespace1, getTable1(), getTableMetadata(), true))
        .doesNotThrowAnyException();
  }

  @Test
  public void dropTable_ForExistingTable_ShouldDropTableProperly() throws ExecutionException {
    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      admin.createTable(namespace1, getTable4(), getTableMetadata(), options);

      // Act
      admin.dropTable(namespace1, getTable4());

      // Assert
      assertThat(admin.tableExists(namespace1, getTable4())).isFalse();
    } finally {
      admin.dropTable(namespace1, getTable4(), true);
    }
  }

  @Test
  public void dropTable_ForNonExistingTable_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> admin.dropTable(namespace1, getTable4()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void dropTable_IfExists_ForNonExistingTable_ShouldNotThrowAnyException() {
    // Arrange

    // Act Assert
    assertThatCode(() -> admin.dropTable(namespace1, getTable4(), true)).doesNotThrowAnyException();
  }

  @Test
  public void truncateTable_ShouldTruncateProperly() throws ExecutionException, IOException {
    DistributedStorage storage = null;
    try {
      // Arrange
      Key partitionKey = new Key(getColumnName2(), "aaa", getColumnName1(), 1);
      Key clusteringKey = new Key(getColumnName4(), 2, getColumnName3(), "bbb");
      storage = storageFactory.getStorage();
      storage.put(
          new Put(partitionKey, clusteringKey)
              .withValue(getColumnName5(), 3)
              .withValue(getColumnName6(), "ccc")
              .withValue(getColumnName7(), 4L)
              .withValue(getColumnName8(), 1.0f)
              .withValue(getColumnName9(), 1.0d)
              .withValue(getColumnName10(), true)
              .withValue(getColumnName11(), "ddd".getBytes(StandardCharsets.UTF_8))
              .forNamespace(namespace1)
              .forTable(getTable1()));

      // Act
      admin.truncateTable(namespace1, getTable1());

      // Assert
      Scanner scanner =
          storage.scan(new Scan(partitionKey).forNamespace(namespace1).forTable(getTable1()));
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
    assertThatThrownBy(() -> admin.truncateTable(namespace1, getTable4()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void getNamespaceTableNames_ShouldReturnCorrectTables() throws ExecutionException {
    // Arrange

    // Act
    Set<String> actual = admin.getNamespaceTableNames(namespace1);

    // Assert
    assertThat(actual)
        .isEqualTo(new HashSet<>(Arrays.asList(getTable1(), getTable2(), getTable3())));
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
    assertThat(admin.tableExists(namespace1, getTable1())).isTrue();
    assertThat(admin.tableExists(namespace1, getTable2())).isTrue();
    assertThat(admin.tableExists(namespace1, getTable3())).isTrue();
    assertThat(admin.tableExists(namespace1, getTable4())).isFalse();
  }

  @Test
  public void createIndex_ForAllDataTypesWithExistingData_ShouldCreateIndexesCorrectly()
      throws ExecutionException {
    DistributedStorage storage = null;
    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata metadata =
          TableMetadata.newBuilder()
              .addColumn(getColumnName1(), DataType.INT)
              .addColumn(getColumnName2(), DataType.INT)
              .addColumn(getColumnName3(), DataType.TEXT)
              .addColumn(getColumnName4(), DataType.BIGINT)
              .addColumn(getColumnName5(), DataType.FLOAT)
              .addColumn(getColumnName6(), DataType.DOUBLE)
              .addColumn(getColumnName7(), DataType.BOOLEAN)
              .addColumn(getColumnName8(), DataType.BLOB)
              .addColumn(getColumnName9(), DataType.TEXT)
              .addPartitionKey(getColumnName1())
              .addSecondaryIndex(getColumnName9())
              .build();
      admin.createTable(namespace1, getTable4(), metadata, options);
      storage = storageFactory.getStorage();
      storage.put(
          Put.newBuilder()
              .namespace(namespace1)
              .table(getTable4())
              .partitionKey(Key.ofInt(getColumnName1(), 1))
              .intValue(getColumnName2(), 2)
              .textValue(getColumnName3(), "3")
              .bigIntValue(getColumnName4(), 4)
              .floatValue(getColumnName5(), 5)
              .doubleValue(getColumnName6(), 6)
              .booleanValue(getColumnName7(), true)
              .blobValue(getColumnName8(), "8".getBytes(StandardCharsets.UTF_8))
              .textValue(getColumnName9(), "9")
              .build());
      // Act
      admin.createIndex(namespace1, getTable4(), getColumnName2(), options);
      admin.createIndex(namespace1, getTable4(), getColumnName3(), options);
      admin.createIndex(namespace1, getTable4(), getColumnName4(), options);
      admin.createIndex(namespace1, getTable4(), getColumnName5(), options);
      admin.createIndex(namespace1, getTable4(), getColumnName6(), options);
      if (isIndexOnBooleanColumnSupported()) {
        admin.createIndex(namespace1, getTable4(), getColumnName7(), options);
      }
      admin.createIndex(namespace1, getTable4(), getColumnName8(), options);

      // Assert
      assertThat(admin.indexExists(namespace1, getTable4(), getColumnName2())).isTrue();
      assertThat(admin.indexExists(namespace1, getTable4(), getColumnName3())).isTrue();
      assertThat(admin.indexExists(namespace1, getTable4(), getColumnName4())).isTrue();
      assertThat(admin.indexExists(namespace1, getTable4(), getColumnName5())).isTrue();
      assertThat(admin.indexExists(namespace1, getTable4(), getColumnName6())).isTrue();
      if (isIndexOnBooleanColumnSupported()) {
        assertThat(admin.indexExists(namespace1, getTable4(), getColumnName7())).isTrue();
      }
      assertThat(admin.indexExists(namespace1, getTable4(), getColumnName8())).isTrue();
      if (isIndexOnBooleanColumnSupported()) {
        assertThat(admin.getTableMetadata(namespace1, getTable4()).getSecondaryIndexNames())
            .containsOnly(
                getColumnName2(),
                getColumnName3(),
                getColumnName4(),
                getColumnName5(),
                getColumnName6(),
                getColumnName7(),
                getColumnName8(),
                getColumnName9());
      } else {
        assertThat(admin.getTableMetadata(namespace1, getTable4()).getSecondaryIndexNames())
            .containsOnly(
                getColumnName2(),
                getColumnName3(),
                getColumnName4(),
                getColumnName5(),
                getColumnName6(),
                getColumnName8(),
                getColumnName9());
      }
    } finally {
      admin.dropTable(namespace1, getTable4(), true);
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
                    namespace1, "non-existing_table", getColumnName2(), getCreationOptions()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void createIndex_ForNonExistingColumn_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(
            () ->
                admin.createIndex(
                    namespace1, getTable1(), "non-existing_column", getCreationOptions()))
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
              .addColumn(getColumnName1(), DataType.INT)
              .addColumn(getColumnName2(), DataType.INT)
              .addPartitionKey(getColumnName1())
              .addSecondaryIndex(getColumnName2())
              .build();
      admin.createTable(namespace1, getTable4(), metadata, options);

      // Act Assert
      assertThatThrownBy(
              () ->
                  admin.createIndex(
                      namespace1, getTable4(), getColumnName2(), getCreationOptions()))
          .isInstanceOf(IllegalArgumentException.class);
    } finally {
      admin.dropTable(namespace1, getTable4(), true);
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
              .addColumn(getColumnName1(), DataType.INT)
              .addColumn(getColumnName2(), DataType.INT)
              .addPartitionKey(getColumnName1())
              .addSecondaryIndex(getColumnName2())
              .build();
      admin.createTable(namespace1, getTable4(), metadata, options);

      // Act Assert
      assertThatCode(
              () ->
                  admin.createIndex(
                      namespace1, getTable4(), getColumnName2(), true, getCreationOptions()))
          .doesNotThrowAnyException();
    } finally {
      admin.dropTable(namespace1, getTable4(), true);
    }
  }

  @Test
  public void dropIndex_ForAllDataTypesWithExistingData_ShouldDropIndexCorrectly()
      throws ExecutionException {
    DistributedStorage storage = null;
    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata metadata =
          TableMetadata.newBuilder()
              .addColumn(getColumnName1(), DataType.INT)
              .addColumn(getColumnName2(), DataType.INT)
              .addColumn(getColumnName3(), DataType.TEXT)
              .addColumn(getColumnName4(), DataType.BIGINT)
              .addColumn(getColumnName5(), DataType.FLOAT)
              .addColumn(getColumnName6(), DataType.DOUBLE)
              .addColumn(getColumnName7(), DataType.BOOLEAN)
              .addColumn(getColumnName8(), DataType.BLOB)
              .addColumn(getColumnName9(), DataType.TEXT)
              .addPartitionKey(getColumnName1())
              .addSecondaryIndex(getColumnName2())
              .addSecondaryIndex(getColumnName3())
              .addSecondaryIndex(getColumnName4())
              .addSecondaryIndex(getColumnName5())
              .addSecondaryIndex(getColumnName6())
              .addSecondaryIndex(getColumnName8())
              .addSecondaryIndex(getColumnName9())
              .addSecondaryIndex(getColumnName9())
              .build();
      if (isIndexOnBooleanColumnSupported()) {
        metadata = TableMetadata.newBuilder(metadata).addSecondaryIndex(getColumnName7()).build();
      }
      admin.createTable(namespace1, getTable4(), metadata, options);
      storage = storageFactory.getStorage();
      storage.put(
          Put.newBuilder()
              .namespace(namespace1)
              .table(getTable4())
              .partitionKey(Key.ofInt(getColumnName1(), 1))
              .intValue(getColumnName2(), 2)
              .textValue(getColumnName3(), "3")
              .bigIntValue(getColumnName4(), 4)
              .floatValue(getColumnName5(), 5)
              .doubleValue(getColumnName6(), 6)
              .booleanValue(getColumnName7(), true)
              .blobValue(getColumnName8(), "8".getBytes(StandardCharsets.UTF_8))
              .textValue(getColumnName9(), "9")
              .build());

      // Act
      admin.dropIndex(namespace1, getTable4(), getColumnName2());
      admin.dropIndex(namespace1, getTable4(), getColumnName3());
      admin.dropIndex(namespace1, getTable4(), getColumnName4());
      admin.dropIndex(namespace1, getTable4(), getColumnName5());
      admin.dropIndex(namespace1, getTable4(), getColumnName6());
      if (isIndexOnBooleanColumnSupported()) {
        admin.dropIndex(namespace1, getTable4(), getColumnName7());
      }
      admin.dropIndex(namespace1, getTable4(), getColumnName8());

      // Assert
      assertThat(admin.indexExists(namespace1, getTable4(), getColumnName2())).isFalse();
      assertThat(admin.indexExists(namespace1, getTable4(), getColumnName3())).isFalse();
      assertThat(admin.indexExists(namespace1, getTable4(), getColumnName4())).isFalse();
      assertThat(admin.indexExists(namespace1, getTable4(), getColumnName5())).isFalse();
      assertThat(admin.indexExists(namespace1, getTable4(), getColumnName6())).isFalse();
      assertThat(admin.indexExists(namespace1, getTable4(), getColumnName7())).isFalse();
      assertThat(admin.indexExists(namespace1, getTable4(), getColumnName8())).isFalse();
      assertThat(admin.getTableMetadata(namespace1, getTable4()).getSecondaryIndexNames())
          .containsOnly(getColumnName9());
    } finally {
      admin.dropTable(namespace1, getTable4(), true);
      if (storage != null) {
        storage.close();
      }
    }
  }

  @Test
  public void dropIndex_ForNonExistingTable_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> admin.dropIndex(namespace1, "non-existing-table", getColumnName2()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void dropIndex_ForNonExistingIndex_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> admin.dropIndex(namespace1, getTable1(), getColumnName2()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void dropIndex_IfExists_ForNonExistingIndex_ShouldNotThrowAnyException() {
    // Arrange

    // Act Assert
    assertThatCode(() -> admin.dropIndex(namespace1, getTable1(), getColumnName2(), true))
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
      admin.createTable(namespace1, getTable4(), currentTableMetadata, options);

      // Act
      admin.addNewColumnToTable(namespace1, getTable4(), "c2", DataType.TEXT);
      admin.addNewColumnToTable(namespace1, getTable4(), "c3", DataType.DOUBLE);
      admin.addNewColumnToTable(namespace1, getTable4(), "c4", DataType.INT);
      admin.addNewColumnToTable(namespace1, getTable4(), "c5", DataType.BIGINT);
      admin.addNewColumnToTable(namespace1, getTable4(), "c6", DataType.BLOB);
      admin.addNewColumnToTable(namespace1, getTable4(), "c7", DataType.BOOLEAN);
      admin.addNewColumnToTable(namespace1, getTable4(), "c8", DataType.FLOAT);

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
      assertThat(admin.getTableMetadata(namespace1, getTable4())).isEqualTo(expectedTableMetadata);
    } finally {
      admin.dropTable(namespace1, getTable4(), true);
    }
  }

  @Test
  public void addNewColumnToTable_ForNonExistingTable_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(
            () ->
                admin.addNewColumnToTable(namespace1, getTable4(), getColumnName2(), DataType.TEXT))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void addNewColumnToTable_ForAlreadyExistingColumn_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(
            () ->
                admin.addNewColumnToTable(namespace1, getTable1(), getColumnName2(), DataType.TEXT))
        .isInstanceOf(IllegalArgumentException.class);
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
