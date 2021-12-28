package com.scalar.db.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

@SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
public abstract class AdminIntegrationTestBase {

  private static final String TEST_NAME = "admin";
  private static final String NAMESPACE1 = "integration_testing_" + TEST_NAME + "1";
  private static final String NAMESPACE2 = "integration_testing_" + TEST_NAME + "2";
  private static final String NAMESPACE3 = "integration_testing_" + TEST_NAME + "3";
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
          .addPartitionKey(COL_NAME2)
          .addPartitionKey(COL_NAME1)
          .addClusteringKey(COL_NAME4, Scan.Ordering.Order.ASC)
          .addClusteringKey(COL_NAME3, Scan.Ordering.Order.DESC)
          .addSecondaryIndex(COL_NAME5)
          .addSecondaryIndex(COL_NAME6)
          .build();

  private static boolean initialized;
  private static DistributedStorageAdmin admin;
  private static DistributedStorage storage;
  private static String namespace1;
  private static String namespace2;
  private static String namespace3;

  @Before
  public void setUp() throws Exception {
    if (!initialized) {
      initialize();
      StorageFactory factory =
          new StorageFactory(TestUtils.addSuffix(getDatabaseConfig(), TEST_NAME));
      admin = factory.getAdmin();
      namespace1 = getNamespace1();
      namespace2 = getNamespace2();
      namespace3 = getNamespace3();
      createTables();
      storage = factory.getStorage();
      initialized = true;
    }
  }

  protected void initialize() throws Exception {}

  protected abstract DatabaseConfig getDatabaseConfig();

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
    Map<String, String> options = getCreateOptions();
    for (String namespace : Arrays.asList(namespace1, namespace2)) {
      admin.createNamespace(namespace, true, options);
      for (String table : Arrays.asList(TABLE1, TABLE2, TABLE3)) {
        admin.createTable(namespace, table, TABLE_METADATA, true, options);
      }
    }
  }

  protected Map<String, String> getCreateOptions() {
    return Collections.emptyMap();
  }

  @AfterClass
  public static void tearDownAfterClass() throws ExecutionException {
    dropTables();
    admin.close();
  }

  private static void dropTables() throws ExecutionException {
    for (String namespace : Arrays.asList(namespace1, namespace2)) {
      for (String table : Arrays.asList(TABLE1, TABLE2, TABLE3)) {
        admin.dropTable(namespace, table);
      }
      admin.dropNamespace(namespace, true);
    }
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
  public void createNamespace_ForExistingNamespace_ShouldExecutionException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> admin.createNamespace(namespace1))
        .isInstanceOf(ExecutionException.class);
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
  public void dropNamespace_ForNonExistingNamespace_ShouldExecutionException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> admin.dropNamespace(namespace3))
        .isInstanceOf(ExecutionException.class);
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
      Map<String, String> options = getCreateOptions();

      // Act
      admin.createTable(namespace1, TABLE4, TABLE_METADATA, options);

      // Assert
      assertThat(admin.tableExists(namespace1, TABLE4)).isTrue();
    } finally {
      admin.dropTable(namespace1, TABLE4, true);
    }
  }

  @Test
  public void createTable_ForExistingTable_ShouldExecutionException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> admin.createTable(namespace1, TABLE1, TABLE_METADATA))
        .isInstanceOf(ExecutionException.class);
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
      Map<String, String> options = getCreateOptions();
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
  public void dropTable_ForNonExistingTable_ShouldExecutionException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> admin.dropTable(namespace1, TABLE4))
        .isInstanceOf(ExecutionException.class);
  }

  @Test
  public void dropTable_IfExists_ForNonExistingTable_ShouldNotThrowAnyException() {
    // Arrange

    // Act Assert
    assertThatCode(() -> admin.dropTable(namespace1, TABLE4, true)).doesNotThrowAnyException();
  }

  @Test
  public void truncateTable_ShouldTruncateProperly() throws ExecutionException, IOException {
    // Arrange
    Key partitionKey = new Key(COL_NAME2, "aaa", COL_NAME1, 1);
    Key clusteringKey = new Key(COL_NAME4, 2, COL_NAME3, "bbb");
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

    // Act  Assert
    assertThat(admin.tableExists(namespace1, TABLE1)).isTrue();
    assertThat(admin.tableExists(namespace1, TABLE2)).isTrue();
    assertThat(admin.tableExists(namespace1, TABLE3)).isTrue();
    assertThat(admin.tableExists(namespace1, TABLE4)).isFalse();
  }
}
