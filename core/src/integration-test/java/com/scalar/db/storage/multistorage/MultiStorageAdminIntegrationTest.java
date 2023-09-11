package com.scalar.db.storage.multistorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.storage.cassandra.CassandraAdmin;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MultiStorageAdminIntegrationTest {

  private static final String TEST_NAME = "mstorage_admin";
  private static final String NAMESPACE1 = "int_test_" + TEST_NAME + "1";
  private static final String NAMESPACE2 = "int_test_" + TEST_NAME + "2";
  private static final String NAMESPACE3 = "int_test_" + TEST_NAME + "3";
  private static final String TABLE1 = "test_table1";
  private static final String TABLE2 = "test_table2";
  private static final String TABLE3 = "test_table3";
  private static final String COL_NAME1 = "c1";
  private static final String COL_NAME2 = "c2";
  private static final String COL_NAME3 = "c3";
  private static final String COL_NAME4 = "c4";
  private static final String COL_NAME5 = "c5";

  private DistributedStorageAdmin cassandraAdmin;
  private DistributedStorageAdmin jdbcAdmin;
  private MultiStorageAdmin multiStorageAdmin;

  @BeforeAll
  public void beforeAll() throws ExecutionException {
    initCassandraAdmin();
    initJdbcAdmin();
    initMultiStorageAdmin();
  }

  private Map<String, String> getCreationOptions() {
    return Collections.singletonMap(CassandraAdmin.REPLICATION_FACTOR, "1");
  }

  private void initCassandraAdmin() throws ExecutionException {
    StorageFactory factory =
        StorageFactory.create(MultiStorageEnv.getPropertiesForCassandra(TEST_NAME));
    cassandraAdmin = factory.getAdmin();

    // create tables
    cassandraAdmin.createNamespace(NAMESPACE1, true, getCreationOptions());
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey(COL_NAME1)
            .addClusteringKey(COL_NAME4)
            .addColumn(COL_NAME1, DataType.INT)
            .addColumn(COL_NAME2, DataType.TEXT)
            .addColumn(COL_NAME3, DataType.INT)
            .addColumn(COL_NAME4, DataType.INT)
            .addColumn(COL_NAME5, DataType.BOOLEAN)
            .build();
    for (String table : Arrays.asList(TABLE1, TABLE2, TABLE3)) {
      cassandraAdmin.createTable(NAMESPACE1, table, tableMetadata, true, getCreationOptions());
    }

    cassandraAdmin.createNamespace(NAMESPACE2, true, getCreationOptions());
    cassandraAdmin.createTable(NAMESPACE2, TABLE1, tableMetadata, true, getCreationOptions());
  }

  private void initJdbcAdmin() throws ExecutionException {
    StorageFactory factory = StorageFactory.create(MultiStorageEnv.getPropertiesForJdbc(TEST_NAME));
    jdbcAdmin = factory.getAdmin();

    // create tables
    jdbcAdmin.createNamespace(NAMESPACE1, true);
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(COL_NAME1, DataType.TEXT)
            .addColumn(COL_NAME2, DataType.INT)
            .addColumn(COL_NAME3, DataType.BOOLEAN)
            .addPartitionKey(COL_NAME1)
            .build();
    for (String table : Arrays.asList(TABLE1, TABLE2, TABLE3)) {
      jdbcAdmin.createTable(NAMESPACE1, table, tableMetadata, true);
    }

    jdbcAdmin.createNamespace(NAMESPACE2, true);
    jdbcAdmin.createTable(NAMESPACE2, TABLE1, tableMetadata, true);
  }

  private void initMultiStorageAdmin() {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "multi-storage");

    // Define storages, cassandra and jdbc
    props.setProperty(MultiStorageConfig.STORAGES, "cassandra,jdbc");

    Properties propertiesForCassandra = MultiStorageEnv.getPropertiesForCassandra(TEST_NAME);
    for (String propertyName : propertiesForCassandra.stringPropertyNames()) {
      props.setProperty(
          MultiStorageConfig.STORAGES
              + ".cassandra."
              + propertyName.substring(DatabaseConfig.PREFIX.length()),
          propertiesForCassandra.getProperty(propertyName));
    }

    Properties propertiesForJdbc = MultiStorageEnv.getPropertiesForJdbc(TEST_NAME);
    for (String propertyName : propertiesForJdbc.stringPropertyNames()) {
      props.setProperty(
          MultiStorageConfig.STORAGES
              + ".jdbc."
              + propertyName.substring(DatabaseConfig.PREFIX.length()),
          propertiesForJdbc.getProperty(propertyName));
    }

    // Define table mapping from table1 in namespace1 to cassandra, and from table2 in namespace1 to
    // jdbc
    props.setProperty(
        MultiStorageConfig.TABLE_MAPPING,
        NAMESPACE1 + "." + TABLE1 + ":cassandra," + NAMESPACE1 + "." + TABLE2 + ":jdbc");

    // Define namespace mapping from namespace2 and namespace3 to jdbc
    props.setProperty(
        MultiStorageConfig.NAMESPACE_MAPPING, NAMESPACE2 + ":jdbc," + NAMESPACE3 + ":jdbc");

    // The default storage is cassandra
    props.setProperty(MultiStorageConfig.DEFAULT_STORAGE, "cassandra");

    multiStorageAdmin = new MultiStorageAdmin(new DatabaseConfig(props));
  }

  @AfterAll
  public void afterAll() throws ExecutionException {
    multiStorageAdmin.close();
    cleanUp(cassandraAdmin);
    cleanUp(jdbcAdmin);
  }

  private void cleanUp(DistributedStorageAdmin admin) throws ExecutionException {
    for (String table : Arrays.asList(TABLE1, TABLE2, TABLE3)) {
      admin.dropTable(NAMESPACE1, table);
    }
    admin.dropNamespace(NAMESPACE1);
    admin.dropTable(NAMESPACE2, TABLE1);
    admin.dropNamespace(NAMESPACE2);
    admin.close();
  }

  @Test
  public void ddlOperationsTest() throws ExecutionException {
    // createNamespace
    multiStorageAdmin.createNamespace(NAMESPACE3, true, getCreationOptions());
    assertThat(cassandraAdmin.namespaceExists(NAMESPACE3)).isFalse();
    assertThat(jdbcAdmin.namespaceExists(NAMESPACE3)).isTrue();

    // createTable
    multiStorageAdmin.createTable(
        NAMESPACE3,
        TABLE1,
        TableMetadata.newBuilder()
            .addColumn(COL_NAME1, DataType.TEXT)
            .addColumn(COL_NAME2, DataType.INT)
            .addColumn(COL_NAME3, DataType.BOOLEAN)
            .addPartitionKey(COL_NAME1)
            .build(),
        true,
        getCreationOptions());

    assertThat(jdbcAdmin.getNamespaceTableNames(NAMESPACE3).contains(TABLE1)).isTrue();

    // truncateTable
    assertThatCode(() -> multiStorageAdmin.truncateTable(NAMESPACE3, TABLE1))
        .doesNotThrowAnyException();

    // dropTable
    multiStorageAdmin.dropTable(NAMESPACE3, TABLE1);

    assertThat(jdbcAdmin.getNamespaceTableNames(NAMESPACE3).contains(TABLE1)).isFalse();

    // dropNamespace
    multiStorageAdmin.dropNamespace(NAMESPACE3);
    assertThat(jdbcAdmin.namespaceExists(NAMESPACE3)).isFalse();
  }

  @Test
  public void getTableMetadata_ForTable1InNamespace1_ShouldReturnMetadataFromCassandraAdmin()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE1;

    // Act
    TableMetadata tableMetadata = multiStorageAdmin.getTableMetadata(namespace, table);

    // Assert
    assertThat(tableMetadata).isNotNull();
    assertThat(tableMetadata.getPartitionKeyNames().size()).isEqualTo(1);
    assertThat(tableMetadata.getPartitionKeyNames().iterator().next()).isEqualTo(COL_NAME1);

    assertThat(tableMetadata.getClusteringKeyNames().size()).isEqualTo(1);
    assertThat(tableMetadata.getClusteringKeyNames().iterator().next()).isEqualTo(COL_NAME4);

    assertThat(tableMetadata.getColumnNames().size()).isEqualTo(5);
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME1)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME2)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME3)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME4)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME5)).isTrue();

    assertThat(tableMetadata.getColumnDataType(COL_NAME1)).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME2)).isEqualTo(DataType.TEXT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME3)).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME4)).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME5)).isEqualTo(DataType.BOOLEAN);

    assertThat(tableMetadata.getClusteringOrder(COL_NAME1)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME2)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME3)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME4)).isEqualTo(Scan.Ordering.Order.ASC);
    assertThat(tableMetadata.getClusteringOrder(COL_NAME5)).isNull();

    assertThat(tableMetadata.getSecondaryIndexNames()).isEmpty();
  }

  @Test
  public void getTableMetadata_ForTable2InNamespace1_ShouldReturnMetadataFromJdbcAdmin()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE2;

    // Act
    TableMetadata tableMetadata = multiStorageAdmin.getTableMetadata(namespace, table);

    // Assert
    assertThat(tableMetadata).isNotNull();
    assertThat(tableMetadata.getPartitionKeyNames().size()).isEqualTo(1);
    assertThat(tableMetadata.getPartitionKeyNames().iterator().next()).isEqualTo(COL_NAME1);

    assertThat(tableMetadata.getClusteringKeyNames()).isEmpty();

    assertThat(tableMetadata.getColumnNames().size()).isEqualTo(3);
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME1)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME2)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME3)).isTrue();

    assertThat(tableMetadata.getColumnDataType(COL_NAME1)).isEqualTo(DataType.TEXT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME2)).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME3)).isEqualTo(DataType.BOOLEAN);

    assertThat(tableMetadata.getClusteringOrder(COL_NAME1)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME2)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME3)).isNull();

    assertThat(tableMetadata.getSecondaryIndexNames()).isEmpty();
  }

  @Test
  public void getTableMetadata_ForTable3InNamespace1_ShouldReturnMetadataFromDefaultAdmin()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE3;

    // Act
    TableMetadata tableMetadata = multiStorageAdmin.getTableMetadata(namespace, table);

    // Assert
    assertThat(tableMetadata).isNotNull();
    assertThat(tableMetadata.getPartitionKeyNames().size()).isEqualTo(1);
    assertThat(tableMetadata.getPartitionKeyNames().iterator().next()).isEqualTo(COL_NAME1);

    assertThat(tableMetadata.getClusteringKeyNames().size()).isEqualTo(1);
    assertThat(tableMetadata.getClusteringKeyNames().iterator().next()).isEqualTo(COL_NAME4);

    assertThat(tableMetadata.getColumnNames().size()).isEqualTo(5);
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME1)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME2)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME3)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME4)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME5)).isTrue();

    assertThat(tableMetadata.getColumnDataType(COL_NAME1)).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME2)).isEqualTo(DataType.TEXT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME3)).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME4)).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME5)).isEqualTo(DataType.BOOLEAN);

    assertThat(tableMetadata.getClusteringOrder(COL_NAME1)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME2)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME3)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME4)).isEqualTo(Scan.Ordering.Order.ASC);
    assertThat(tableMetadata.getClusteringOrder(COL_NAME5)).isNull();

    assertThat(tableMetadata.getSecondaryIndexNames()).isEmpty();
  }

  @Test
  public void getTableMetadata_ForWrongTable_ShouldReturnNull() throws ExecutionException {
    // Arrange
    String namespace = "wrong_ns";
    String table = "wrong_table";

    // Act
    TableMetadata tableMetadata = multiStorageAdmin.getTableMetadata(namespace, table);

    // Assert
    assertThat(tableMetadata).isNull();
  }

  @Test
  public void getTableMetadata_ForTable1InNamespace2_ShouldReturnMetadataFromJdbcAdmin()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE2;
    String table = TABLE1;

    // Act
    TableMetadata tableMetadata = multiStorageAdmin.getTableMetadata(namespace, table);

    // Assert
    assertThat(tableMetadata).isNotNull();
    assertThat(tableMetadata.getPartitionKeyNames().size()).isEqualTo(1);
    assertThat(tableMetadata.getPartitionKeyNames().iterator().next()).isEqualTo(COL_NAME1);

    assertThat(tableMetadata.getClusteringKeyNames()).isEmpty();

    assertThat(tableMetadata.getColumnNames().size()).isEqualTo(3);
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME1)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME2)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME3)).isTrue();

    assertThat(tableMetadata.getColumnDataType(COL_NAME1)).isEqualTo(DataType.TEXT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME2)).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME3)).isEqualTo(DataType.BOOLEAN);

    assertThat(tableMetadata.getClusteringOrder(COL_NAME1)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME2)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME3)).isNull();

    assertThat(tableMetadata.getSecondaryIndexNames()).isEmpty();
  }

  @Test
  public void getNamespaceNames_ShouldReturnExistingNamespaces() throws ExecutionException {
    // Arrange

    // Act
    Set<String> namespaces = multiStorageAdmin.getNamespaceNames();

    // Assert
    assertThat(namespaces).containsExactlyInAnyOrder(NAMESPACE1, NAMESPACE2);
  }
}
