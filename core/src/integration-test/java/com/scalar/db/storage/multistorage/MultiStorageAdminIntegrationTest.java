package com.scalar.db.storage.multistorage;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.cassandra.CassandraAdmin;
import com.scalar.db.storage.jdbc.test.TestEnv;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class MultiStorageAdminIntegrationTest {

  protected static final String NAMESPACE1 = "integration_testing1";
  protected static final String NAMESPACE2 = "integration_testing2";
  protected static final String TABLE1 = "test_table1";
  protected static final String TABLE2 = "test_table2";
  protected static final String TABLE3 = "test_table3";
  protected static final String COL_NAME1 = "c1";
  protected static final String COL_NAME2 = "c2";
  protected static final String COL_NAME3 = "c3";
  protected static final String COL_NAME4 = "c4";
  protected static final String COL_NAME5 = "c5";

  private static final String CASSANDRA_CONTACT_POINT = "localhost";
  private static final String CASSANDRA_USERNAME = "cassandra";
  private static final String CASSANDRA_PASSWORD = "cassandra";

  private static final String MYSQL_CONTACT_POINT = "jdbc:mysql://localhost:3306/";
  private static final String MYSQL_USERNAME = "root";
  private static final String MYSQL_PASSWORD = "mysql";
  private static TestEnv testEnv;
  private static MultiStorageAdmin multiStorageAdmin;
  private static CassandraAdmin cassandraAdmin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    initCassandra();
    initMySql();
    initMultiStorageAdmin();
  }

  private static void initCassandra() throws Exception {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, CASSANDRA_CONTACT_POINT);
    props.setProperty(DatabaseConfig.USERNAME, CASSANDRA_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, CASSANDRA_PASSWORD);
    DatabaseConfig config = new DatabaseConfig(props);
    cassandraAdmin = new CassandraAdmin(config);
    cassandraAdmin.createNamespace(NAMESPACE1);
    cassandraAdmin.createNamespace(NAMESPACE2);
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
      cassandraAdmin.createTable(NAMESPACE1, table, tableMetadata);
    }
    cassandraAdmin.createTable(NAMESPACE2, TABLE1, tableMetadata);
  }

  private static void initMySql() throws Exception {
    testEnv = new TestEnv(MYSQL_CONTACT_POINT, MYSQL_USERNAME, MYSQL_PASSWORD, Optional.empty());
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(COL_NAME1, DataType.TEXT)
            .addColumn(COL_NAME2, DataType.INT)
            .addColumn(COL_NAME3, DataType.BOOLEAN)
            .addPartitionKey(COL_NAME1)
            .build();
    for (String table : Arrays.asList(TABLE1, TABLE2, TABLE3)) {
      testEnv.createTable(NAMESPACE1, table, tableMetadata);
    }
    testEnv.createTable(NAMESPACE2, TABLE1, tableMetadata);
  }

  private static void initMultiStorageAdmin() {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "multi-storage");

    // Define storages, cassandra and mysql
    props.setProperty(MultiStorageConfig.STORAGES, "cassandra,mysql");
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.storage", "cassandra");
    props.setProperty(
        MultiStorageConfig.STORAGES + ".cassandra.contact_points", CASSANDRA_CONTACT_POINT);
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.username", CASSANDRA_USERNAME);
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.password", CASSANDRA_PASSWORD);
    props.setProperty(MultiStorageConfig.STORAGES + ".mysql.storage", "jdbc");
    props.setProperty(MultiStorageConfig.STORAGES + ".mysql.contact_points", MYSQL_CONTACT_POINT);
    props.setProperty(MultiStorageConfig.STORAGES + ".mysql.username", MYSQL_USERNAME);
    props.setProperty(MultiStorageConfig.STORAGES + ".mysql.password", MYSQL_PASSWORD);

    // Define table mapping from table1 to cassandra, and from table2 to mysql
    props.setProperty(
        MultiStorageConfig.TABLE_MAPPING,
        NAMESPACE1 + "." + TABLE1 + ":cassandra," + NAMESPACE1 + "." + TABLE2 + ":mysql");

    // Define namespace mapping from namespace2 to mysql
    props.setProperty(MultiStorageConfig.NAMESPACE_MAPPING, NAMESPACE2 + ":mysql");

    // The default storage is cassandra
    props.setProperty(MultiStorageConfig.DEFAULT_STORAGE, "cassandra");

    multiStorageAdmin = new MultiStorageAdmin(new MultiStorageConfig(props));
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    multiStorageAdmin.close();
    cleanUpCassandra();
    cleanUpMySql();
  }

  private static void cleanUpCassandra() throws Exception {
    for (String table : Arrays.asList(TABLE1, TABLE2, TABLE3)) {
      cassandraAdmin.dropTable(NAMESPACE1, table);
    }
    cassandraAdmin.dropNamespace(NAMESPACE1);
    cassandraAdmin.dropTable(NAMESPACE2, TABLE1);
    cassandraAdmin.dropNamespace(NAMESPACE2);
    cassandraAdmin.close();
  }

  private static void cleanUpMySql() throws Exception {
    testEnv.deleteTables();
    testEnv.close();
  }

  @Test
  public void getTableMetadata_ForTable1_ShouldReturnMetadataFromCassandra()
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
  public void getTableMetadata_ForTable2_ShouldReturnMetadataFromMySql() throws ExecutionException {
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
  public void getTableMetadata_ForTable3_ShouldReturnMetadataFromDefaultAdmin()
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
  public void getTableMetadata_ForTable1InNamespace2_ShouldReturnMetadataFromMySql()
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
}
