package com.scalar.db.storage.multistorage;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.test.TestEnv;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class MultiStorageAdminIntegrationTest {

  protected static final String NAMESPACE = "integration_testing";
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

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    initCassandra();
    initMySql();
    initMultiStorageAdmin();
  }

  @Test
  public void getTableMetadata_ForTable1_ShouldReturnMetadataFromCassandra() {
    // Arrange
    String namespace = NAMESPACE;
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
  public void getTableMetadata_ForTable2_ShouldReturnMetadataFromMySql() {
    // Arrange
    String namespace = NAMESPACE;
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
  public void getTableMetadata_ForTable3_ShouldReturnMetadataFromDefaultAdmin() {
    // Arrange
    String namespace = NAMESPACE;
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
  public void getTableMetadata_ForWrongTable_ShouldReturnNull() {
    // Arrange
    String namespace = "wrong_ns";
    String table = "wrong_table";

    // Act
    TableMetadata tableMetadata = multiStorageAdmin.getTableMetadata(namespace, table);

    // Assert
    assertThat(tableMetadata).isNull();
  }

  private static void initCassandra() throws Exception {
    ProcessBuilder builder;
    Process process;
    int ret;

    String createKeyspaceStmt =
        "CREATE KEYSPACE "
            + NAMESPACE
            + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }";
    builder =
        new ProcessBuilder(
            "cqlsh", "-u", CASSANDRA_USERNAME, "-p", CASSANDRA_PASSWORD, "-e", createKeyspaceStmt);
    process = builder.start();
    ret = process.waitFor();
    if (ret != 0) {
      Assert.fail("CREATE KEYSPACE failed.");
    }

    for (String table : Arrays.asList(TABLE1, TABLE2, TABLE3)) {
      String createTableStmt =
          "CREATE TABLE "
              + NAMESPACE
              + "."
              + table
              + " (c1 int, c2 text, c3 int, c4 int, c5 boolean, PRIMARY KEY((c1), c4))";

      builder =
          new ProcessBuilder(
              "cqlsh", "-u", CASSANDRA_USERNAME, "-p", CASSANDRA_PASSWORD, "-e", createTableStmt);
      process = builder.start();
      ret = process.waitFor();
      if (ret != 0) {
        Assert.fail("CREATE TABLE failed: " + table);
      }
    }
  }

  private static void initMySql() throws Exception {
    testEnv = new TestEnv(MYSQL_CONTACT_POINT, MYSQL_USERNAME, MYSQL_PASSWORD, Optional.empty());

    for (String table : Arrays.asList(TABLE1, TABLE2, TABLE3)) {
      testEnv.register(
          NAMESPACE,
          table,
          TableMetadata.newBuilder()
              .addColumn(COL_NAME1, DataType.TEXT)
              .addColumn(COL_NAME2, DataType.INT)
              .addColumn(COL_NAME3, DataType.BOOLEAN)
              .addPartitionKey(COL_NAME1)
              .build());
    }
    testEnv.createMetadataTable();
    testEnv.createTables();
    testEnv.insertMetadata();
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
        NAMESPACE + "." + TABLE1 + ":cassandra," + NAMESPACE + "." + TABLE2 + ":mysql");

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
    ProcessBuilder builder;
    Process process;
    int ret;

    String dropKeyspaceStmt = "DROP KEYSPACE " + NAMESPACE;
    builder =
        new ProcessBuilder(
            "cqlsh", "-u", CASSANDRA_USERNAME, "-p", CASSANDRA_PASSWORD, "-e", dropKeyspaceStmt);
    process = builder.start();
    ret = process.waitFor();
    if (ret != 0) {
      Assert.fail("DROP KEYSPACE failed.");
    }
  }

  private static void cleanUpMySql() throws Exception {
    testEnv.dropMetadataTable();
    testEnv.dropTables();
    testEnv.close();
  }
}
