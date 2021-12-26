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
import java.util.Arrays;
import java.util.Properties;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class MultiStorageAdminIntegrationTest {

  private static final String NAMESPACE1 = "integration_testing1";
  private static final String NAMESPACE2 = "integration_testing2";
  private static final String NAMESPACE3 = "integration_testing3";
  private static final String TABLE1 = "test_table1";
  private static final String TABLE2 = "test_table2";
  private static final String TABLE3 = "test_table3";
  private static final String COL_NAME1 = "c1";
  private static final String COL_NAME2 = "c2";
  private static final String COL_NAME3 = "c3";
  private static final String COL_NAME4 = "c4";
  private static final String COL_NAME5 = "c5";

  private static DistributedStorageAdmin admin1;
  private static DistributedStorageAdmin admin2;
  private static MultiStorageAdmin multiStorageAdmin;

  @BeforeClass
  public static void setUpBeforeClass() throws ExecutionException {
    initAdmin1();
    initAdmin2();
    initMultiStorageAdmin();
  }

  private static void initAdmin1() throws ExecutionException {
    StorageFactory factory = new StorageFactory(MultiStorageEnv.getDatabaseConfigForStorage1());
    admin1 = factory.getAdmin();

    // create tables
    admin1.createNamespace(NAMESPACE1, true);
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
      admin1.createTable(NAMESPACE1, table, tableMetadata, true);
    }

    admin1.createNamespace(NAMESPACE2, true);
    admin1.createTable(NAMESPACE2, TABLE1, tableMetadata, true);
  }

  private static void initAdmin2() throws ExecutionException {
    StorageFactory factory = new StorageFactory(MultiStorageEnv.getDatabaseConfigForStorage2());
    admin2 = factory.getAdmin();

    // create tables
    admin2.createNamespace(NAMESPACE1, true);
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(COL_NAME1, DataType.TEXT)
            .addColumn(COL_NAME2, DataType.INT)
            .addColumn(COL_NAME3, DataType.BOOLEAN)
            .addPartitionKey(COL_NAME1)
            .build();
    for (String table : Arrays.asList(TABLE1, TABLE2, TABLE3)) {
      admin2.createTable(NAMESPACE1, table, tableMetadata, true);
    }

    admin2.createNamespace(NAMESPACE2, true);
    admin2.createTable(NAMESPACE2, TABLE1, tableMetadata, true);
  }

  private static void initMultiStorageAdmin() {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "multi-storage");

    // Define storages, storage1 and storage2
    props.setProperty(MultiStorageConfig.STORAGES, "storage1,storage2");

    DatabaseConfig configForStorage1 = MultiStorageEnv.getDatabaseConfigForStorage1();
    props.setProperty(
        MultiStorageConfig.STORAGES + ".storage1.storage",
        configForStorage1.getProperties().getProperty(DatabaseConfig.STORAGE));
    props.setProperty(
        MultiStorageConfig.STORAGES + ".storage1.contact_points",
        configForStorage1.getProperties().getProperty(DatabaseConfig.CONTACT_POINTS));
    if (configForStorage1.getProperties().containsValue(DatabaseConfig.CONTACT_PORT)) {
      props.setProperty(
          MultiStorageConfig.STORAGES + ".storage1.contact_port",
          configForStorage1.getProperties().getProperty(DatabaseConfig.CONTACT_PORT));
    }
    props.setProperty(
        MultiStorageConfig.STORAGES + ".storage1.username",
        configForStorage1.getProperties().getProperty(DatabaseConfig.USERNAME));
    props.setProperty(
        MultiStorageConfig.STORAGES + ".storage1.password",
        configForStorage1.getProperties().getProperty(DatabaseConfig.PASSWORD));

    DatabaseConfig configForStorage2 = MultiStorageEnv.getDatabaseConfigForStorage2();
    props.setProperty(
        MultiStorageConfig.STORAGES + ".storage2.storage",
        configForStorage2.getProperties().getProperty(DatabaseConfig.STORAGE));
    props.setProperty(
        MultiStorageConfig.STORAGES + ".storage2.contact_points",
        configForStorage2.getProperties().getProperty(DatabaseConfig.CONTACT_POINTS));
    if (configForStorage2.getProperties().containsValue(DatabaseConfig.CONTACT_PORT)) {
      props.setProperty(
          MultiStorageConfig.STORAGES + ".storage2.contact_port",
          configForStorage2.getProperties().getProperty(DatabaseConfig.CONTACT_PORT));
    }
    props.setProperty(
        MultiStorageConfig.STORAGES + ".storage2.username",
        configForStorage2.getProperties().getProperty(DatabaseConfig.USERNAME));
    props.setProperty(
        MultiStorageConfig.STORAGES + ".storage2.password",
        configForStorage2.getProperties().getProperty(DatabaseConfig.PASSWORD));

    // Define table mapping from table1 in namespace1 to storage1, and from table2 in namespace1 to
    // storage2
    props.setProperty(
        MultiStorageConfig.TABLE_MAPPING,
        NAMESPACE1 + "." + TABLE1 + ":storage1," + NAMESPACE1 + "." + TABLE2 + ":storage2");

    // Define namespace mapping from namespace2 and namespace3 to storage2
    props.setProperty(
        MultiStorageConfig.NAMESPACE_MAPPING, NAMESPACE2 + ":storage2," + NAMESPACE3 + ":storage2");

    // The default storage is storage1
    props.setProperty(MultiStorageConfig.DEFAULT_STORAGE, "storage1");

    multiStorageAdmin = new MultiStorageAdmin(new MultiStorageConfig(props));
  }

  @AfterClass
  public static void tearDownAfterClass() throws ExecutionException {
    multiStorageAdmin.close();
    cleanUp(admin1);
    cleanUp(admin2);
  }

  private static void cleanUp(DistributedStorageAdmin admin) throws ExecutionException {
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
    multiStorageAdmin.createNamespace(NAMESPACE3, true);
    assertThat(admin1.namespaceExists(NAMESPACE3)).isFalse();
    assertThat(admin2.namespaceExists(NAMESPACE3)).isTrue();

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
        true);

    assertThat(admin2.getNamespaceTableNames(NAMESPACE3).contains(TABLE1)).isTrue();

    // truncateTable
    assertThatCode(() -> multiStorageAdmin.truncateTable(NAMESPACE3, TABLE1))
        .doesNotThrowAnyException();

    // dropTable
    multiStorageAdmin.dropTable(NAMESPACE3, TABLE1);

    assertThat(admin2.getNamespaceTableNames(NAMESPACE3).contains(TABLE1)).isFalse();

    // dropNamespace
    multiStorageAdmin.dropNamespace(NAMESPACE3);
    assertThat(admin2.namespaceExists(NAMESPACE3)).isFalse();
  }

  @Test
  public void getTableMetadata_ForTable1InNamespace1_ShouldReturnMetadataFromStorage1()
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
  public void getTableMetadata_ForTable2InNamespace1_ShouldReturnMetadataFromStorage2()
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
  public void getTableMetadata_ForTable1InNamespace2_ShouldReturnMetadataFromStorage2()
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
