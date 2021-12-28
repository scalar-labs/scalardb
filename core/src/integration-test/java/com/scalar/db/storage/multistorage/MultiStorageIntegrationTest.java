package com.scalar.db.storage.multistorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class MultiStorageIntegrationTest {

  private static final String NAMESPACE1 = "integration_testing1";
  private static final String NAMESPACE2 = "integration_testing2";
  private static final String TABLE1 = "test_table1";
  private static final String TABLE2 = "test_table2";
  private static final String TABLE3 = "test_table3";
  private static final String COL_NAME1 = "c1";
  private static final String COL_NAME2 = "c2";
  private static final String COL_NAME3 = "c3";
  private static final String COL_NAME4 = "c4";
  private static final String COL_NAME5 = "c5";

  private static final TableMetadata TABLE_METADATA =
      TableMetadata.newBuilder()
          .addColumn(COL_NAME1, DataType.INT)
          .addColumn(COL_NAME2, DataType.TEXT)
          .addColumn(COL_NAME3, DataType.INT)
          .addColumn(COL_NAME4, DataType.INT)
          .addColumn(COL_NAME5, DataType.BOOLEAN)
          .addPartitionKey(COL_NAME1)
          .addClusteringKey(COL_NAME4)
          .build();

  private static DistributedStorage storage1;
  private static DistributedStorageAdmin admin1;
  private static DistributedStorage storage2;
  private static DistributedStorageAdmin admin2;
  private static MultiStorage multiStorage;

  @BeforeClass
  public static void setUpBeforeClass() throws ExecutionException {
    initStorage1AndAdmin1();
    initStorage2AndAdmin2();
    initMultiStorage();
  }

  private static void initStorage1AndAdmin1() throws ExecutionException {
    StorageFactory factory = new StorageFactory(MultiStorageEnv.getDatabaseConfigForStorage1());
    admin1 = factory.getAdmin();
    createTables(admin1);
    storage1 = factory.getStorage();
  }

  private static void initStorage2AndAdmin2() throws ExecutionException {
    StorageFactory factory = new StorageFactory(MultiStorageEnv.getDatabaseConfigForStorage2());
    admin2 = factory.getAdmin();
    createTables(admin2);
    storage2 = factory.getStorage();
  }

  private static void createTables(DistributedStorageAdmin admin) throws ExecutionException {
    admin.createNamespace(NAMESPACE1, true);
    for (String table : Arrays.asList(TABLE1, TABLE2, TABLE3)) {
      admin.createTable(NAMESPACE1, table, TABLE_METADATA, true);
    }
    admin.createNamespace(NAMESPACE2, true);
    admin.createTable(NAMESPACE2, TABLE1, TABLE_METADATA, true);
  }

  private static void initMultiStorage() {
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

    // Define table mapping from table1 to storage1, and from table2 to storage2
    props.setProperty(
        MultiStorageConfig.TABLE_MAPPING,
        NAMESPACE1 + "." + TABLE1 + ":storage1," + NAMESPACE1 + "." + TABLE2 + ":storage2");

    // Define namespace mapping from namespace2 to storage2
    props.setProperty(MultiStorageConfig.NAMESPACE_MAPPING, NAMESPACE2 + ":storage2");

    // The default storage is storage1
    props.setProperty(MultiStorageConfig.DEFAULT_STORAGE, "storage1");

    multiStorage = new MultiStorage(new MultiStorageConfig(props));
  }

  @Before
  public void setUp() throws ExecutionException {
    truncateTables(admin1);
    truncateTables(admin2);
  }

  private void truncateTables(DistributedStorageAdmin admin) throws ExecutionException {
    for (String table : Arrays.asList(TABLE1, TABLE2, TABLE3)) {
      admin.truncateTable(NAMESPACE1, table);
    }
    admin.truncateTable(NAMESPACE2, TABLE1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws ExecutionException {
    multiStorage.close();
    cleanUp(storage1, admin1);
    cleanUp(storage2, admin2);
  }

  private static void cleanUp(DistributedStorage storage, DistributedStorageAdmin admin)
      throws ExecutionException {
    storage.close();
    for (String table : Arrays.asList(TABLE1, TABLE2, TABLE3)) {
      admin.dropTable(NAMESPACE1, table);
    }
    admin.dropNamespace(NAMESPACE1);

    admin.dropTable(NAMESPACE2, TABLE1);
    admin.dropNamespace(NAMESPACE2);
    admin.close();
  }

  @Test
  public void whenPutDataIntoTable1_DataShouldBeWrittenIntoStorage1() throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE1;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey = new Key(COL_NAME4, 4);
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValue(COL_NAME2, "val2")
            .withValue(COL_NAME3, 3)
            .withValue(COL_NAME5, true)
            .forNamespace(namespace)
            .forTable(table);

    // Act
    multiStorage.put(put);

    // Assert
    Get get = new Get(partitionKey, clusteringKey).forNamespace(namespace).forTable(table);

    Optional<Result> result = multiStorage.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol2Value(result.get())).isEqualTo("val2");
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(4);
    assertThat(getCol5Value(result.get())).isTrue();

    result = storage1.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol2Value(result.get())).isEqualTo("val2");
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(4);
    assertThat(getCol5Value(result.get())).isTrue();

    result = storage2.get(get);
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void whenPutDataIntoTable2_DataShouldBeWrittenIntoStorage2() throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE2;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey = new Key(COL_NAME4, 4);
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValue(COL_NAME2, "val2")
            .withValue(COL_NAME3, 3)
            .withValue(COL_NAME5, true)
            .forNamespace(namespace)
            .forTable(table);

    // Act
    multiStorage.put(put);

    // Assert
    Get get = new Get(partitionKey, clusteringKey).forNamespace(namespace).forTable(table);

    Optional<Result> result = multiStorage.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol2Value(result.get())).isEqualTo("val2");
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(4);
    assertThat(getCol5Value(result.get())).isTrue();

    result = storage1.get(get);
    assertThat(result.isPresent()).isFalse();

    result = storage2.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol2Value(result.get())).isEqualTo("val2");
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(4);
    assertThat(getCol5Value(result.get())).isTrue();
  }

  @Test
  public void whenPutDataIntoTable3_DataShouldBeWrittenIntoDefaultStorage()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE3;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey = new Key(COL_NAME4, 4);

    Put put =
        new Put(partitionKey, clusteringKey)
            .withValue(COL_NAME2, "val2")
            .withValue(COL_NAME3, 3)
            .withValue(COL_NAME5, true)
            .forNamespace(namespace)
            .forTable(table);

    // Act
    multiStorage.put(put);

    // Assert
    Get get = new Get(partitionKey, clusteringKey).forNamespace(namespace).forTable(table);

    Optional<Result> result = multiStorage.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol2Value(result.get())).isEqualTo("val2");
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(4);
    assertThat(getCol5Value(result.get())).isTrue();

    result = storage1.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol2Value(result.get())).isEqualTo("val2");
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(4);
    assertThat(getCol5Value(result.get())).isTrue();

    result = storage2.get(get);
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void whenScanDataFromTable1_DataShouldBeScannedFromStorage1()
      throws ExecutionException, IOException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE1;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey1 = new Key(COL_NAME4, 0);
    Key clusteringKey2 = new Key(COL_NAME4, 1);
    Key clusteringKey3 = new Key(COL_NAME4, 2);

    storage1.mutate(
        Arrays.asList(
            new Put(partitionKey, clusteringKey1)
                .withValue(COL_NAME3, 2)
                .forNamespace(namespace)
                .forTable(table),
            new Put(partitionKey, clusteringKey2)
                .withValue(COL_NAME3, 1)
                .forNamespace(namespace)
                .forTable(table),
            new Put(partitionKey, clusteringKey3)
                .withValue(COL_NAME3, 0)
                .forNamespace(namespace)
                .forTable(table)));

    // Act
    List<Result> results = scanAll(new Scan(partitionKey).forNamespace(namespace).forTable(table));

    // Assert
    assertThat(results.size()).isEqualTo(3);
    assertThat(getCol1Value(results.get(0))).isEqualTo(1);
    assertThat(getCol3Value(results.get(0))).isEqualTo(2);
    assertThat(getCol4Value(results.get(0))).isEqualTo(0);
    assertThat(getCol1Value(results.get(1))).isEqualTo(1);
    assertThat(getCol3Value(results.get(1))).isEqualTo(1);
    assertThat(getCol4Value(results.get(1))).isEqualTo(1);
    assertThat(getCol1Value(results.get(2))).isEqualTo(1);
    assertThat(getCol3Value(results.get(2))).isEqualTo(0);
    assertThat(getCol4Value(results.get(2))).isEqualTo(2);
  }

  @Test
  public void whenScanDataFromTable2_DataShouldBeScannedFromStorage2()
      throws ExecutionException, IOException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE2;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey1 = new Key(COL_NAME4, 0);
    Key clusteringKey2 = new Key(COL_NAME4, 1);
    Key clusteringKey3 = new Key(COL_NAME4, 2);

    storage2.mutate(
        Arrays.asList(
            new Put(partitionKey, clusteringKey1)
                .withValue(COL_NAME3, 2)
                .forNamespace(namespace)
                .forTable(table),
            new Put(partitionKey, clusteringKey2)
                .withValue(COL_NAME3, 1)
                .forNamespace(namespace)
                .forTable(table),
            new Put(partitionKey, clusteringKey3)
                .withValue(COL_NAME3, 0)
                .forNamespace(namespace)
                .forTable(table)));

    // Act
    List<Result> results = scanAll(new Scan(partitionKey).forNamespace(namespace).forTable(table));

    // Assert
    assertThat(results.size()).isEqualTo(3);
    assertThat(getCol1Value(results.get(0))).isEqualTo(1);
    assertThat(getCol3Value(results.get(0))).isEqualTo(2);
    assertThat(getCol4Value(results.get(0))).isEqualTo(0);
    assertThat(getCol1Value(results.get(1))).isEqualTo(1);
    assertThat(getCol3Value(results.get(1))).isEqualTo(1);
    assertThat(getCol4Value(results.get(1))).isEqualTo(1);
    assertThat(getCol1Value(results.get(2))).isEqualTo(1);
    assertThat(getCol3Value(results.get(2))).isEqualTo(0);
    assertThat(getCol4Value(results.get(2))).isEqualTo(2);
  }

  @Test
  public void whenScanDataFromTable3_DataShouldBeScannedFromDefaultStorage()
      throws IOException, ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE3;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey1 = new Key(COL_NAME4, 0);
    Key clusteringKey2 = new Key(COL_NAME4, 1);
    Key clusteringKey3 = new Key(COL_NAME4, 2);

    storage1.mutate(
        Arrays.asList(
            new Put(partitionKey, clusteringKey1)
                .withValue(COL_NAME3, 2)
                .forNamespace(namespace)
                .forTable(table),
            new Put(partitionKey, clusteringKey2)
                .withValue(COL_NAME3, 1)
                .forNamespace(namespace)
                .forTable(table),
            new Put(partitionKey, clusteringKey3)
                .withValue(COL_NAME3, 0)
                .forNamespace(namespace)
                .forTable(table)));

    // Act
    List<Result> results = scanAll(new Scan(partitionKey).forNamespace(namespace).forTable(table));

    // Assert
    assertThat(results.size()).isEqualTo(3);
    assertThat(getCol1Value(results.get(0))).isEqualTo(1);
    assertThat(getCol3Value(results.get(0))).isEqualTo(2);
    assertThat(getCol4Value(results.get(0))).isEqualTo(0);
    assertThat(getCol1Value(results.get(1))).isEqualTo(1);
    assertThat(getCol3Value(results.get(1))).isEqualTo(1);
    assertThat(getCol4Value(results.get(1))).isEqualTo(1);
    assertThat(getCol1Value(results.get(2))).isEqualTo(1);
    assertThat(getCol3Value(results.get(2))).isEqualTo(0);
    assertThat(getCol4Value(results.get(2))).isEqualTo(2);
  }

  private List<Result> scanAll(Scan scan) throws ExecutionException, IOException {
    try (Scanner scanner = multiStorage.scan(scan)) {
      return scanner.all();
    }
  }

  @Test
  public void whenDeleteDataFromTable1_DataShouldBeDeletedFromStorage1() throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE1;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey = new Key(COL_NAME4, 4);
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValue(COL_NAME3, 3)
            .forNamespace(namespace)
            .forTable(table);

    storage1.put(put);
    storage2.put(put);

    // Act
    multiStorage.delete(
        new Delete(partitionKey, clusteringKey).forNamespace(namespace).forTable(table));

    // Assert
    Get get = new Get(partitionKey, clusteringKey).forNamespace(namespace).forTable(table);

    Optional<Result> result = multiStorage.get(get);
    assertThat(result.isPresent()).isFalse();

    result = storage1.get(get);
    assertThat(result.isPresent()).isFalse();

    result = storage2.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(4);
  }

  @Test
  public void whenDeleteDataFromTable2_DataShouldBeDeletedFromStorage2() throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE2;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey = new Key(COL_NAME4, 4);
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValue(COL_NAME3, 3)
            .forNamespace(namespace)
            .forTable(table);

    storage1.put(put);
    storage2.put(put);

    // Act
    multiStorage.delete(
        new Delete(partitionKey, clusteringKey).forNamespace(namespace).forTable(table));

    // Assert
    Get get = new Get(partitionKey, clusteringKey).forNamespace(namespace).forTable(table);

    Optional<Result> result = multiStorage.get(get);
    assertThat(result.isPresent()).isFalse();

    result = storage1.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(4);

    result = storage2.get(get);
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void whenDeleteDataFromTable3_DataShouldBeDeletedFromDefaultStorage()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE3;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey = new Key(COL_NAME4, 4);
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValue(COL_NAME3, 3)
            .forNamespace(namespace)
            .forTable(table);

    storage1.put(put);
    storage2.put(put);

    // Act
    multiStorage.delete(
        new Delete(partitionKey, clusteringKey).forNamespace(namespace).forTable(table));

    // Assert
    Get get = new Get(partitionKey, clusteringKey).forNamespace(namespace).forTable(table);

    Optional<Result> result = multiStorage.get(get);
    assertThat(result.isPresent()).isFalse();

    result = storage1.get(get);
    assertThat(result.isPresent()).isFalse();

    result = storage2.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(4);
  }

  @Test
  public void whenMutateDataToTable1_ShouldExecuteForStorage1() throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE1;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey1 = new Key(COL_NAME4, 1);
    Key clusteringKey2 = new Key(COL_NAME4, 2);
    Put put =
        new Put(partitionKey, clusteringKey1)
            .withValue(COL_NAME3, 3)
            .forNamespace(namespace)
            .forTable(table);

    storage1.put(put);
    storage2.put(put);

    // Act
    multiStorage.mutate(
        Arrays.asList(
            new Put(partitionKey, clusteringKey2)
                .withValue(COL_NAME3, 3)
                .forNamespace(namespace)
                .forTable(table),
            new Delete(partitionKey, clusteringKey1).forNamespace(namespace).forTable(table)));

    // Assert
    Get get1 = new Get(partitionKey, clusteringKey1).forNamespace(namespace).forTable(table);
    Get get2 = new Get(partitionKey, clusteringKey2).forNamespace(namespace).forTable(table);

    Optional<Result> result = multiStorage.get(get1);
    assertThat(result.isPresent()).isFalse();
    result = multiStorage.get(get2);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(2);

    result = storage1.get(get1);
    assertThat(result.isPresent()).isFalse();
    result = storage1.get(get2);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(2);

    result = storage2.get(get1);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(1);
    result = storage2.get(get2);
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void whenMutateDataToTable2_ShouldExecuteForStorage2() throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE2;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey1 = new Key(COL_NAME4, 1);
    Key clusteringKey2 = new Key(COL_NAME4, 2);
    Put put =
        new Put(partitionKey, clusteringKey1)
            .withValue(COL_NAME3, 3)
            .forNamespace(namespace)
            .forTable(table);

    storage1.put(put);
    storage2.put(put);

    // Act
    multiStorage.mutate(
        Arrays.asList(
            new Put(partitionKey, clusteringKey2)
                .withValue(COL_NAME3, 3)
                .forNamespace(namespace)
                .forTable(table),
            new Delete(partitionKey, clusteringKey1).forNamespace(namespace).forTable(table)));

    // Assert
    Get get1 = new Get(partitionKey, clusteringKey1).forNamespace(namespace).forTable(table);
    Get get2 = new Get(partitionKey, clusteringKey2).forNamespace(namespace).forTable(table);

    Optional<Result> result = multiStorage.get(get1);
    assertThat(result.isPresent()).isFalse();
    result = multiStorage.get(get2);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(2);

    result = storage1.get(get1);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(1);
    result = storage1.get(get2);
    assertThat(result.isPresent()).isFalse();

    result = storage2.get(get1);
    assertThat(result.isPresent()).isFalse();
    result = storage2.get(get2);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(2);
  }

  @Test
  public void whenMutateDataToTable3_ShouldExecuteForDefaultStorage() throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE3;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey1 = new Key(COL_NAME4, 1);
    Key clusteringKey2 = new Key(COL_NAME4, 2);
    Put put =
        new Put(partitionKey, clusteringKey1)
            .withValue(COL_NAME3, 3)
            .forNamespace(namespace)
            .forTable(table);

    storage1.put(put);
    storage2.put(put);

    // Act
    multiStorage.mutate(
        Arrays.asList(
            new Put(partitionKey, clusteringKey2)
                .withValue(COL_NAME3, 3)
                .forNamespace(namespace)
                .forTable(table),
            new Delete(partitionKey, clusteringKey1).forNamespace(namespace).forTable(table)));

    // Assert
    Get get1 = new Get(partitionKey, clusteringKey1).forNamespace(namespace).forTable(table);
    Get get2 = new Get(partitionKey, clusteringKey2).forNamespace(namespace).forTable(table);

    Optional<Result> result = multiStorage.get(get1);
    assertThat(result.isPresent()).isFalse();
    result = multiStorage.get(get2);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(2);

    result = storage1.get(get1);
    assertThat(result.isPresent()).isFalse();
    result = storage1.get(get2);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(2);

    result = storage2.get(get1);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(1);
    result = storage2.get(get2);
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void whenPutDataIntoTable1InNamespace2_DataShouldBeWrittenIntoStorage2()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE2;
    String table = TABLE1;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey = new Key(COL_NAME4, 4);
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValue(COL_NAME2, "val2")
            .withValue(COL_NAME3, 3)
            .withValue(COL_NAME5, true)
            .forNamespace(namespace)
            .forTable(table);

    // Act
    multiStorage.put(put);

    // Assert
    Get get = new Get(partitionKey, clusteringKey).forNamespace(namespace).forTable(table);

    Optional<Result> result = multiStorage.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol2Value(result.get())).isEqualTo("val2");
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(4);
    assertThat(getCol5Value(result.get())).isTrue();

    result = storage1.get(get);
    assertThat(result.isPresent()).isFalse();

    result = storage2.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol2Value(result.get())).isEqualTo("val2");
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(4);
    assertThat(getCol5Value(result.get())).isTrue();
  }

  @Test
  public void whenScanDataFromTable1InNamespace2_DataShouldBeScannedFromStorage2()
      throws ExecutionException, IOException {
    // Arrange
    String namespace = NAMESPACE2;
    String table = TABLE1;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey1 = new Key(COL_NAME4, 0);
    Key clusteringKey2 = new Key(COL_NAME4, 1);
    Key clusteringKey3 = new Key(COL_NAME4, 2);

    storage2.mutate(
        Arrays.asList(
            new Put(partitionKey, clusteringKey1)
                .withValue(COL_NAME3, 2)
                .forNamespace(namespace)
                .forTable(table),
            new Put(partitionKey, clusteringKey2)
                .withValue(COL_NAME3, 1)
                .forNamespace(namespace)
                .forTable(table),
            new Put(partitionKey, clusteringKey3)
                .withValue(COL_NAME3, 0)
                .forNamespace(namespace)
                .forTable(table)));

    // Act
    List<Result> results = scanAll(new Scan(partitionKey).forNamespace(namespace).forTable(table));

    // Assert
    assertThat(results.size()).isEqualTo(3);
    assertThat(getCol1Value(results.get(0))).isEqualTo(1);
    assertThat(getCol3Value(results.get(0))).isEqualTo(2);
    assertThat(getCol4Value(results.get(0))).isEqualTo(0);
    assertThat(getCol1Value(results.get(1))).isEqualTo(1);
    assertThat(getCol3Value(results.get(1))).isEqualTo(1);
    assertThat(getCol4Value(results.get(1))).isEqualTo(1);
    assertThat(getCol1Value(results.get(2))).isEqualTo(1);
    assertThat(getCol3Value(results.get(2))).isEqualTo(0);
    assertThat(getCol4Value(results.get(2))).isEqualTo(2);
  }

  @Test
  public void whenDeleteDataFromTable1InNamespace2_DataShouldBeDeletedFromStorage2()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE2;
    String table = TABLE1;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey = new Key(COL_NAME4, 4);
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValue(COL_NAME3, 3)
            .forNamespace(namespace)
            .forTable(table);

    storage1.put(put);
    storage2.put(put);

    // Act
    multiStorage.delete(
        new Delete(partitionKey, clusteringKey).forNamespace(namespace).forTable(table));

    // Assert
    Get get = new Get(partitionKey, clusteringKey).forNamespace(namespace).forTable(table);

    Optional<Result> result = multiStorage.get(get);
    assertThat(result.isPresent()).isFalse();

    result = storage1.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(4);

    result = storage2.get(get);
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void whenMutateDataToTable1InNamespace2_ShouldExecuteForStorage1()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE2;
    String table = TABLE1;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey1 = new Key(COL_NAME4, 1);
    Key clusteringKey2 = new Key(COL_NAME4, 2);
    Put put =
        new Put(partitionKey, clusteringKey1)
            .withValue(COL_NAME3, 3)
            .forNamespace(namespace)
            .forTable(table);

    storage1.put(put);
    storage2.put(put);

    // Act
    multiStorage.mutate(
        Arrays.asList(
            new Put(partitionKey, clusteringKey2)
                .withValue(COL_NAME3, 3)
                .forNamespace(namespace)
                .forTable(table),
            new Delete(partitionKey, clusteringKey1).forNamespace(namespace).forTable(table)));

    // Assert
    Get get1 = new Get(partitionKey, clusteringKey1).forNamespace(namespace).forTable(table);
    Get get2 = new Get(partitionKey, clusteringKey2).forNamespace(namespace).forTable(table);

    Optional<Result> result = multiStorage.get(get1);
    assertThat(result.isPresent()).isFalse();
    result = multiStorage.get(get2);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(2);

    result = storage1.get(get1);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(1);
    result = storage1.get(get2);
    assertThat(result.isPresent()).isFalse();

    result = storage2.get(get1);
    assertThat(result.isPresent()).isFalse();
    result = storage2.get(get2);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(2);
  }

  @Test
  public void whenCallMutateWithEmptyList_ShouldThrowIllegalArgumentException() {
    // Arrange Act Assert
    assertThatThrownBy(() -> multiStorage.mutate(Collections.emptyList()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private int getCol1Value(Result result) {
    assertThat(result.getValue(COL_NAME1).isPresent()).isTrue();
    return result.getValue(COL_NAME1).get().getAsInt();
  }

  private String getCol2Value(Result result) {
    assertThat(result.getValue(COL_NAME2).isPresent()).isTrue();
    assertThat(result.getValue(COL_NAME2).get().getAsString().isPresent()).isTrue();
    return result.getValue(COL_NAME2).get().getAsString().get();
  }

  private int getCol3Value(Result result) {
    assertThat(result.getValue(COL_NAME3).isPresent()).isTrue();
    return result.getValue(COL_NAME3).get().getAsInt();
  }

  private int getCol4Value(Result result) {
    assertThat(result.getValue(COL_NAME4).isPresent()).isTrue();
    return result.getValue(COL_NAME4).get().getAsInt();
  }

  private boolean getCol5Value(Result result) {
    assertThat(result.getValue(COL_NAME5).isPresent()).isTrue();
    return result.getValue(COL_NAME5).get().getAsBoolean();
  }
}
