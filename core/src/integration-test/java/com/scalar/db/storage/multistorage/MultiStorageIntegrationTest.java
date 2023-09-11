package com.scalar.db.storage.multistorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
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
import com.scalar.db.storage.cassandra.CassandraAdmin;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MultiStorageIntegrationTest {

  private static final String TEST_NAME = "mstorage";
  private static final String NAMESPACE1 = "int_test_" + TEST_NAME + "1";
  private static final String NAMESPACE2 = "int_test_" + TEST_NAME + "2";
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

  private DistributedStorage cassandra;
  private DistributedStorageAdmin cassandraAdmin;
  private DistributedStorage jdbcDatabase;
  private DistributedStorageAdmin jdbcAdmin;
  private MultiStorage multiStorage;

  @BeforeAll
  public void beforeAll() throws ExecutionException {
    initCassandraAndCassandraAdmin();
    initJdbcDatabaseAndJdbcAdmin();
    initMultiStorage();
  }

  private void initCassandraAndCassandraAdmin() throws ExecutionException {
    StorageFactory factory =
        StorageFactory.create(MultiStorageEnv.getPropertiesForCassandra(TEST_NAME));
    cassandraAdmin = factory.getAdmin();
    createTables(cassandraAdmin);
    cassandra = factory.getStorage();
  }

  private void initJdbcDatabaseAndJdbcAdmin() throws ExecutionException {
    StorageFactory factory = StorageFactory.create(MultiStorageEnv.getPropertiesForJdbc(TEST_NAME));
    jdbcAdmin = factory.getAdmin();
    createTables(jdbcAdmin);
    jdbcDatabase = factory.getStorage();
  }

  private void createTables(DistributedStorageAdmin admin) throws ExecutionException {
    admin.createNamespace(NAMESPACE1, true, getCreationOptions());
    for (String table : Arrays.asList(TABLE1, TABLE2, TABLE3)) {
      admin.createTable(NAMESPACE1, table, TABLE_METADATA, true, getCreationOptions());
    }
    admin.createNamespace(NAMESPACE2, true, getCreationOptions());
    admin.createTable(NAMESPACE2, TABLE1, TABLE_METADATA, true, getCreationOptions());
  }

  private Map<String, String> getCreationOptions() {
    return Collections.singletonMap(CassandraAdmin.REPLICATION_FACTOR, "1");
  }

  private void initMultiStorage() {
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

    // Define table mapping from table1 to cassandra, and from table2 to jdbc
    props.setProperty(
        MultiStorageConfig.TABLE_MAPPING,
        NAMESPACE1 + "." + TABLE1 + ":cassandra," + NAMESPACE1 + "." + TABLE2 + ":jdbc");

    // Define namespace mapping from namespace2 to jdbc
    props.setProperty(MultiStorageConfig.NAMESPACE_MAPPING, NAMESPACE2 + ":jdbc");

    // The default storage is cassandra
    props.setProperty(MultiStorageConfig.DEFAULT_STORAGE, "cassandra");

    multiStorage = new MultiStorage(new DatabaseConfig(props));
  }

  @BeforeEach
  public void setUp() throws ExecutionException {
    truncateTables(cassandraAdmin);
    truncateTables(jdbcAdmin);
  }

  private void truncateTables(DistributedStorageAdmin admin) throws ExecutionException {
    for (String table : Arrays.asList(TABLE1, TABLE2, TABLE3)) {
      admin.truncateTable(NAMESPACE1, table);
    }
    admin.truncateTable(NAMESPACE2, TABLE1);
  }

  @AfterAll
  public void afterAll() throws ExecutionException {
    multiStorage.close();
    cleanUp(cassandra, cassandraAdmin);
    cleanUp(jdbcDatabase, jdbcAdmin);
  }

  private void cleanUp(DistributedStorage storage, DistributedStorageAdmin admin)
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
  public void whenPutDataIntoTable1_DataShouldBeWrittenIntoCassandra() throws ExecutionException {
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

    result = cassandra.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol2Value(result.get())).isEqualTo("val2");
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(4);
    assertThat(getCol5Value(result.get())).isTrue();

    result = jdbcDatabase.get(get);
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void whenPutDataIntoTable2_DataShouldBeWrittenIntoJdbcDatabase()
      throws ExecutionException {
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

    result = cassandra.get(get);
    assertThat(result.isPresent()).isFalse();

    result = jdbcDatabase.get(get);
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

    result = cassandra.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol2Value(result.get())).isEqualTo("val2");
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(4);
    assertThat(getCol5Value(result.get())).isTrue();

    result = jdbcDatabase.get(get);
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void whenScanDataFromTable1_DataShouldBeScannedFromCassandra()
      throws ExecutionException, IOException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE1;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey1 = new Key(COL_NAME4, 0);
    Key clusteringKey2 = new Key(COL_NAME4, 1);
    Key clusteringKey3 = new Key(COL_NAME4, 2);

    cassandra.mutate(
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
  public void whenScanDataFromTable2_DataShouldBeScannedFromJdbcDatabase()
      throws ExecutionException, IOException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE2;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey1 = new Key(COL_NAME4, 0);
    Key clusteringKey2 = new Key(COL_NAME4, 1);
    Key clusteringKey3 = new Key(COL_NAME4, 2);

    jdbcDatabase.mutate(
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

    cassandra.mutate(
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
  public void whenDeleteDataFromTable1_DataShouldBeDeletedFromCassandra()
      throws ExecutionException {
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

    cassandra.put(put);
    jdbcDatabase.put(put);

    // Act
    multiStorage.delete(
        new Delete(partitionKey, clusteringKey).forNamespace(namespace).forTable(table));

    // Assert
    Get get = new Get(partitionKey, clusteringKey).forNamespace(namespace).forTable(table);

    Optional<Result> result = multiStorage.get(get);
    assertThat(result.isPresent()).isFalse();

    result = cassandra.get(get);
    assertThat(result.isPresent()).isFalse();

    result = jdbcDatabase.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(4);
  }

  @Test
  public void whenDeleteDataFromTable2_DataShouldBeDeletedFromJdbcDatabase()
      throws ExecutionException {
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

    cassandra.put(put);
    jdbcDatabase.put(put);

    // Act
    multiStorage.delete(
        new Delete(partitionKey, clusteringKey).forNamespace(namespace).forTable(table));

    // Assert
    Get get = new Get(partitionKey, clusteringKey).forNamespace(namespace).forTable(table);

    Optional<Result> result = multiStorage.get(get);
    assertThat(result.isPresent()).isFalse();

    result = cassandra.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(4);

    result = jdbcDatabase.get(get);
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

    cassandra.put(put);
    jdbcDatabase.put(put);

    // Act
    multiStorage.delete(
        new Delete(partitionKey, clusteringKey).forNamespace(namespace).forTable(table));

    // Assert
    Get get = new Get(partitionKey, clusteringKey).forNamespace(namespace).forTable(table);

    Optional<Result> result = multiStorage.get(get);
    assertThat(result.isPresent()).isFalse();

    result = cassandra.get(get);
    assertThat(result.isPresent()).isFalse();

    result = jdbcDatabase.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(4);
  }

  @Test
  public void whenMutateDataToTable1_ShouldExecuteForCassandra() throws ExecutionException {
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

    cassandra.put(put);
    jdbcDatabase.put(put);

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

    result = cassandra.get(get1);
    assertThat(result.isPresent()).isFalse();
    result = cassandra.get(get2);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(2);

    result = jdbcDatabase.get(get1);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(1);
    result = jdbcDatabase.get(get2);
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void whenMutateDataToTable2_ShouldExecuteForJdbcDatabase() throws ExecutionException {
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

    cassandra.put(put);
    jdbcDatabase.put(put);

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

    result = cassandra.get(get1);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(1);
    result = cassandra.get(get2);
    assertThat(result.isPresent()).isFalse();

    result = jdbcDatabase.get(get1);
    assertThat(result.isPresent()).isFalse();
    result = jdbcDatabase.get(get2);
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

    cassandra.put(put);
    jdbcDatabase.put(put);

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

    result = cassandra.get(get1);
    assertThat(result.isPresent()).isFalse();
    result = cassandra.get(get2);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(2);

    result = jdbcDatabase.get(get1);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(1);
    result = jdbcDatabase.get(get2);
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void whenPutDataIntoTable1InNamespace2_DataShouldBeWrittenIntoJdbcDatabase()
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

    result = cassandra.get(get);
    assertThat(result.isPresent()).isFalse();

    result = jdbcDatabase.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol2Value(result.get())).isEqualTo("val2");
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(4);
    assertThat(getCol5Value(result.get())).isTrue();
  }

  @Test
  public void whenScanDataFromTable1InNamespace2_DataShouldBeScannedFromJdbcDatabase()
      throws ExecutionException, IOException {
    // Arrange
    String namespace = NAMESPACE2;
    String table = TABLE1;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey1 = new Key(COL_NAME4, 0);
    Key clusteringKey2 = new Key(COL_NAME4, 1);
    Key clusteringKey3 = new Key(COL_NAME4, 2);

    jdbcDatabase.mutate(
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
  public void whenDeleteDataFromTable1InNamespace2_DataShouldBeDeletedFromJdbcDatabase()
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

    cassandra.put(put);
    jdbcDatabase.put(put);

    // Act
    multiStorage.delete(
        new Delete(partitionKey, clusteringKey).forNamespace(namespace).forTable(table));

    // Assert
    Get get = new Get(partitionKey, clusteringKey).forNamespace(namespace).forTable(table);

    Optional<Result> result = multiStorage.get(get);
    assertThat(result.isPresent()).isFalse();

    result = cassandra.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(4);

    result = jdbcDatabase.get(get);
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void whenMutateDataToTable1InNamespace2_ShouldExecuteForCassandra()
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

    cassandra.put(put);
    jdbcDatabase.put(put);

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

    result = cassandra.get(get1);
    assertThat(result.isPresent()).isTrue();
    assertThat(getCol1Value(result.get())).isEqualTo(1);
    assertThat(getCol3Value(result.get())).isEqualTo(3);
    assertThat(getCol4Value(result.get())).isEqualTo(1);
    result = cassandra.get(get2);
    assertThat(result.isPresent()).isFalse();

    result = jdbcDatabase.get(get1);
    assertThat(result.isPresent()).isFalse();
    result = jdbcDatabase.get(get2);
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

  @Test
  public void operation_WhenDefaultNamespaceGiven_ShouldWorkProperly() {
    Properties properties = MultiStorageEnv.getPropertiesForCassandra(TEST_NAME);
    properties.put(DatabaseConfig.DEFAULT_NAMESPACE_NAME, NAMESPACE1);
    final DistributedStorage storageWithDefaultNamespace =
        StorageFactory.create(properties).getStorage();
    try {
      Put putRecord1 =
          Put.newBuilder()
              .table(TABLE1)
              .partitionKey(Key.ofInt(COL_NAME1, 0))
              .clusteringKey(Key.ofInt(COL_NAME4, 1))
              .build();
      Put putRecord2 =
          Put.newBuilder()
              .table(TABLE1)
              .partitionKey(Key.ofInt(COL_NAME1, 0))
              .clusteringKey(Key.ofInt(COL_NAME4, 2))
              .build();
      Get getRecord1 =
          Get.newBuilder()
              .table(TABLE1)
              .partitionKey(Key.ofInt(COL_NAME1, 0))
              .clusteringKey(Key.ofInt(COL_NAME4, 1))
              .build();
      Scan scanAllRecords = Scan.newBuilder().table(TABLE1).all().build();
      Mutation updateRecord1 =
          Put.newBuilder()
              .table(TABLE1)
              .partitionKey(Key.ofInt(COL_NAME1, 0))
              .clusteringKey(Key.ofInt(COL_NAME4, 1))
              .textValue(COL_NAME2, "foo")
              .build();
      Mutation deleteRecord2 =
          Delete.newBuilder()
              .table(TABLE1)
              .partitionKey(Key.ofInt(COL_NAME1, 0))
              .clusteringKey(Key.ofInt(COL_NAME4, 2))
              .build();
      Delete deleteRecord1 =
          Delete.newBuilder()
              .table(TABLE1)
              .partitionKey(Key.ofInt(COL_NAME1, 0))
              .clusteringKey(Key.ofInt(COL_NAME4, 1))
              .build();

      // Act Assert
      Assertions.assertThatCode(
              () -> {
                storageWithDefaultNamespace.put(putRecord1);
                storageWithDefaultNamespace.put(putRecord2);
                storageWithDefaultNamespace.get(getRecord1);
                storageWithDefaultNamespace.scan(scanAllRecords).close();
                storageWithDefaultNamespace.mutate(ImmutableList.of(updateRecord1, deleteRecord2));
                storageWithDefaultNamespace.delete(deleteRecord1);
              })
          .doesNotThrowAnyException();
    } finally {
      if (storageWithDefaultNamespace != null) {
        storageWithDefaultNamespace.close();
      }
    }
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
