package com.scalar.db.storage.multistorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.api.Delete;
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
import com.scalar.db.storage.cassandra.Cassandra;
import com.scalar.db.storage.jdbc.JdbcDatabase;
import com.scalar.db.storage.jdbc.test.TestEnv;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class MultiStorageIntegrationTest {

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

  private static Cassandra cassandra;
  private static TestEnv testEnv;
  private static JdbcDatabase mysql;
  private static MultiStorage multiStorage;

  @After
  public void tearDown() throws Exception {
    truncateCassandra();
    truncateMySql();
  }

  private void truncateCassandra() throws Exception {
    for (String table : Arrays.asList(TABLE1, TABLE2, TABLE3)) {
      truncateTable(NAMESPACE1, table);
    }
    truncateTable(NAMESPACE2, TABLE1);
  }

  private static void truncateTable(String keyspace, String table)
      throws IOException, InterruptedException {
    String truncateTableStmt = "TRUNCATE " + keyspace + "." + table;
    ProcessBuilder builder =
        new ProcessBuilder(
            "cqlsh", "-u", CASSANDRA_USERNAME, "-p", CASSANDRA_PASSWORD, "-e", truncateTableStmt);
    Process process = builder.start();
    int ret = process.waitFor();
    if (ret != 0) {
      Assert.fail("TRUNCATE TABLE failed: " + keyspace + "." + table);
    }
  }

  private void truncateMySql() throws Exception {
    testEnv.deleteTableData();
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
    assertThat(result.get().getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(result.get().getValue(COL_NAME2).get().getAsString().get()).isEqualTo("val2");
    assertThat(result.get().getValue(COL_NAME3).get().getAsInt()).isEqualTo(3);
    assertThat(result.get().getValue(COL_NAME4).get().getAsInt()).isEqualTo(4);
    assertThat(result.get().getValue(COL_NAME5).get().getAsBoolean()).isTrue();

    result = cassandra.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(result.get().getValue(COL_NAME2).get().getAsString().get()).isEqualTo("val2");
    assertThat(result.get().getValue(COL_NAME3).get().getAsInt()).isEqualTo(3);
    assertThat(result.get().getValue(COL_NAME4).get().getAsInt()).isEqualTo(4);
    assertThat(result.get().getValue(COL_NAME5).get().getAsBoolean()).isTrue();

    result = mysql.get(get);
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void whenPutDataIntoTable2_DataShouldBeWrittenIntoMySql() throws ExecutionException {
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
    assertThat(result.get().getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(result.get().getValue(COL_NAME2).get().getAsString().get()).isEqualTo("val2");
    assertThat(result.get().getValue(COL_NAME3).get().getAsInt()).isEqualTo(3);
    assertThat(result.get().getValue(COL_NAME4).get().getAsInt()).isEqualTo(4);
    assertThat(result.get().getValue(COL_NAME5).get().getAsBoolean()).isTrue();

    result = cassandra.get(get);
    assertThat(result.isPresent()).isFalse();

    result = mysql.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(result.get().getValue(COL_NAME2).get().getAsString().get()).isEqualTo("val2");
    assertThat(result.get().getValue(COL_NAME3).get().getAsInt()).isEqualTo(3);
    assertThat(result.get().getValue(COL_NAME4).get().getAsInt()).isEqualTo(4);
    assertThat(result.get().getValue(COL_NAME5).get().getAsBoolean()).isTrue();
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
    assertThat(result.get().getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(result.get().getValue(COL_NAME2).get().getAsString().get()).isEqualTo("val2");
    assertThat(result.get().getValue(COL_NAME3).get().getAsInt()).isEqualTo(3);
    assertThat(result.get().getValue(COL_NAME4).get().getAsInt()).isEqualTo(4);
    assertThat(result.get().getValue(COL_NAME5).get().getAsBoolean()).isTrue();

    result = cassandra.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(result.get().getValue(COL_NAME2).get().getAsString().get()).isEqualTo("val2");
    assertThat(result.get().getValue(COL_NAME3).get().getAsInt()).isEqualTo(3);
    assertThat(result.get().getValue(COL_NAME4).get().getAsInt()).isEqualTo(4);
    assertThat(result.get().getValue(COL_NAME5).get().getAsBoolean()).isTrue();

    result = mysql.get(get);
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void whenScanDataFromTable1_DataShouldBeScannedFromCassandra() throws Exception {
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
    assertThat(results.get(0).getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(results.get(0).getValue(COL_NAME3).get().getAsInt()).isEqualTo(2);
    assertThat(results.get(0).getValue(COL_NAME4).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(1).getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(results.get(1).getValue(COL_NAME3).get().getAsInt()).isEqualTo(1);
    assertThat(results.get(1).getValue(COL_NAME4).get().getAsInt()).isEqualTo(1);
    assertThat(results.get(2).getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(results.get(2).getValue(COL_NAME3).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(2).getValue(COL_NAME4).get().getAsInt()).isEqualTo(2);
  }

  @Test
  public void whenScanDataFromTable2_DataShouldBeScannedFromMySql() throws Exception {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE2;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey1 = new Key(COL_NAME4, 0);
    Key clusteringKey2 = new Key(COL_NAME4, 1);
    Key clusteringKey3 = new Key(COL_NAME4, 2);

    mysql.mutate(
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
    assertThat(results.get(0).getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(results.get(0).getValue(COL_NAME3).get().getAsInt()).isEqualTo(2);
    assertThat(results.get(0).getValue(COL_NAME4).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(1).getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(results.get(1).getValue(COL_NAME3).get().getAsInt()).isEqualTo(1);
    assertThat(results.get(1).getValue(COL_NAME4).get().getAsInt()).isEqualTo(1);
    assertThat(results.get(2).getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(results.get(2).getValue(COL_NAME3).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(2).getValue(COL_NAME4).get().getAsInt()).isEqualTo(2);
  }

  @Test
  public void whenScanDataFromTable3_DataShouldBeScannedFromDefaultStorage() throws Exception {
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
    assertThat(results.get(0).getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(results.get(0).getValue(COL_NAME3).get().getAsInt()).isEqualTo(2);
    assertThat(results.get(0).getValue(COL_NAME4).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(1).getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(results.get(1).getValue(COL_NAME3).get().getAsInt()).isEqualTo(1);
    assertThat(results.get(1).getValue(COL_NAME4).get().getAsInt()).isEqualTo(1);
    assertThat(results.get(2).getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(results.get(2).getValue(COL_NAME3).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(2).getValue(COL_NAME4).get().getAsInt()).isEqualTo(2);
  }

  private List<Result> scanAll(Scan scan) throws Exception {
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
    mysql.put(put);

    // Act
    multiStorage.delete(
        new Delete(partitionKey, clusteringKey).forNamespace(namespace).forTable(table));

    // Assert
    Get get = new Get(partitionKey, clusteringKey).forNamespace(namespace).forTable(table);

    Optional<Result> result = multiStorage.get(get);
    assertThat(result.isPresent()).isFalse();

    result = cassandra.get(get);
    assertThat(result.isPresent()).isFalse();

    result = mysql.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(result.get().getValue(COL_NAME3).get().getAsInt()).isEqualTo(3);
    assertThat(result.get().getValue(COL_NAME4).get().getAsInt()).isEqualTo(4);
  }

  @Test
  public void whenDeleteDataFromTable2_DataShouldBeDeletedFromMySql() throws ExecutionException {
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
    mysql.put(put);

    // Act
    multiStorage.delete(
        new Delete(partitionKey, clusteringKey).forNamespace(namespace).forTable(table));

    // Assert
    Get get = new Get(partitionKey, clusteringKey).forNamespace(namespace).forTable(table);

    Optional<Result> result = multiStorage.get(get);
    assertThat(result.isPresent()).isFalse();

    result = cassandra.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(result.get().getValue(COL_NAME3).get().getAsInt()).isEqualTo(3);
    assertThat(result.get().getValue(COL_NAME4).get().getAsInt()).isEqualTo(4);

    result = mysql.get(get);
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
    mysql.put(put);

    // Act
    multiStorage.delete(
        new Delete(partitionKey, clusteringKey).forNamespace(namespace).forTable(table));

    // Assert
    Get get = new Get(partitionKey, clusteringKey).forNamespace(namespace).forTable(table);

    Optional<Result> result = multiStorage.get(get);
    assertThat(result.isPresent()).isFalse();

    result = cassandra.get(get);
    assertThat(result.isPresent()).isFalse();

    result = mysql.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(result.get().getValue(COL_NAME3).get().getAsInt()).isEqualTo(3);
    assertThat(result.get().getValue(COL_NAME4).get().getAsInt()).isEqualTo(4);
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
    mysql.put(put);

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
    assertThat(result.get().getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(result.get().getValue(COL_NAME3).get().getAsInt()).isEqualTo(3);
    assertThat(result.get().getValue(COL_NAME4).get().getAsInt()).isEqualTo(2);

    result = cassandra.get(get1);
    assertThat(result.isPresent()).isFalse();
    result = cassandra.get(get2);
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(result.get().getValue(COL_NAME3).get().getAsInt()).isEqualTo(3);
    assertThat(result.get().getValue(COL_NAME4).get().getAsInt()).isEqualTo(2);

    result = mysql.get(get1);
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(result.get().getValue(COL_NAME3).get().getAsInt()).isEqualTo(3);
    assertThat(result.get().getValue(COL_NAME4).get().getAsInt()).isEqualTo(1);
    result = mysql.get(get2);
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void whenMutateDataToTable2_ShouldExecuteForMySql() throws ExecutionException {
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
    mysql.put(put);

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
    assertThat(result.get().getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(result.get().getValue(COL_NAME3).get().getAsInt()).isEqualTo(3);
    assertThat(result.get().getValue(COL_NAME4).get().getAsInt()).isEqualTo(2);

    result = cassandra.get(get1);
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(result.get().getValue(COL_NAME3).get().getAsInt()).isEqualTo(3);
    assertThat(result.get().getValue(COL_NAME4).get().getAsInt()).isEqualTo(1);
    result = cassandra.get(get2);
    assertThat(result.isPresent()).isFalse();

    result = mysql.get(get1);
    assertThat(result.isPresent()).isFalse();
    result = mysql.get(get2);
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(result.get().getValue(COL_NAME3).get().getAsInt()).isEqualTo(3);
    assertThat(result.get().getValue(COL_NAME4).get().getAsInt()).isEqualTo(2);
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
    mysql.put(put);

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
    assertThat(result.get().getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(result.get().getValue(COL_NAME3).get().getAsInt()).isEqualTo(3);
    assertThat(result.get().getValue(COL_NAME4).get().getAsInt()).isEqualTo(2);

    result = cassandra.get(get1);
    assertThat(result.isPresent()).isFalse();
    result = cassandra.get(get2);
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(result.get().getValue(COL_NAME3).get().getAsInt()).isEqualTo(3);
    assertThat(result.get().getValue(COL_NAME4).get().getAsInt()).isEqualTo(2);

    result = mysql.get(get1);
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(result.get().getValue(COL_NAME3).get().getAsInt()).isEqualTo(3);
    assertThat(result.get().getValue(COL_NAME4).get().getAsInt()).isEqualTo(1);
    result = mysql.get(get2);
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void whenPutDataIntoTable1InNamespace2_DataShouldBeWrittenIntoMySql()
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
    assertThat(result.get().getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(result.get().getValue(COL_NAME2).get().getAsString().get()).isEqualTo("val2");
    assertThat(result.get().getValue(COL_NAME3).get().getAsInt()).isEqualTo(3);
    assertThat(result.get().getValue(COL_NAME4).get().getAsInt()).isEqualTo(4);
    assertThat(result.get().getValue(COL_NAME5).get().getAsBoolean()).isTrue();

    result = cassandra.get(get);
    assertThat(result.isPresent()).isFalse();

    result = mysql.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(result.get().getValue(COL_NAME2).get().getAsString().get()).isEqualTo("val2");
    assertThat(result.get().getValue(COL_NAME3).get().getAsInt()).isEqualTo(3);
    assertThat(result.get().getValue(COL_NAME4).get().getAsInt()).isEqualTo(4);
    assertThat(result.get().getValue(COL_NAME5).get().getAsBoolean()).isTrue();
  }

  @Test
  public void whenScanDataFromTable1InNamespace2_DataShouldBeScannedFromMySql() throws Exception {
    // Arrange
    String namespace = NAMESPACE2;
    String table = TABLE1;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey1 = new Key(COL_NAME4, 0);
    Key clusteringKey2 = new Key(COL_NAME4, 1);
    Key clusteringKey3 = new Key(COL_NAME4, 2);

    mysql.mutate(
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
    assertThat(results.get(0).getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(results.get(0).getValue(COL_NAME3).get().getAsInt()).isEqualTo(2);
    assertThat(results.get(0).getValue(COL_NAME4).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(1).getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(results.get(1).getValue(COL_NAME3).get().getAsInt()).isEqualTo(1);
    assertThat(results.get(1).getValue(COL_NAME4).get().getAsInt()).isEqualTo(1);
    assertThat(results.get(2).getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(results.get(2).getValue(COL_NAME3).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(2).getValue(COL_NAME4).get().getAsInt()).isEqualTo(2);
  }

  @Test
  public void whenDeleteDataFromTable1InNamespace2_DataShouldBeDeletedFromMySql()
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
    mysql.put(put);

    // Act
    multiStorage.delete(
        new Delete(partitionKey, clusteringKey).forNamespace(namespace).forTable(table));

    // Assert
    Get get = new Get(partitionKey, clusteringKey).forNamespace(namespace).forTable(table);

    Optional<Result> result = multiStorage.get(get);
    assertThat(result.isPresent()).isFalse();

    result = cassandra.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(result.get().getValue(COL_NAME3).get().getAsInt()).isEqualTo(3);
    assertThat(result.get().getValue(COL_NAME4).get().getAsInt()).isEqualTo(4);

    result = mysql.get(get);
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
    mysql.put(put);

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
    assertThat(result.get().getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(result.get().getValue(COL_NAME3).get().getAsInt()).isEqualTo(3);
    assertThat(result.get().getValue(COL_NAME4).get().getAsInt()).isEqualTo(2);

    result = cassandra.get(get1);
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(result.get().getValue(COL_NAME3).get().getAsInt()).isEqualTo(3);
    assertThat(result.get().getValue(COL_NAME4).get().getAsInt()).isEqualTo(1);
    result = cassandra.get(get2);
    assertThat(result.isPresent()).isFalse();

    result = mysql.get(get1);
    assertThat(result.isPresent()).isFalse();
    result = mysql.get(get2);
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getValue(COL_NAME1).get().getAsInt()).isEqualTo(1);
    assertThat(result.get().getValue(COL_NAME3).get().getAsInt()).isEqualTo(3);
    assertThat(result.get().getValue(COL_NAME4).get().getAsInt()).isEqualTo(2);
  }

  @Test
  public void whenCallMutateWithEmptyList_ShouldThrowIllegalArgumentException() {
    // Arrange Act Assert
    assertThatThrownBy(() -> multiStorage.mutate(Collections.emptyList()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    initCassandra();
    initMySql();
    initMultiStorage();
  }

  private static void initCassandra() throws Exception {
    createKeyspace(NAMESPACE1);
    createKeyspace(NAMESPACE2);

    for (String table : Arrays.asList(TABLE1, TABLE2, TABLE3)) {
      createTable(NAMESPACE1, table);
    }
    createTable(NAMESPACE2, TABLE1);

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, CASSANDRA_CONTACT_POINT);
    props.setProperty(DatabaseConfig.USERNAME, CASSANDRA_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, CASSANDRA_PASSWORD);
    cassandra = new Cassandra(new DatabaseConfig(props));
  }

  private static void createKeyspace(String keyspace) throws IOException, InterruptedException {
    String createKeyspaceStmt =
        "CREATE KEYSPACE "
            + keyspace
            + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }";
    ProcessBuilder builder =
        new ProcessBuilder(
            "cqlsh", "-u", CASSANDRA_USERNAME, "-p", CASSANDRA_PASSWORD, "-e", createKeyspaceStmt);
    Process process = builder.start();
    int ret = process.waitFor();
    if (ret != 0) {
      Assert.fail("CREATE KEYSPACE failed: " + keyspace);
    }
  }

  private static void createTable(String keyspace, String table)
      throws IOException, InterruptedException {
    String createTableStmt =
        "CREATE TABLE "
            + keyspace
            + "."
            + table
            + " (c1 int, c2 text, c3 int, c4 int, c5 boolean, PRIMARY KEY((c1), c4))";

    ProcessBuilder builder =
        new ProcessBuilder(
            "cqlsh", "-u", CASSANDRA_USERNAME, "-p", CASSANDRA_PASSWORD, "-e", createTableStmt);
    Process process = builder.start();
    int ret = process.waitFor();
    if (ret != 0) {
      Assert.fail("CREATE TABLE failed: " + keyspace + "." + table);
    }
  }

  private static void initMySql() throws Exception {
    testEnv = new TestEnv(MYSQL_CONTACT_POINT, MYSQL_USERNAME, MYSQL_PASSWORD, Optional.empty());

    for (String table : Arrays.asList(TABLE1, TABLE2, TABLE3)) {
      testEnv.register(
          NAMESPACE1,
          table,
          TableMetadata.newBuilder()
              .addColumn(COL_NAME1, DataType.INT)
              .addColumn(COL_NAME2, DataType.TEXT)
              .addColumn(COL_NAME3, DataType.INT)
              .addColumn(COL_NAME4, DataType.INT)
              .addColumn(COL_NAME5, DataType.BOOLEAN)
              .addPartitionKey(COL_NAME1)
              .addClusteringKey(COL_NAME4)
              .build());
    }
    testEnv.register(
        NAMESPACE2,
        TABLE1,
        TableMetadata.newBuilder()
            .addColumn(COL_NAME1, DataType.INT)
            .addColumn(COL_NAME2, DataType.TEXT)
            .addColumn(COL_NAME3, DataType.INT)
            .addColumn(COL_NAME4, DataType.INT)
            .addColumn(COL_NAME5, DataType.BOOLEAN)
            .addPartitionKey(COL_NAME1)
            .addClusteringKey(COL_NAME4)
            .build());
    testEnv.createMetadataTable();
    testEnv.createTables();
    testEnv.insertMetadata();

    mysql = new JdbcDatabase(testEnv.getJdbcConfig());
  }

  private static void initMultiStorage() {
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

    multiStorage = new MultiStorage(new MultiStorageConfig(props));
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    multiStorage.close();
    cleanUpCassandra();
    cleanUpMySql();
  }

  private static void cleanUpCassandra() throws Exception {
    cassandra.close();
    dropKeyspace(NAMESPACE1);
    dropKeyspace(NAMESPACE2);
  }

  private static void dropKeyspace(String keyspace) throws IOException, InterruptedException {
    String dropKeyspaceStmt = "DROP KEYSPACE " + keyspace;
    ProcessBuilder builder =
        new ProcessBuilder(
            "cqlsh", "-u", CASSANDRA_USERNAME, "-p", CASSANDRA_PASSWORD, "-e", dropKeyspaceStmt);
    Process process = builder.start();
    int ret = process.waitFor();
    if (ret != 0) {
      Assert.fail("DROP KEYSPACE failed: " + keyspace);
    }
  }

  private static void cleanUpMySql() throws Exception {
    mysql.close();
    testEnv.dropMetadataTable();
    testEnv.dropTables();
    testEnv.close();
  }
}
