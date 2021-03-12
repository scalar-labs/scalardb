package com.scalar.db.storage.multi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.Scanner;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.storage.cassandra.Cassandra;
import com.scalar.db.storage.common.metadata.DataType;
import com.scalar.db.storage.jdbc.JdbcDatabase;
import com.scalar.db.storage.jdbc.test.TestEnv;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class MultiDatabasesIntegrationTest {

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

  private static Cassandra cassandra;
  private static TestEnv testEnv;
  private static JdbcDatabase mysql;
  private static MultiDatabases multiDatabases;

  @After
  public void tearDown() throws Exception {
    truncateCassandra();
    truncateMySql();
  }

  private void truncateCassandra() throws Exception {
    ProcessBuilder builder;
    Process process;
    int ret;

    for (String table : Arrays.asList(TABLE1, TABLE2, TABLE3)) {
      String truncateTableStmt = "TRUNCATE " + NAMESPACE + "." + table;
      builder =
          new ProcessBuilder(
              "cqlsh", "-u", CASSANDRA_USERNAME, "-p", CASSANDRA_PASSWORD, "-e", truncateTableStmt);
      process = builder.start();
      ret = process.waitFor();
      if (ret != 0) {
        Assert.fail("TRUNCATE TABLE failed: " + table);
      }
    }
  }

  private void truncateMySql() throws Exception {
    testEnv.deleteTableData();
  }

  @Test
  public void whenPutDataIntoTable1_DataShouldBeWrittenIntoCassandra() throws ExecutionException {
    // Arrange
    String table = TABLE1;
    Key partitionKey = new Key(new IntValue(COL_NAME1, 1));
    Key clusteringKey = new Key(new IntValue(COL_NAME4, 4));
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValue(new TextValue(COL_NAME2, "val2"))
            .withValue(new IntValue(COL_NAME3, 3))
            .withValue(new BooleanValue(COL_NAME5, true))
            .forNamespace(NAMESPACE)
            .forTable(table);

    // Act
    multiDatabases.put(put);

    // Assert
    Get get = new Get(partitionKey, clusteringKey).forNamespace(NAMESPACE).forTable(table);

    Optional<Result> result = multiDatabases.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(((IntValue) result.get().getValue(COL_NAME1).get()).get()).isEqualTo(1);
    assertThat(((TextValue) result.get().getValue(COL_NAME2).get()).getString().get())
        .isEqualTo("val2");
    assertThat(((IntValue) result.get().getValue(COL_NAME3).get()).get()).isEqualTo(3);
    assertThat(((IntValue) result.get().getValue(COL_NAME4).get()).get()).isEqualTo(4);
    assertThat(((BooleanValue) result.get().getValue(COL_NAME5).get()).get()).isTrue();

    result = cassandra.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(((IntValue) result.get().getValue(COL_NAME1).get()).get()).isEqualTo(1);
    assertThat(((TextValue) result.get().getValue(COL_NAME2).get()).getString().get())
        .isEqualTo("val2");
    assertThat(((IntValue) result.get().getValue(COL_NAME3).get()).get()).isEqualTo(3);
    assertThat(((IntValue) result.get().getValue(COL_NAME4).get()).get()).isEqualTo(4);
    assertThat(((BooleanValue) result.get().getValue(COL_NAME5).get()).get()).isTrue();

    result = mysql.get(get);
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void whenPutDataIntoTable2_DataShouldBeWrittenIntoMySql() throws ExecutionException {
    // Arrange
    String table = TABLE2;
    Key partitionKey = new Key(new IntValue(COL_NAME1, 1));
    Key clusteringKey = new Key(new IntValue(COL_NAME4, 4));
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValue(new TextValue(COL_NAME2, "val2"))
            .withValue(new IntValue(COL_NAME3, 3))
            .withValue(new BooleanValue(COL_NAME5, true))
            .forNamespace(NAMESPACE)
            .forTable(table);

    // Act
    multiDatabases.put(put);

    // Assert
    Get get = new Get(partitionKey, clusteringKey).forNamespace(NAMESPACE).forTable(table);

    Optional<Result> result = multiDatabases.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(((IntValue) result.get().getValue(COL_NAME1).get()).get()).isEqualTo(1);
    assertThat(((TextValue) result.get().getValue(COL_NAME2).get()).getString().get())
        .isEqualTo("val2");
    assertThat(((IntValue) result.get().getValue(COL_NAME3).get()).get()).isEqualTo(3);
    assertThat(((IntValue) result.get().getValue(COL_NAME4).get()).get()).isEqualTo(4);
    assertThat(((BooleanValue) result.get().getValue(COL_NAME5).get()).get()).isTrue();

    result = cassandra.get(get);
    assertThat(result.isPresent()).isFalse();

    result = mysql.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(((IntValue) result.get().getValue(COL_NAME1).get()).get()).isEqualTo(1);
    assertThat(((TextValue) result.get().getValue(COL_NAME2).get()).getString().get())
        .isEqualTo("val2");
    assertThat(((IntValue) result.get().getValue(COL_NAME3).get()).get()).isEqualTo(3);
    assertThat(((IntValue) result.get().getValue(COL_NAME4).get()).get()).isEqualTo(4);
    assertThat(((BooleanValue) result.get().getValue(COL_NAME5).get()).get()).isTrue();
  }

  @Test
  public void whenPutDataIntoTable2_DataShouldBeWrittenIntoDefault() throws ExecutionException {
    // Arrange
    String table = TABLE3;
    Key partitionKey = new Key(new IntValue(COL_NAME1, 1));
    Key clusteringKey = new Key(new IntValue(COL_NAME4, 4));

    Put put =
        new Put(partitionKey, clusteringKey)
            .withValue(new TextValue(COL_NAME2, "val2"))
            .withValue(new IntValue(COL_NAME3, 3))
            .withValue(new BooleanValue(COL_NAME5, true))
            .forNamespace(NAMESPACE)
            .forTable(table);

    // Act
    multiDatabases.put(put);

    // Assert
    Get get = new Get(partitionKey, clusteringKey).forNamespace(NAMESPACE).forTable(table);

    Optional<Result> result = multiDatabases.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(((IntValue) result.get().getValue(COL_NAME1).get()).get()).isEqualTo(1);
    assertThat(((TextValue) result.get().getValue(COL_NAME2).get()).getString().get())
        .isEqualTo("val2");
    assertThat(((IntValue) result.get().getValue(COL_NAME3).get()).get()).isEqualTo(3);
    assertThat(((IntValue) result.get().getValue(COL_NAME4).get()).get()).isEqualTo(4);
    assertThat(((BooleanValue) result.get().getValue(COL_NAME5).get()).get()).isTrue();

    result = cassandra.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(((IntValue) result.get().getValue(COL_NAME1).get()).get()).isEqualTo(1);
    assertThat(((TextValue) result.get().getValue(COL_NAME2).get()).getString().get())
        .isEqualTo("val2");
    assertThat(((IntValue) result.get().getValue(COL_NAME3).get()).get()).isEqualTo(3);
    assertThat(((IntValue) result.get().getValue(COL_NAME4).get()).get()).isEqualTo(4);
    assertThat(((BooleanValue) result.get().getValue(COL_NAME5).get()).get()).isTrue();

    result = mysql.get(get);
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void whenScanDataFromTable1_DataShouldBeScannedFromCassandra() throws Exception {
    // Arrange
    String table = TABLE1;
    Key partitionKey = new Key(new IntValue(COL_NAME1, 1));
    Key clusteringKey1 = new Key(new IntValue(COL_NAME4, 0));
    Key clusteringKey2 = new Key(new IntValue(COL_NAME4, 1));
    Key clusteringKey3 = new Key(new IntValue(COL_NAME4, 2));

    cassandra.mutate(
        Arrays.asList(
            new Put(partitionKey, clusteringKey1)
                .withValue(new IntValue(COL_NAME3, 2))
                .forNamespace(NAMESPACE)
                .forTable(table),
            new Put(partitionKey, clusteringKey2)
                .withValue(new IntValue(COL_NAME3, 1))
                .forNamespace(NAMESPACE)
                .forTable(table),
            new Put(partitionKey, clusteringKey3)
                .withValue(new IntValue(COL_NAME3, 0))
                .forNamespace(NAMESPACE)
                .forTable(table)));

    // Act
    List<Result> results = scanAll(new Scan(partitionKey).forNamespace(NAMESPACE).forTable(table));

    // Assert
    assertThat(results.size()).isEqualTo(3);
    assertThat(((IntValue) results.get(0).getValue(COL_NAME1).get()).get()).isEqualTo(1);
    assertThat(((IntValue) results.get(0).getValue(COL_NAME3).get()).get()).isEqualTo(2);
    assertThat(((IntValue) results.get(0).getValue(COL_NAME4).get()).get()).isEqualTo(0);
    assertThat(((IntValue) results.get(1).getValue(COL_NAME1).get()).get()).isEqualTo(1);
    assertThat(((IntValue) results.get(1).getValue(COL_NAME3).get()).get()).isEqualTo(1);
    assertThat(((IntValue) results.get(1).getValue(COL_NAME4).get()).get()).isEqualTo(1);
    assertThat(((IntValue) results.get(2).getValue(COL_NAME1).get()).get()).isEqualTo(1);
    assertThat(((IntValue) results.get(2).getValue(COL_NAME3).get()).get()).isEqualTo(0);
    assertThat(((IntValue) results.get(2).getValue(COL_NAME4).get()).get()).isEqualTo(2);
  }

  @Test
  public void whenScanDataFromTable2_DataShouldBeScannedFromMySql() throws Exception {
    // Arrange
    String table = TABLE2;
    Key partitionKey = new Key(new IntValue(COL_NAME1, 1));
    Key clusteringKey1 = new Key(new IntValue(COL_NAME4, 0));
    Key clusteringKey2 = new Key(new IntValue(COL_NAME4, 1));
    Key clusteringKey3 = new Key(new IntValue(COL_NAME4, 2));

    mysql.mutate(
        Arrays.asList(
            new Put(partitionKey, clusteringKey1)
                .withValue(new IntValue(COL_NAME3, 2))
                .forNamespace(NAMESPACE)
                .forTable(table),
            new Put(partitionKey, clusteringKey2)
                .withValue(new IntValue(COL_NAME3, 1))
                .forNamespace(NAMESPACE)
                .forTable(table),
            new Put(partitionKey, clusteringKey3)
                .withValue(new IntValue(COL_NAME3, 0))
                .forNamespace(NAMESPACE)
                .forTable(table)));

    // Act
    List<Result> results = scanAll(new Scan(partitionKey).forNamespace(NAMESPACE).forTable(table));

    // Assert
    assertThat(results.size()).isEqualTo(3);
    assertThat(((IntValue) results.get(0).getValue(COL_NAME1).get()).get()).isEqualTo(1);
    assertThat(((IntValue) results.get(0).getValue(COL_NAME3).get()).get()).isEqualTo(2);
    assertThat(((IntValue) results.get(0).getValue(COL_NAME4).get()).get()).isEqualTo(0);
    assertThat(((IntValue) results.get(1).getValue(COL_NAME1).get()).get()).isEqualTo(1);
    assertThat(((IntValue) results.get(1).getValue(COL_NAME3).get()).get()).isEqualTo(1);
    assertThat(((IntValue) results.get(1).getValue(COL_NAME4).get()).get()).isEqualTo(1);
    assertThat(((IntValue) results.get(2).getValue(COL_NAME1).get()).get()).isEqualTo(1);
    assertThat(((IntValue) results.get(2).getValue(COL_NAME3).get()).get()).isEqualTo(0);
    assertThat(((IntValue) results.get(2).getValue(COL_NAME4).get()).get()).isEqualTo(2);
  }

  @Test
  public void whenScanDataFromTable3_DataShouldBeScannedFromDefault() throws Exception {
    // Arrange
    String table = TABLE3;
    Key partitionKey = new Key(new IntValue(COL_NAME1, 1));
    Key clusteringKey1 = new Key(new IntValue(COL_NAME4, 0));
    Key clusteringKey2 = new Key(new IntValue(COL_NAME4, 1));
    Key clusteringKey3 = new Key(new IntValue(COL_NAME4, 2));

    cassandra.mutate(
        Arrays.asList(
            new Put(partitionKey, clusteringKey1)
                .withValue(new IntValue(COL_NAME3, 2))
                .forNamespace(NAMESPACE)
                .forTable(table),
            new Put(partitionKey, clusteringKey2)
                .withValue(new IntValue(COL_NAME3, 1))
                .forNamespace(NAMESPACE)
                .forTable(table),
            new Put(partitionKey, clusteringKey3)
                .withValue(new IntValue(COL_NAME3, 0))
                .forNamespace(NAMESPACE)
                .forTable(table)));

    // Act
    List<Result> results = scanAll(new Scan(partitionKey).forNamespace(NAMESPACE).forTable(table));

    // Assert
    assertThat(results.size()).isEqualTo(3);
    assertThat(((IntValue) results.get(0).getValue(COL_NAME1).get()).get()).isEqualTo(1);
    assertThat(((IntValue) results.get(0).getValue(COL_NAME3).get()).get()).isEqualTo(2);
    assertThat(((IntValue) results.get(0).getValue(COL_NAME4).get()).get()).isEqualTo(0);
    assertThat(((IntValue) results.get(1).getValue(COL_NAME1).get()).get()).isEqualTo(1);
    assertThat(((IntValue) results.get(1).getValue(COL_NAME3).get()).get()).isEqualTo(1);
    assertThat(((IntValue) results.get(1).getValue(COL_NAME4).get()).get()).isEqualTo(1);
    assertThat(((IntValue) results.get(2).getValue(COL_NAME1).get()).get()).isEqualTo(1);
    assertThat(((IntValue) results.get(2).getValue(COL_NAME3).get()).get()).isEqualTo(0);
    assertThat(((IntValue) results.get(2).getValue(COL_NAME4).get()).get()).isEqualTo(2);
  }

  private List<Result> scanAll(Scan scan) throws Exception {
    try (Scanner scanner = multiDatabases.scan(scan)) {
      return scanner.all();
    }
  }

  @Test
  public void whenDeleteDataFromTable1_DataShouldBeDeletedFromCassandra()
      throws ExecutionException {
    // Arrange
    String table = TABLE1;
    Key partitionKey = new Key(new IntValue(COL_NAME1, 1));
    Key clusteringKey = new Key(new IntValue(COL_NAME4, 4));
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValue(new IntValue(COL_NAME3, 3))
            .forNamespace(NAMESPACE)
            .forTable(table);

    cassandra.put(put);
    mysql.put(put);

    // Act
    multiDatabases.delete(
        new Delete(partitionKey, clusteringKey).forNamespace(NAMESPACE).forTable(table));

    // Assert
    Get get = new Get(partitionKey, clusteringKey).forNamespace(NAMESPACE).forTable(table);

    Optional<Result> result = multiDatabases.get(get);
    assertThat(result.isPresent()).isFalse();

    result = cassandra.get(get);
    assertThat(result.isPresent()).isFalse();

    result = mysql.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(((IntValue) result.get().getValue(COL_NAME1).get()).get()).isEqualTo(1);
    assertThat(((IntValue) result.get().getValue(COL_NAME3).get()).get()).isEqualTo(3);
    assertThat(((IntValue) result.get().getValue(COL_NAME4).get()).get()).isEqualTo(4);
  }

  @Test
  public void whenDeleteDataFromTable2_DataShouldBeDeletedFromMySql() throws ExecutionException {
    // Arrange
    String table = TABLE2;
    Key partitionKey = new Key(new IntValue(COL_NAME1, 1));
    Key clusteringKey = new Key(new IntValue(COL_NAME4, 4));
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValue(new IntValue(COL_NAME3, 3))
            .forNamespace(NAMESPACE)
            .forTable(table);

    cassandra.put(put);
    mysql.put(put);

    // Act
    multiDatabases.delete(
        new Delete(partitionKey, clusteringKey).forNamespace(NAMESPACE).forTable(table));

    // Assert
    Get get = new Get(partitionKey, clusteringKey).forNamespace(NAMESPACE).forTable(table);

    Optional<Result> result = multiDatabases.get(get);
    assertThat(result.isPresent()).isFalse();

    result = cassandra.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(((IntValue) result.get().getValue(COL_NAME1).get()).get()).isEqualTo(1);
    assertThat(((IntValue) result.get().getValue(COL_NAME3).get()).get()).isEqualTo(3);
    assertThat(((IntValue) result.get().getValue(COL_NAME4).get()).get()).isEqualTo(4);

    result = mysql.get(get);
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void whenDeleteDataFromTable3_DataShouldBeDeletedFromDefault() throws ExecutionException {
    // Arrange
    String table = TABLE3;
    Key partitionKey = new Key(new IntValue(COL_NAME1, 1));
    Key clusteringKey = new Key(new IntValue(COL_NAME4, 4));
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValue(new IntValue(COL_NAME3, 3))
            .forNamespace(NAMESPACE)
            .forTable(table);

    cassandra.put(put);
    mysql.put(put);

    // Act
    multiDatabases.delete(
        new Delete(partitionKey, clusteringKey).forNamespace(NAMESPACE).forTable(table));

    // Assert
    Get get = new Get(partitionKey, clusteringKey).forNamespace(NAMESPACE).forTable(table);

    Optional<Result> result = multiDatabases.get(get);
    assertThat(result.isPresent()).isFalse();

    result = cassandra.get(get);
    assertThat(result.isPresent()).isFalse();

    result = mysql.get(get);
    assertThat(result.isPresent()).isTrue();
    assertThat(((IntValue) result.get().getValue(COL_NAME1).get()).get()).isEqualTo(1);
    assertThat(((IntValue) result.get().getValue(COL_NAME3).get()).get()).isEqualTo(3);
    assertThat(((IntValue) result.get().getValue(COL_NAME4).get()).get()).isEqualTo(4);
  }

  @Test
  public void whenMutateDataToTable1_ShouldExecuteForCassandra() throws ExecutionException {
    // Arrange
    String table = TABLE1;
    Key partitionKey = new Key(new IntValue(COL_NAME1, 1));
    Key clusteringKey1 = new Key(new IntValue(COL_NAME4, 1));
    Key clusteringKey2 = new Key(new IntValue(COL_NAME4, 2));
    Put put =
        new Put(partitionKey, clusteringKey1)
            .withValue(new IntValue(COL_NAME3, 3))
            .forNamespace(NAMESPACE)
            .forTable(table);

    cassandra.put(put);
    mysql.put(put);

    // Act
    multiDatabases.mutate(
        Arrays.asList(
            new Put(partitionKey, clusteringKey2)
                .withValue(new IntValue(COL_NAME3, 3))
                .forNamespace(NAMESPACE)
                .forTable(table),
            new Delete(partitionKey, clusteringKey1).forNamespace(NAMESPACE).forTable(table)));

    // Assert
    Get get1 = new Get(partitionKey, clusteringKey1).forNamespace(NAMESPACE).forTable(table);
    Get get2 = new Get(partitionKey, clusteringKey2).forNamespace(NAMESPACE).forTable(table);

    Optional<Result> result = multiDatabases.get(get1);
    assertThat(result.isPresent()).isFalse();
    result = multiDatabases.get(get2);
    assertThat(result.isPresent()).isTrue();
    assertThat(((IntValue) result.get().getValue(COL_NAME1).get()).get()).isEqualTo(1);
    assertThat(((IntValue) result.get().getValue(COL_NAME3).get()).get()).isEqualTo(3);
    assertThat(((IntValue) result.get().getValue(COL_NAME4).get()).get()).isEqualTo(2);

    result = cassandra.get(get1);
    assertThat(result.isPresent()).isFalse();
    result = cassandra.get(get2);
    assertThat(result.isPresent()).isTrue();
    assertThat(((IntValue) result.get().getValue(COL_NAME1).get()).get()).isEqualTo(1);
    assertThat(((IntValue) result.get().getValue(COL_NAME3).get()).get()).isEqualTo(3);
    assertThat(((IntValue) result.get().getValue(COL_NAME4).get()).get()).isEqualTo(2);

    result = mysql.get(get1);
    assertThat(result.isPresent()).isTrue();
    assertThat(((IntValue) result.get().getValue(COL_NAME1).get()).get()).isEqualTo(1);
    assertThat(((IntValue) result.get().getValue(COL_NAME3).get()).get()).isEqualTo(3);
    assertThat(((IntValue) result.get().getValue(COL_NAME4).get()).get()).isEqualTo(1);
    result = mysql.get(get2);
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void whenMutateDataToTable2_ShouldExecuteForMySql() throws ExecutionException {
    // Arrange
    String table = TABLE2;
    Key partitionKey = new Key(new IntValue(COL_NAME1, 1));
    Key clusteringKey1 = new Key(new IntValue(COL_NAME4, 1));
    Key clusteringKey2 = new Key(new IntValue(COL_NAME4, 2));
    Put put =
        new Put(partitionKey, clusteringKey1)
            .withValue(new IntValue(COL_NAME3, 3))
            .forNamespace(NAMESPACE)
            .forTable(table);

    cassandra.put(put);
    mysql.put(put);

    // Act
    multiDatabases.mutate(
        Arrays.asList(
            new Put(partitionKey, clusteringKey2)
                .withValue(new IntValue(COL_NAME3, 3))
                .forNamespace(NAMESPACE)
                .forTable(table),
            new Delete(partitionKey, clusteringKey1).forNamespace(NAMESPACE).forTable(table)));

    // Assert
    Get get1 = new Get(partitionKey, clusteringKey1).forNamespace(NAMESPACE).forTable(table);
    Get get2 = new Get(partitionKey, clusteringKey2).forNamespace(NAMESPACE).forTable(table);

    Optional<Result> result = multiDatabases.get(get1);
    assertThat(result.isPresent()).isFalse();
    result = multiDatabases.get(get2);
    assertThat(result.isPresent()).isTrue();
    assertThat(((IntValue) result.get().getValue(COL_NAME1).get()).get()).isEqualTo(1);
    assertThat(((IntValue) result.get().getValue(COL_NAME3).get()).get()).isEqualTo(3);
    assertThat(((IntValue) result.get().getValue(COL_NAME4).get()).get()).isEqualTo(2);

    result = cassandra.get(get1);
    assertThat(result.isPresent()).isTrue();
    assertThat(((IntValue) result.get().getValue(COL_NAME1).get()).get()).isEqualTo(1);
    assertThat(((IntValue) result.get().getValue(COL_NAME3).get()).get()).isEqualTo(3);
    assertThat(((IntValue) result.get().getValue(COL_NAME4).get()).get()).isEqualTo(1);
    result = cassandra.get(get2);
    assertThat(result.isPresent()).isFalse();

    result = mysql.get(get1);
    assertThat(result.isPresent()).isFalse();
    result = mysql.get(get2);
    assertThat(result.isPresent()).isTrue();
    assertThat(((IntValue) result.get().getValue(COL_NAME1).get()).get()).isEqualTo(1);
    assertThat(((IntValue) result.get().getValue(COL_NAME3).get()).get()).isEqualTo(3);
    assertThat(((IntValue) result.get().getValue(COL_NAME4).get()).get()).isEqualTo(2);
  }

  @Test
  public void whenMutateDataToTable3_ShouldExecuteForDefault() throws ExecutionException {
    // Arrange
    String table = TABLE3;
    Key partitionKey = new Key(new IntValue(COL_NAME1, 1));
    Key clusteringKey1 = new Key(new IntValue(COL_NAME4, 1));
    Key clusteringKey2 = new Key(new IntValue(COL_NAME4, 2));
    Put put =
        new Put(partitionKey, clusteringKey1)
            .withValue(new IntValue(COL_NAME3, 3))
            .forNamespace(NAMESPACE)
            .forTable(table);

    cassandra.put(put);
    mysql.put(put);

    // Act
    multiDatabases.mutate(
        Arrays.asList(
            new Put(partitionKey, clusteringKey2)
                .withValue(new IntValue(COL_NAME3, 3))
                .forNamespace(NAMESPACE)
                .forTable(table),
            new Delete(partitionKey, clusteringKey1).forNamespace(NAMESPACE).forTable(table)));

    // Assert
    Get get1 = new Get(partitionKey, clusteringKey1).forNamespace(NAMESPACE).forTable(table);
    Get get2 = new Get(partitionKey, clusteringKey2).forNamespace(NAMESPACE).forTable(table);

    Optional<Result> result = multiDatabases.get(get1);
    assertThat(result.isPresent()).isFalse();
    result = multiDatabases.get(get2);
    assertThat(result.isPresent()).isTrue();
    assertThat(((IntValue) result.get().getValue(COL_NAME1).get()).get()).isEqualTo(1);
    assertThat(((IntValue) result.get().getValue(COL_NAME3).get()).get()).isEqualTo(3);
    assertThat(((IntValue) result.get().getValue(COL_NAME4).get()).get()).isEqualTo(2);

    result = cassandra.get(get1);
    assertThat(result.isPresent()).isFalse();
    result = cassandra.get(get2);
    assertThat(result.isPresent()).isTrue();
    assertThat(((IntValue) result.get().getValue(COL_NAME1).get()).get()).isEqualTo(1);
    assertThat(((IntValue) result.get().getValue(COL_NAME3).get()).get()).isEqualTo(3);
    assertThat(((IntValue) result.get().getValue(COL_NAME4).get()).get()).isEqualTo(2);

    result = mysql.get(get1);
    assertThat(result.isPresent()).isTrue();
    assertThat(((IntValue) result.get().getValue(COL_NAME1).get()).get()).isEqualTo(1);
    assertThat(((IntValue) result.get().getValue(COL_NAME3).get()).get()).isEqualTo(3);
    assertThat(((IntValue) result.get().getValue(COL_NAME4).get()).get()).isEqualTo(1);
    result = mysql.get(get2);
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void whenCallMutateWithEmptyList_ShouldThrowIllegalArgumentException() {
    // Arrange Act Assert
    assertThatThrownBy(() -> multiDatabases.mutate(Collections.emptyList()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    initCassandra();
    initMySql();
    initMultiDatabases();
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

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, CASSANDRA_CONTACT_POINT);
    props.setProperty(DatabaseConfig.USERNAME, CASSANDRA_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, CASSANDRA_PASSWORD);
    cassandra = new Cassandra(new DatabaseConfig(props));
  }

  private static void initMySql() throws Exception {
    testEnv = new TestEnv(MYSQL_CONTACT_POINT, MYSQL_USERNAME, MYSQL_PASSWORD, Optional.empty());

    for (String table : Arrays.asList(TABLE1, TABLE2, TABLE3)) {
      testEnv.register(
          NAMESPACE,
          table,
          Collections.singletonList(COL_NAME1),
          Collections.singletonList(COL_NAME4),
          new HashMap<String, Order>() {
            {
              put(COL_NAME4, Scan.Ordering.Order.ASC);
            }
          },
          new HashMap<String, DataType>() {
            {
              put(COL_NAME1, DataType.INT);
              put(COL_NAME2, DataType.TEXT);
              put(COL_NAME3, DataType.INT);
              put(COL_NAME4, DataType.INT);
              put(COL_NAME5, DataType.BOOLEAN);
            }
          });
    }
    testEnv.createMetadataTable();
    testEnv.createTables();
    testEnv.insertMetadata();

    mysql = new JdbcDatabase(testEnv.getJdbcDatabaseConfig());
  }

  private static void initMultiDatabases() {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "multi");

    // Define databases, cassandra and mysql
    props.setProperty(MultiDatabasesConfig.DATABASES, "cassandra,mysql");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.storage", "cassandra");
    props.setProperty(
        MultiDatabasesConfig.DATABASES + ".cassandra.contact_points", CASSANDRA_CONTACT_POINT);
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.username", CASSANDRA_USERNAME);
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.password", CASSANDRA_PASSWORD);
    props.setProperty(MultiDatabasesConfig.DATABASES + ".mysql.storage", "jdbc");
    props.setProperty(
        MultiDatabasesConfig.DATABASES + ".mysql.contact_points", MYSQL_CONTACT_POINT);
    props.setProperty(MultiDatabasesConfig.DATABASES + ".mysql.username", MYSQL_USERNAME);
    props.setProperty(MultiDatabasesConfig.DATABASES + ".mysql.password", MYSQL_PASSWORD);

    // Define table mapping from table1 to cassandra, and from table2 to mysql
    props.setProperty(
        MultiDatabasesConfig.TABLE_MAPPING,
        NAMESPACE + "." + TABLE1 + "," + NAMESPACE + "." + TABLE2);
    props.setProperty(
        MultiDatabasesConfig.TABLE_MAPPING + "." + NAMESPACE + "." + TABLE1, "cassandra");
    props.setProperty(MultiDatabasesConfig.TABLE_MAPPING + "." + NAMESPACE + "." + TABLE2, "mysql");

    // The default database is cassandra
    props.setProperty(MultiDatabasesConfig.DEFAULT_DATABASE, "cassandra");

    multiDatabases = new MultiDatabases(new MultiDatabasesConfig(props));
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    multiDatabases.close();
    cleanUpCassandra();
    cleanUpMySql();
  }

  private static void cleanUpCassandra() throws Exception {
    cassandra.close();

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
    mysql.close();
    testEnv.dropMetadataTable();
    testEnv.dropTables();
    testEnv.close();
  }
}
