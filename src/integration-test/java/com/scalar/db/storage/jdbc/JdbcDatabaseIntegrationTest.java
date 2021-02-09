package com.scalar.db.storage.jdbc;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.exception.storage.MultiPartitionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.storage.jdbc.metadata.DataType;
import com.scalar.db.storage.jdbc.test.JdbcConnectionInfo;
import com.scalar.db.storage.jdbc.test.TestEnv;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.scalar.db.storage.jdbc.test.TestEnv.MYSQL_INFO;
import static com.scalar.db.storage.jdbc.test.TestEnv.POSTGRESQL_INFO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(Parameterized.class)
public class JdbcDatabaseIntegrationTest {

  private static final String NAMESPACE = "integration_testing";
  private static final String TABLE = "test_table";
  private static final String COL_NAME1 = "c1";
  private static final String COL_NAME2 = "c2";
  private static final String COL_NAME3 = "c3";
  private static final String COL_NAME4 = "c4";
  private static final String COL_NAME5 = "c5";

  @Parameterized.Parameter public JdbcConnectionInfo jdbcConnectionInfo;

  @Parameterized.Parameter(1)
  public String namespacePrefix;

  private TestEnv testEnv;
  private DistributedStorage storage;
  private List<Put> puts;
  private List<Delete> deletes;

  @Parameterized.Parameters(name = "RDB={0}, namespace_prefix={1}")
  public static Collection<Object[]> jdbcConnectionInfos() {
    return Arrays.asList(
        new Object[] {MYSQL_INFO, null},
        new Object[] {MYSQL_INFO, "ns_prefix"},
        new Object[] {POSTGRESQL_INFO, null},
        new Object[] {POSTGRESQL_INFO, "ns_prefix"});
  }

  @Before
  public void setUp() throws Exception {
    testEnv = new TestEnv(jdbcConnectionInfo, Optional.ofNullable(namespacePrefix));
    testEnv.register(
        NAMESPACE,
        TABLE,
        new LinkedHashMap<String, DataType>() {
          {
            put(COL_NAME1, DataType.INT);
            put(COL_NAME2, DataType.TEXT);
            put(COL_NAME3, DataType.INT);
            put(COL_NAME4, DataType.INT);
            put(COL_NAME5, DataType.BOOLEAN);
          }
        },
        Collections.singletonList(COL_NAME1),
        Collections.singletonList(COL_NAME4),
        new HashMap<String, Scan.Ordering.Order>() {
          {
            put(COL_NAME4, Scan.Ordering.Order.ASC);
          }
        },
        new HashSet<String>() {
          {
            add(COL_NAME3);
          }
        },
        new HashMap<String, Scan.Ordering.Order>() {
          {
            put(COL_NAME3, Scan.Ordering.Order.ASC);
          }
        });
    testEnv.createTables();

    storage = new JdbcDatabase(testEnv.getJdbcDatabaseConfig());
    storage.with(NAMESPACE, TABLE);
  }

  @After
  public void tearDown() throws Exception {
    storage.close();

    testEnv.dropTables();
    testEnv.close();
  }

  @Test
  public void operation_NoTargetGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    storage.with(null, TABLE);
    Key partitionKey = new Key(new IntValue(COL_NAME1, 0));
    Key clusteringKey = new Key(new IntValue(COL_NAME4, 0));
    Get get = new Get(partitionKey, clusteringKey);

    // Act Assert
    assertThatThrownBy(
            () -> {
              storage.get(get);
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void get_GetWithPartitionKeyAndClusteringKeyGiven_ShouldRetrieveSingleResult()
      throws Exception {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Get get = prepareGet(pKey, 0);
    Optional<Result> actual = storage.get(get);

    // Assert
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get().getValue(COL_NAME4)).isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));
  }

  @Test
  public void get_GetWithoutPartitionKeyGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act Assert
    Key partitionKey = new Key(new IntValue(COL_NAME1, pKey));
    Get get = new Get(partitionKey);
    assertThatThrownBy(
            () -> {
              storage.get(get);
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void get_GetWithProjectionsGiven_ShouldRetrieveSpecifiedValues() throws Exception {
    // Arrange
    populateRecords();
    int pKey = 0;
    int cKey = 0;

    // Act
    Get get = prepareGet(pKey, cKey);
    get.withProjection(COL_NAME1).withProjection(COL_NAME2).withProjection(COL_NAME3);
    Optional<Result> actual = storage.get(get);

    // Assert
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get().getValue(COL_NAME2))
        .isEqualTo(Optional.of(new TextValue(COL_NAME2, Integer.toString(pKey + cKey))));
    assertThat(actual.get().getValue(COL_NAME3))
        .isEqualTo(Optional.of(new IntValue(COL_NAME3, pKey + cKey)));
    assertThat(actual.get().getValue(COL_NAME4).isPresent()).isTrue(); // since it's clustering key
    assertThat(actual.get().getValue(COL_NAME5).isPresent()).isFalse();
  }

  @Test
  public void scan_ScanWithPartitionKeyGiven_ShouldRetrieveMultipleResults() throws Exception {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan = new Scan(new Key(new IntValue(COL_NAME1, pKey)));
    List<Result> actual = new ArrayList<>();
    try (Scanner scanner = storage.scan(scan)) {
      scanner.forEach(actual::add); // use iterator
    }

    // Assert
    assertThat(actual.size()).isEqualTo(3);
    assertThat(actual.get(0).getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));
    assertThat(actual.get(1).getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get(1).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 1)));
    assertThat(actual.get(2).getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get(2).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 2)));
  }

  @Test
  public void scan_ScanWithPartitionKeyGivenAndResultsIteratedWithOne_ShouldReturnWhatsPut()
      throws Exception {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan = new Scan(new Key(new IntValue(COL_NAME1, pKey)));
    try (Scanner scanner = storage.scan(scan)) {
      // Assert
      Optional<Result> result = scanner.one();
      assertThat(result.isPresent()).isTrue();
      assertThat(result.get().getValue(COL_NAME1))
          .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
      assertThat(result.get().getValue(COL_NAME4))
          .isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));
      result = scanner.one();
      assertThat(result.isPresent()).isTrue();
      assertThat(result.get().getValue(COL_NAME1))
          .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
      assertThat(result.get().getValue(COL_NAME4))
          .isEqualTo(Optional.of(new IntValue(COL_NAME4, 1)));
      result = scanner.one();
      assertThat(result.isPresent()).isTrue();
      assertThat(result.get().getValue(COL_NAME1))
          .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
      assertThat(result.get().getValue(COL_NAME4))
          .isEqualTo(Optional.of(new IntValue(COL_NAME4, 2)));
      result = scanner.one();
      assertThat(result.isPresent()).isFalse();
    }
  }

  @Test
  public void scan_ScanWithPartitionGivenThreeTimes_ShouldRetrieveResultsProperlyEveryTime()
      throws Exception {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan = new Scan(new Key(new IntValue(COL_NAME1, pKey)));
    double t1 = System.currentTimeMillis();
    List<Result> actual = scanAll(scan);
    double t2 = System.currentTimeMillis();
    storage.scan(scan).close();
    double t3 = System.currentTimeMillis();
    storage.scan(scan).close();
    double t4 = System.currentTimeMillis();

    // Assert
    assertThat(actual.get(0).getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));
    System.err.println("first: " + (t2 - t1) + " (ms)");
    System.err.println("second: " + (t3 - t2) + " (ms)");
    System.err.println("third: " + (t4 - t3) + " (ms)");
  }

  @Test
  public void scan_ScanWithStartInclusiveRangeGiven_ShouldRetrieveResultsOfGivenRange()
      throws Exception {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan =
        new Scan(new Key(new IntValue(COL_NAME1, pKey)))
            .withStart(new Key(new IntValue(COL_NAME4, 0)), true)
            .withEnd(new Key(new IntValue(COL_NAME4, 2)), false);
    List<Result> actual = scanAll(scan);

    // verify
    assertThat(actual.size()).isEqualTo(2);
    assertThat(actual.get(0).getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));
    assertThat(actual.get(1).getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get(1).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 1)));
  }

  @Test
  public void scan_ScanWithEndInclusiveRangeGiven_ShouldRetrieveResultsOfGivenRange()
      throws Exception {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan =
        new Scan(new Key(new IntValue(COL_NAME1, pKey)))
            .withStart(new Key(new IntValue(COL_NAME4, 0)), false)
            .withEnd(new Key(new IntValue(COL_NAME4, 2)), true);
    List<Result> actual = scanAll(scan);

    // verify
    assertThat(actual.size()).isEqualTo(2);
    assertThat(actual.get(0).getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 1)));
    assertThat(actual.get(1).getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get(1).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 2)));
  }

  @Test
  public void scan_ScanWithOrderAscGiven_ShouldReturnAscendingOrderedResults() throws Exception {
    // Arrange
    puts = preparePuts();
    storage.mutate(Arrays.asList(puts.get(0), puts.get(1), puts.get(2)));
    Scan scan =
        new Scan(new Key(new IntValue(COL_NAME1, 0)))
            .withOrdering(new Scan.Ordering(COL_NAME4, Scan.Ordering.Order.ASC));

    // Act
    List<Result> actual = scanAll(scan);

    // Assert
    assertThat(actual.size()).isEqualTo(3);
    assertThat(actual.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));
    assertThat(actual.get(1).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 1)));
    assertThat(actual.get(2).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 2)));
  }

  @Test
  public void scan_ScanWithOrderDescGiven_ShouldReturnDescendingOrderedResults() throws Exception {
    // Arrange
    puts = preparePuts();
    storage.mutate(Arrays.asList(puts.get(0), puts.get(1), puts.get(2)));
    Scan scan =
        new Scan(new Key(new IntValue(COL_NAME1, 0)))
            .withOrdering(new Scan.Ordering(COL_NAME4, Scan.Ordering.Order.DESC));

    // Act
    List<Result> actual = scanAll(scan);

    // Assert
    assertThat(actual.size()).isEqualTo(3);
    assertThat(actual.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 2)));
    assertThat(actual.get(1).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 1)));
    assertThat(actual.get(2).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));
  }

  @Test
  public void scan_ScanWithLimitGiven_ShouldReturnGivenNumberOfResults() throws Exception {
    // setup
    puts = preparePuts();
    storage.mutate(Arrays.asList(puts.get(0), puts.get(1), puts.get(2)));

    Scan scan =
        new Scan(new Key(new IntValue(COL_NAME1, 0)))
            .withOrdering(new Scan.Ordering(COL_NAME4, Scan.Ordering.Order.DESC))
            .withLimit(1);

    // exercise
    List<Result> actual = scanAll(scan);

    // verify
    assertThat(actual.size()).isEqualTo(1);
    assertThat(actual.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 2)));
  }

  @Test
  public void put_SinglePutGiven_ShouldStoreProperly() throws Exception {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
    Key partitionKey = new Key(new IntValue(COL_NAME1, pKey));
    Key clusteringKey = new Key(new IntValue(COL_NAME4, cKey));
    Get get = new Get(partitionKey, clusteringKey);

    // Act
    storage.put(puts.get(pKey * 2 + cKey));

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get().getValue(COL_NAME2))
        .isEqualTo(Optional.of(new TextValue(COL_NAME2, Integer.toString(pKey + cKey))));
    assertThat(actual.get().getValue(COL_NAME3))
        .isEqualTo(Optional.of(new IntValue(COL_NAME3, pKey + cKey)));
    assertThat(actual.get().getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, cKey)));
    assertThat(actual.get().getValue(COL_NAME5))
        .isEqualTo(Optional.of(new BooleanValue(COL_NAME5, cKey % 2 == 0)));
  }

  @Test
  public void put_SinglePutWithIfNotExistsGiven_ShouldStoreProperly() throws Exception {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
    puts.get(0).withCondition(new PutIfNotExists());
    Key partitionKey = new Key(new IntValue(COL_NAME1, pKey));
    Key clusteringKey = new Key(new IntValue(COL_NAME4, cKey));
    Get get = new Get(partitionKey, clusteringKey);

    // Act
    storage.put(puts.get(0));
    puts.get(0).withValue(new IntValue(COL_NAME3, Integer.MAX_VALUE));
    assertThatThrownBy(
            () -> {
              storage.put(puts.get(0));
            })
        .isInstanceOf(NoMutationException.class);

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get().getValue(COL_NAME2))
        .isEqualTo(Optional.of(new TextValue(COL_NAME2, Integer.toString(pKey + cKey))));
    assertThat(actual.get().getValue(COL_NAME3))
        .isEqualTo(Optional.of(new IntValue(COL_NAME3, pKey + cKey)));
    assertThat(actual.get().getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, cKey)));
    assertThat(actual.get().getValue(COL_NAME5))
        .isEqualTo(Optional.of(new BooleanValue(COL_NAME5, cKey % 2 == 0)));
  }

  @Test
  public void put_MultiplePutGiven_ShouldStoreProperly() throws Exception {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
    Scan scan = new Scan(new Key(new IntValue(COL_NAME1, pKey)));

    // Act
    assertThatCode(
            () -> {
              storage.put(Arrays.asList(puts.get(0), puts.get(1), puts.get(2)));
            })
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(scan);
    assertThat(results.size()).isEqualTo(3);
    assertThat(results.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, pKey + cKey)));
    assertThat(results.get(1).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, pKey + cKey + 1)));
    assertThat(results.get(2).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, pKey + cKey + 2)));
  }

  @Test
  public void put_MultiplePutWithIfNotExistsGiven_ShouldStoreProperly() throws Exception {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
    puts.get(0).withCondition(new PutIfNotExists());
    puts.get(1).withCondition(new PutIfNotExists());
    puts.get(2).withCondition(new PutIfNotExists());
    Scan scan = new Scan(new Key(new IntValue(COL_NAME1, pKey)));

    // Act
    assertThatCode(
            () -> {
              storage.put(Arrays.asList(puts.get(0), puts.get(1), puts.get(2)));
            })
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(scan);
    assertThat(results.size()).isEqualTo(3);
    assertThat(results.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, pKey + cKey)));
    assertThat(results.get(1).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, pKey + cKey + 1)));
    assertThat(results.get(2).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, pKey + cKey + 2)));
  }

  @Test
  public void put_MultiplePutWithIfNotExistsGivenWhenOneExists_ShouldThrowNoMutationException()
      throws Exception {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
    assertThatCode(
            () -> {
              storage.put(puts.get(0));
            })
        .doesNotThrowAnyException();
    puts.get(0).withCondition(new PutIfNotExists());
    puts.get(1).withCondition(new PutIfNotExists());
    puts.get(2).withCondition(new PutIfNotExists());
    Scan scan = new Scan(new Key(new IntValue(COL_NAME1, pKey)));

    // Act
    assertThatThrownBy(
            () -> {
              storage.put(Arrays.asList(puts.get(0), puts.get(1), puts.get(2)));
            })
        .isInstanceOf(NoMutationException.class);

    // Assert
    List<Result> results = scanAll(scan);
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, pKey + cKey)));
  }

  @Test
  public void
      put_MultiplePutWithDifferentPartitionsWithIfNotExistsGiven_ShouldThrowMultiPartitionException()
          throws Exception {
    // Arrange
    puts = preparePuts();
    puts.get(0).withCondition(new PutIfNotExists());
    puts.get(3).withCondition(new PutIfNotExists());
    puts.get(6).withCondition(new PutIfNotExists());

    // Act
    assertThatThrownBy(
            () -> {
              storage.put(Arrays.asList(puts.get(0), puts.get(3), puts.get(6)));
            })
        .isInstanceOf(MultiPartitionException.class);

    // Assert
    List<Result> results;
    results = scanAll(new Scan(new Key(new IntValue(COL_NAME1, 0))));
    assertThat(results.size()).isEqualTo(0);
    results = scanAll(new Scan(new Key(new IntValue(COL_NAME1, 3))));
    assertThat(results.size()).isEqualTo(0);
    results = scanAll(new Scan(new Key(new IntValue(COL_NAME1, 6))));
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  public void put_MultiplePutWithDifferentPartitionsGiven_ShouldThrowMultiPartitionException()
      throws Exception {
    // Arrange
    puts = preparePuts();

    // Act
    assertThatThrownBy(
            () -> {
              storage.put(Arrays.asList(puts.get(0), puts.get(3), puts.get(6)));
            })
        .isInstanceOf(MultiPartitionException.class);

    // Assert
    List<Result> results;
    results = scanAll(new Scan(new Key(new IntValue(COL_NAME1, 0))));
    assertThat(results.size()).isEqualTo(0);
    results = scanAll(new Scan(new Key(new IntValue(COL_NAME1, 3))));
    assertThat(results.size()).isEqualTo(0);
    results = scanAll(new Scan(new Key(new IntValue(COL_NAME1, 6))));
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  public void put_MultiplePutWithDifferentConditionsGiven_ShouldStoreProperly() throws Exception {
    // Arrange
    puts = preparePuts();
    storage.put(puts.get(1));
    puts.get(0).withCondition(new PutIfNotExists());
    puts.get(1)
        .withCondition(
            new PutIf(
                new ConditionalExpression(
                    COL_NAME2, new TextValue("1"), ConditionalExpression.Operator.EQ)));

    // Act
    assertThatCode(
            () -> {
              storage.put(Arrays.asList(puts.get(0), puts.get(1)));
            })
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(new Scan(new Key(new IntValue(COL_NAME1, 0))));
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));
    assertThat(results.get(1).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 1)));
  }

  @Test
  public void put_PutWithIfExistsGivenWhenNoSuchRecord_ShouldThrowNoMutationException()
      throws Exception {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
    puts.get(0).withCondition(new PutIfExists());
    Get get = prepareGet(pKey, cKey);

    // Act Assert
    assertThatThrownBy(
            () -> {
              storage.put(puts.get(0));
            })
        .isInstanceOf(NoMutationException.class);

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isFalse();
  }

  @Test
  public void put_PutWithIfExistsGivenWhenSuchRecordExists_ShouldUpdateRecord() throws Exception {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
    Get get = prepareGet(pKey, cKey);

    // Act Assert
    storage.put(puts.get(0));
    puts.get(0).withCondition(new PutIfExists());
    puts.get(0).withValue(new IntValue(COL_NAME3, Integer.MAX_VALUE));
    assertThatCode(
            () -> {
              storage.put(puts.get(0));
            })
        .doesNotThrowAnyException();

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isTrue();
    Result result = actual.get();
    assertThat(result.getValue(COL_NAME1)).isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(result.getValue(COL_NAME4)).isEqualTo(Optional.of(new IntValue(COL_NAME4, cKey)));
    assertThat(result.getValue(COL_NAME3))
        .isEqualTo(Optional.of(new IntValue(COL_NAME3, Integer.MAX_VALUE)));
  }

  @Test
  public void put_PutWithIfGivenWhenSuchRecordExists_ShouldUpdateRecord() throws Exception {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
    Get get = prepareGet(pKey, cKey);

    // Act Assert
    storage.put(puts.get(0));
    puts.get(0)
        .withCondition(
            new PutIf(
                new ConditionalExpression(
                    COL_NAME3, new IntValue(pKey + cKey), ConditionalExpression.Operator.EQ)));
    puts.get(0).withValue(new IntValue(COL_NAME3, Integer.MAX_VALUE));
    assertThatCode(
            () -> {
              storage.put(puts.get(0));
            })
        .doesNotThrowAnyException();

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isTrue();
    Result result = actual.get();
    assertThat(result.getValue(COL_NAME1)).isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(result.getValue(COL_NAME4)).isEqualTo(Optional.of(new IntValue(COL_NAME4, cKey)));
    assertThat(result.getValue(COL_NAME3))
        .isEqualTo(Optional.of(new IntValue(COL_NAME3, Integer.MAX_VALUE)));
  }

  @Test
  public void put_PutWithIfGivenWhenNoSuchRecord_ShouldThrowNoMutationException() throws Exception {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
    Get get = prepareGet(pKey, cKey);

    // Act Assert
    storage.put(puts.get(0));
    puts.get(0)
        .withCondition(
            new PutIf(
                new ConditionalExpression(
                    COL_NAME3, new IntValue(pKey + cKey + 1), ConditionalExpression.Operator.EQ)));
    puts.get(0).withValue(new IntValue(COL_NAME3, Integer.MAX_VALUE));
    assertThatThrownBy(
            () -> {
              storage.put(puts.get(0));
            })
        .isInstanceOf(NoMutationException.class);

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isTrue();
    Result result = actual.get();
    assertThat(result.getValue(COL_NAME1)).isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(result.getValue(COL_NAME4)).isEqualTo(Optional.of(new IntValue(COL_NAME4, cKey)));
    assertThat(result.getValue(COL_NAME3))
        .isEqualTo(Optional.of(new IntValue(COL_NAME3, pKey + cKey)));
  }

  @Test
  public void delete_DeleteWithPartitionKeyGiven_ShouldDeleteRecordProperly() throws Exception {
    // Arrange
    populateRecords();
    int pKey = 0;
    int cKey = 0;

    // Act
    Key partitionKey = new Key(new IntValue(COL_NAME1, pKey));
    Key clusteringKey = new Key(new IntValue(COL_NAME4, cKey));
    Delete delete = new Delete(partitionKey, clusteringKey);
    assertThatCode(
            () -> {
              storage.delete(delete);
            })
        .doesNotThrowAnyException();

    // Assert
    List<Result> actual = scanAll(new Scan(partitionKey));
    assertThat(actual.size()).isEqualTo(2);
  }

  @Test
  public void delete_DeleteWithPartitionKeyAndClusteringKeyGiven_ShouldDeleteSingleRecordProperly()
      throws Exception {
    // Arrange
    populateRecords();
    int pKey = 0;
    int cKey = 0;
    Key partitionKey = new Key(new IntValue(COL_NAME1, pKey));

    // Act
    Delete delete = prepareDelete(pKey, cKey);
    assertThatCode(
            () -> {
              storage.delete(delete);
            })
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(new Scan(partitionKey));
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, cKey + 1)));
    assertThat(results.get(1).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, cKey + 2)));
  }

  @Test
  public void delete_DeleteWithIfExistsGivenWhenNoSuchRecord_ShouldThrowNoMutationException()
      throws Exception {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act Assert
    Delete delete = prepareDelete(pKey, Integer.MAX_VALUE);
    delete.withCondition(new DeleteIfExists());
    assertThatThrownBy(
            () -> {
              storage.delete(delete);
            })
        .isInstanceOf(NoMutationException.class);
  }

  @Test
  public void delete_DeleteWithIfExistsGivenWhenSuchRecordExists_ShouldDeleteProperly()
      throws Exception {
    // Arrange
    populateRecords();
    int pKey = 0;
    int cKey = 0;
    Key partitionKey = new Key(new IntValue(COL_NAME1, pKey));
    Key clusteringKey = new Key(new IntValue(COL_NAME4, cKey));

    // Act
    Delete delete = prepareDelete(pKey, cKey);
    delete.withCondition(new DeleteIfExists());
    assertThatCode(
            () -> {
              storage.delete(delete);
            })
        .doesNotThrowAnyException();

    // Assert
    Optional<Result> actual = storage.get(new Get(partitionKey, clusteringKey));
    assertThat(actual.isPresent()).isFalse();
  }

  @Test
  public void delete_DeleteWithIfGivenWhenNoSuchRecord_ShouldThrowNoMutationException()
      throws Exception {
    // Arrange
    populateRecords();
    int pKey = 0;
    int cKey = 0;
    Key partitionKey = new Key(new IntValue(COL_NAME1, pKey));
    Key clusteringKey = new Key(new IntValue(COL_NAME4, cKey));

    // Act
    Delete delete = prepareDelete(pKey, cKey);
    delete.withCondition(
        new DeleteIf(
            new ConditionalExpression(
                COL_NAME2,
                new TextValue(Integer.toString(Integer.MAX_VALUE)),
                ConditionalExpression.Operator.EQ)));
    assertThatThrownBy(
            () -> {
              storage.delete(delete);
            })
        .isInstanceOf(NoMutationException.class);

    // Assert
    Optional<Result> actual = storage.get(new Get(partitionKey, clusteringKey));
    assertThat(actual.isPresent()).isTrue();
  }

  @Test
  public void delete_DeleteWithIfGivenWhenSuchRecordExists_ShouldDeleteProperly() throws Exception {
    // Arrange
    populateRecords();
    int pKey = 0;
    int cKey = 0;
    Key partitionKey = new Key(new IntValue(COL_NAME1, pKey));
    Key clusteringKey = new Key(new IntValue(COL_NAME4, cKey));

    // Act
    Delete delete = prepareDelete(pKey, cKey);
    delete.withCondition(
        new DeleteIf(
            new ConditionalExpression(
                COL_NAME2,
                new TextValue(Integer.toString(pKey)),
                ConditionalExpression.Operator.EQ)));
    assertThatCode(
            () -> {
              storage.delete(delete);
            })
        .doesNotThrowAnyException();

    // Assert
    Optional<Result> actual = storage.get(new Get(partitionKey, clusteringKey));
    assertThat(actual.isPresent()).isFalse();
  }

  @Test
  public void delete_MultipleDeleteWithDifferentConditionsGiven_ShouldDeleteProperly()
      throws Exception {
    // Arrange
    puts = preparePuts();
    deletes = prepareDeletes();
    storage.mutate(Arrays.asList(puts.get(0), puts.get(1), puts.get(2)));
    deletes.get(0).withCondition(new DeleteIfExists());
    deletes
        .get(1)
        .withCondition(
            new DeleteIf(
                new ConditionalExpression(
                    COL_NAME2, new TextValue("1"), ConditionalExpression.Operator.EQ)));

    // Act
    assertThatCode(
            () -> {
              storage.delete(Arrays.asList(deletes.get(0), deletes.get(1), deletes.get(2)));
            })
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(new Scan(new Key(new IntValue(COL_NAME1, 0))));
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  public void mutate_MultiplePutGiven_ShouldStoreProperly() throws Exception {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
    Scan scan = new Scan(new Key(new IntValue(COL_NAME1, pKey)));

    // Act
    assertThatCode(
            () -> {
              storage.mutate(Arrays.asList(puts.get(0), puts.get(1), puts.get(2)));
            })
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(scan);
    assertThat(results.size()).isEqualTo(3);
    assertThat(results.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, pKey + cKey)));
    assertThat(results.get(1).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, pKey + cKey + 1)));
    assertThat(results.get(2).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, pKey + cKey + 2)));
  }

  @Test
  public void mutate_MultiplePutWithDifferentPartitionsGiven_ShouldThrowMultiPartitionException()
      throws Exception {
    // Arrange
    puts = preparePuts();

    // Act
    assertThatCode(
            () -> {
              storage.mutate(Arrays.asList(puts.get(0), puts.get(3), puts.get(6)));
            })
        .isInstanceOf(MultiPartitionException.class);

    // Assert
    List<Result> results;
    results = scanAll(new Scan(new Key(new IntValue(COL_NAME1, 0))));
    assertThat(results.size()).isEqualTo(0);
    results = scanAll(new Scan(new Key(new IntValue(COL_NAME1, 3))));
    assertThat(results.size()).isEqualTo(0);
    results = scanAll(new Scan(new Key(new IntValue(COL_NAME1, 6))));
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  public void mutate_PutAndDeleteGiven_ShouldUpdateAndDeleteRecordsProperly() throws Exception {
    // Arrange
    populateRecords();
    puts = preparePuts();
    puts.get(1).withValue(new IntValue(COL_NAME3, Integer.MAX_VALUE));
    puts.get(2).withValue(new IntValue(COL_NAME3, Integer.MIN_VALUE));

    int pKey = 0;
    int cKey = 0;
    Delete delete = prepareDelete(pKey, cKey);

    Scan scan = new Scan(new Key(new IntValue(COL_NAME1, pKey)));

    // Act
    assertThatCode(
            () -> {
              storage.mutate(Arrays.asList(delete, puts.get(1), puts.get(2)));
            })
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(scan);
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get(0).getValue(COL_NAME3))
        .isEqualTo(Optional.of(new IntValue(COL_NAME3, Integer.MAX_VALUE)));
    assertThat(results.get(1).getValue(COL_NAME3))
        .isEqualTo(Optional.of(new IntValue(COL_NAME3, Integer.MIN_VALUE)));
  }

  @Test
  public void mutate_SinglePutGiven_ShouldStoreProperly() throws Exception {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    puts = preparePuts();
    Key partitionKey = new Key(new IntValue(COL_NAME1, pKey));
    Key clusteringKey = new Key(new IntValue(COL_NAME4, cKey));
    Get get = new Get(partitionKey, clusteringKey);

    // Act
    storage.mutate(Arrays.asList(puts.get(pKey * 2 + cKey)));

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, pKey)));
    assertThat(actual.get().getValue(COL_NAME2))
        .isEqualTo(Optional.of(new TextValue(COL_NAME2, Integer.toString(pKey + cKey))));
    assertThat(actual.get().getValue(COL_NAME3))
        .isEqualTo(Optional.of(new IntValue(COL_NAME3, pKey + cKey)));
    assertThat(actual.get().getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, cKey)));
    assertThat(actual.get().getValue(COL_NAME5))
        .isEqualTo(Optional.of(new BooleanValue(COL_NAME5, cKey % 2 == 0)));
  }

  @Test
  public void mutate_SingleDeleteGiven_ShouldDeleteRecordProperly() throws Exception {
    // Arrange
    populateRecords();
    int pKey = 0;
    int cKey = 0;

    // Act
    Key partitionKey = new Key(new IntValue(COL_NAME1, pKey));
    Key clusteringKey = new Key(new IntValue(COL_NAME4, cKey));
    Delete delete = new Delete(partitionKey, clusteringKey);
    assertThatCode(
            () -> {
              storage.mutate(Arrays.asList(delete));
            })
        .doesNotThrowAnyException();

    // Assert
    List<Result> actual = scanAll(new Scan(partitionKey));
    assertThat(actual.size()).isEqualTo(2);
  }

  @Test
  public void mutate_EmptyListGiven_ShouldThrowIllegalArgumentException() throws Exception {
    // Act
    assertThatCode(
            () -> {
              storage.mutate(new ArrayList<>());
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void put_PutWithoutClusteringKeyGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    int pKey = 0;
    Key partitionKey = new Key(new IntValue(COL_NAME1, pKey));
    Put put = new Put(partitionKey);

    // Act Assert
    assertThatCode(
            () -> {
              storage.put(put);
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void put_IncorrectPutGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    Key partitionKey = new Key(new IntValue(COL_NAME1, pKey));
    Put put = new Put(partitionKey).withValue(new IntValue(COL_NAME4, cKey));

    // Act Assert
    assertThatCode(
            () -> {
              storage.put(put);
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void get_GetGivenForIndexedColumn_ShouldGet() throws Exception {
    // Arrange
    storage.put(preparePuts().get(0)); // (0,0)
    int c3 = 0;
    Get get = new Get(new Key(new IntValue(COL_NAME3, c3)));

    // Act
    Optional<Result> actual = storage.get(get);

    // Assert
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME1)).isEqualTo(Optional.of(new IntValue(COL_NAME1, 0)));
    assertThat(actual.get().getValue(COL_NAME4)).isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));
  }

  @Test
  public void
      get_GetGivenForIndexedColumnMatchingMultipleRecords_ShouldThrowIllegalArgumentException() {
    // Arrange
    populateRecords();
    int c3 = 3;
    Get get = new Get(new Key(new IntValue(COL_NAME3, c3)));

    // Act Assert
    assertThatThrownBy(() -> storage.get(get)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void scan_ScanGivenForIndexedColumn_ShouldScan() throws Exception {
    // Arrange
    populateRecords();
    int c3 = 3;
    Scan scan = new Scan(new Key(new IntValue(COL_NAME3, c3)));

    // Act
    List<Result> actual = scanAll(scan);

    // Assert
    assertThat(actual.size()).isEqualTo(3); // (1,2), (2,1), (3,0)
    assertThat(actual.get(0).getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, 1)));
    assertThat(actual.get(0).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 2)));
    assertThat(actual.get(1).getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, 2)));
    assertThat(actual.get(1).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 1)));
    assertThat(actual.get(2).getValue(COL_NAME1))
        .isEqualTo(Optional.of(new IntValue(COL_NAME1, 3)));
    assertThat(actual.get(2).getValue(COL_NAME4))
        .isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));
  }

  @Test
  public void scan_ScanGivenForNonIndexedColumn_ShouldThrowIllegalArgumentException() {
    // Arrange
    populateRecords();
    String c2 = "test";
    Scan scan = new Scan(new Key(new TextValue(COL_NAME2, c2)));

    // Act Assert
    assertThatThrownBy(() -> storage.scan(scan)).isInstanceOf(IllegalArgumentException.class);
  }

  private void populateRecords() {
    puts = preparePuts();
    puts.forEach(
        p -> {
          assertThatCode(
                  () -> {
                    storage.put(p);
                  })
              .doesNotThrowAnyException();
        });
  }

  private Get prepareGet(int pKey, int cKey) {
    Key partitionKey = new Key(new IntValue(COL_NAME1, pKey));
    Key clusteringKey = new Key(new IntValue(COL_NAME4, cKey));
    return new Get(partitionKey, clusteringKey);
  }

  private List<Put> preparePuts() {
    List<Put> puts = new ArrayList<>();

    IntStream.range(0, 5)
        .forEach(
            i -> {
              IntStream.range(0, 3)
                  .forEach(
                      j -> {
                        Key partitionKey = new Key(new IntValue(COL_NAME1, i));
                        Key clusteringKey = new Key(new IntValue(COL_NAME4, j));
                        Put put =
                            new Put(partitionKey, clusteringKey)
                                .withValue(new TextValue(COL_NAME2, Integer.toString(i + j)))
                                .withValue(new IntValue(COL_NAME3, i + j))
                                .withValue(new BooleanValue(COL_NAME5, j % 2 == 0));
                        puts.add(put);
                      });
            });

    return puts;
  }

  private Delete prepareDelete(int pKey, int cKey) {
    Key partitionKey = new Key(new IntValue(COL_NAME1, pKey));
    Key clusteringKey = new Key(new IntValue(COL_NAME4, cKey));
    return new Delete(partitionKey, clusteringKey);
  }

  private List<Delete> prepareDeletes() {
    List<Delete> deletes = new ArrayList<>();

    IntStream.range(0, 5)
        .forEach(
            i -> {
              IntStream.range(0, 3)
                  .forEach(
                      j -> {
                        Key partitionKey = new Key(new IntValue(COL_NAME1, i));
                        Key clusteringKey = new Key(new IntValue(COL_NAME4, j));
                        Delete delete = new Delete(partitionKey, clusteringKey);
                        deletes.add(delete);
                      });
            });

    return deletes;
  }

  private List<Result> scanAll(Scan scan) throws Exception {
    try (Scanner scanner = storage.scan(scan)) {
      return scanner.all();
    }
  }
}
