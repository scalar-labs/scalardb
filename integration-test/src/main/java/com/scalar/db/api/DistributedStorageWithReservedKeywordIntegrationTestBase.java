package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.google.common.collect.ImmutableList;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.service.StorageFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DistributedStorageWithReservedKeywordIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(DistributedStorageWithReservedKeywordIntegrationTestBase.class);

  private static final String TEST_NAME = "storage_reserved_kw";

  private DistributedStorage storage;
  private DistributedStorageAdmin admin;

  private String namespace;
  private String tableName;
  private String columnName1;
  private String columnName2;
  private String columnName3;
  private String columnName4;
  private String columnName5;

  @BeforeAll
  public void beforeAll() throws Exception {
    initialize(TEST_NAME);
    StorageFactory factory = StorageFactory.create(getProperties(TEST_NAME));
    admin = factory.getAdmin();
    namespace = getNamespace();
    tableName = getTableName();
    columnName1 = getColumnName1();
    columnName2 = getColumnName2();
    columnName3 = getColumnName3();
    columnName4 = getColumnName4();
    columnName5 = getColumnName5();
    createTable();
    storage = factory.getStorage();
  }

  protected void initialize(String testName) throws Exception {}

  protected abstract Properties getProperties(String testName);

  protected abstract String getNamespace();

  protected abstract String getTableName();

  protected abstract String getColumnName1();

  protected abstract String getColumnName2();

  protected abstract String getColumnName3();

  protected abstract String getColumnName4();

  protected abstract String getColumnName5();

  private void createTable() throws ExecutionException {
    Map<String, String> options = getCreationOptions();
    admin.createNamespace(namespace, true, options);
    admin.createTable(
        namespace,
        tableName,
        TableMetadata.newBuilder()
            .addColumn(columnName1, DataType.INT)
            .addColumn(columnName2, DataType.TEXT)
            .addColumn(columnName3, DataType.INT)
            .addColumn(columnName4, DataType.INT)
            .addColumn(columnName5, DataType.BOOLEAN)
            .addPartitionKey(columnName1)
            .addClusteringKey(columnName4)
            .addSecondaryIndex(columnName3)
            .build(),
        true,
        options);
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  @BeforeEach
  public void setUp() throws Exception {
    truncateTable();
    storage.with(namespace, tableName);
  }

  private void truncateTable() throws ExecutionException {
    admin.truncateTable(namespace, tableName);
  }

  @AfterAll
  public void afterAll() throws Exception {
    try {
      dropTable();
    } catch (Exception e) {
      logger.warn("Failed to drop tables", e);
    }

    try {
      if (admin != null) {
        admin.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close admin", e);
    }

    try {
      if (storage != null) {
        storage.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close storage", e);
    }
  }

  private void dropTable() throws ExecutionException {
    admin.dropTable(namespace, tableName);
    admin.dropNamespace(namespace);
  }

  @Test
  public void
      get_GetWithReservedKeywordAndPartitionKeyAndClusteringKeyGiven_ShouldRetrieveSingleResult()
          throws ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Get get = prepareGet(pKey, 0);
    Optional<Result> actual = storage.get(get);

    // Assert
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getValue(columnName1))
        .isEqualTo(Optional.of(new IntValue(columnName1, pKey)));
    assertThat(actual.get().getValue(columnName4))
        .isEqualTo(Optional.of(new IntValue(columnName4, 0)));
  }

  @Test
  public void get_GetWithReservedKeywordAndProjectionsGiven_ShouldRetrieveSpecifiedValues()
      throws ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;
    int cKey = 0;

    // Act
    Get get = prepareGet(pKey, cKey);
    get.withProjection(columnName1).withProjection(columnName2).withProjection(columnName3);
    Optional<Result> actual = storage.get(get);

    // Assert
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getValue(columnName1))
        .isEqualTo(Optional.of(new IntValue(columnName1, pKey)));
    assertThat(actual.get().getValue(columnName2))
        .isEqualTo(Optional.of(new TextValue(columnName2, Integer.toString(pKey + cKey))));
    assertThat(actual.get().getValue(columnName3))
        .isEqualTo(Optional.of(new IntValue(columnName3, pKey + cKey)));
    assertThat(actual.get().getValue(columnName4).isPresent()).isFalse();
    assertThat(actual.get().getValue(columnName5).isPresent()).isFalse();
  }

  @Test
  public void scan_ScanWithReservedKeywordAndProjectionsGiven_ShouldRetrieveSpecifiedValues()
      throws IOException, ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan =
        new Scan(new Key(columnName1, pKey))
            .withProjection(columnName1)
            .withProjection(columnName2)
            .withProjection(columnName3);
    List<Result> actual = scanAll(scan);

    // Assert
    actual.forEach(
        a ->
            assertThat(a.getContainedColumnNames())
                .containsOnly(columnName1, columnName2, columnName3));
    assertThat(actual.size()).isEqualTo(3);
    assertThat(actual.get(0).getInt(columnName1)).isEqualTo(0);
    assertThat(actual.get(0).getInt(columnName3)).isEqualTo(0);
    assertThat(actual.get(1).getInt(columnName1)).isEqualTo(0);
    assertThat(actual.get(1).getInt(columnName3)).isEqualTo(1);
    assertThat(actual.get(2).getInt(columnName1)).isEqualTo(0);
    assertThat(actual.get(2).getInt(columnName3)).isEqualTo(2);
  }

  @Test
  public void
      scan_ScanWithReservedKeywordAndPartitionKeyGivenAndResultsIteratedWithOne_ShouldReturnWhatsPut()
          throws ExecutionException, IOException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan = new Scan(new Key(columnName1, pKey));
    Scanner scanner = storage.scan(scan);

    // Assert
    List<Result> results = new ArrayList<>();

    Optional<Result> result = scanner.one();
    assertThat(result.isPresent()).isTrue();
    results.add(result.get());

    result = scanner.one();
    assertThat(result.isPresent()).isTrue();
    results.add(result.get());

    result = scanner.one();
    assertThat(result.isPresent()).isTrue();
    results.add(result.get());

    result = scanner.one();
    assertThat(result.isPresent()).isFalse();

    assertThat(results.size()).isEqualTo(3);
    assertThat(results.get(0).getValue(columnName1).isPresent()).isTrue();
    assertThat(results.get(0).getValue(columnName1).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(0).getValue(columnName4).isPresent()).isTrue();
    assertThat(results.get(0).getValue(columnName4).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(1).getValue(columnName1).isPresent()).isTrue();
    assertThat(results.get(1).getValue(columnName1).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(1).getValue(columnName4).isPresent()).isTrue();
    assertThat(results.get(1).getValue(columnName4).get().getAsInt()).isEqualTo(1);
    assertThat(results.get(2).getValue(columnName1).isPresent()).isTrue();
    assertThat(results.get(2).getValue(columnName1).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(2).getValue(columnName4).isPresent()).isTrue();
    assertThat(results.get(2).getValue(columnName4).get().getAsInt()).isEqualTo(2);

    scanner.close();
  }

  @Test
  public void put_WithReservedKeywordAndSinglePutGiven_ShouldStoreProperly()
      throws ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    List<Put> puts = preparePuts();
    Key partitionKey = new Key(columnName1, pKey);
    Key clusteringKey = new Key(columnName4, cKey);
    Get get = new Get(partitionKey, clusteringKey);

    // Act
    storage.put(puts.get(pKey * 2 + cKey));

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getValue(columnName1))
        .isEqualTo(Optional.of(new IntValue(columnName1, pKey)));
    assertThat(actual.get().getValue(columnName2))
        .isEqualTo(Optional.of(new TextValue(columnName2, Integer.toString(pKey + cKey))));
    assertThat(actual.get().getValue(columnName3))
        .isEqualTo(Optional.of(new IntValue(columnName3, pKey + cKey)));
    assertThat(actual.get().getValue(columnName4))
        .isEqualTo(Optional.of(new IntValue(columnName4, cKey)));
    assertThat(actual.get().getValue(columnName5))
        .isEqualTo(Optional.of(new BooleanValue(columnName5, cKey % 2 == 0)));
  }

  @Test
  public void put_PutWithReservedKeywordAndIfGivenWhenSuchRecordExists_ShouldUpdateRecord()
      throws ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    List<Put> puts = preparePuts();
    Get get = prepareGet(pKey, cKey);

    // Act Assert
    storage.put(puts.get(0));
    puts.get(0)
        .withCondition(
            new PutIf(
                new ConditionalExpression(
                    columnName3, new IntValue(pKey + cKey), ConditionalExpression.Operator.EQ)));
    puts.get(0).withValue(columnName3, Integer.MAX_VALUE);
    assertThatCode(() -> storage.put(puts.get(0))).doesNotThrowAnyException();

    // Assert
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isTrue();
    Result result = actual.get();
    assertThat(result.getValue(columnName1))
        .isEqualTo(Optional.of(new IntValue(columnName1, pKey)));
    assertThat(result.getValue(columnName4))
        .isEqualTo(Optional.of(new IntValue(columnName4, cKey)));
    assertThat(result.getValue(columnName3))
        .isEqualTo(Optional.of(new IntValue(columnName3, Integer.MAX_VALUE)));
  }

  @Test
  public void put_WithReservedKeywordAndMultiplePutGiven_ShouldStoreProperly()
      throws IOException, ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    List<Put> puts = preparePuts();
    Scan scan = new Scan(new Key(columnName1, pKey));

    // Act
    assertThatCode(() -> storage.put(Arrays.asList(puts.get(0), puts.get(1), puts.get(2))))
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(scan);
    assertThat(results.size()).isEqualTo(3);
    assertThat(results.get(0).getValue(columnName1).isPresent()).isTrue();
    assertThat(results.get(0).getValue(columnName1).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(0).getValue(columnName4).isPresent()).isTrue();
    assertThat(results.get(0).getValue(columnName4).get().getAsInt()).isEqualTo(pKey + cKey);
    assertThat(results.get(1).getValue(columnName1).isPresent()).isTrue();
    assertThat(results.get(1).getValue(columnName1).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(1).getValue(columnName4).isPresent()).isTrue();
    assertThat(results.get(1).getValue(columnName4).get().getAsInt()).isEqualTo(pKey + cKey + 1);
    assertThat(results.get(2).getValue(columnName1).isPresent()).isTrue();
    assertThat(results.get(2).getValue(columnName1).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(2).getValue(columnName4).isPresent()).isTrue();
    assertThat(results.get(2).getValue(columnName4).get().getAsInt()).isEqualTo(pKey + cKey + 2);
  }

  @Test
  public void
      put_WithReservedKeywordAndMultiplePutWithDifferentConditionsGiven_ShouldStoreProperly()
          throws IOException, ExecutionException {
    // Arrange
    List<Put> puts = preparePuts();
    storage.put(puts.get(1));
    puts.get(0).withCondition(new PutIfNotExists());
    puts.get(1)
        .withCondition(
            new PutIf(
                new ConditionalExpression(
                    columnName2, new TextValue("1"), ConditionalExpression.Operator.EQ)));

    // Act
    assertThatCode(() -> storage.put(Arrays.asList(puts.get(0), puts.get(1))))
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(new Scan(new Key(columnName1, 0)));
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get(0).getValue(columnName1).isPresent()).isTrue();
    assertThat(results.get(0).getValue(columnName1).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(0).getValue(columnName4).isPresent()).isTrue();
    assertThat(results.get(0).getValue(columnName4).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(1).getValue(columnName1).isPresent()).isTrue();
    assertThat(results.get(1).getValue(columnName1).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(1).getValue(columnName4).isPresent()).isTrue();
    assertThat(results.get(1).getValue(columnName4).get().getAsInt()).isEqualTo(1);
  }

  @Test
  public void
      delete_WithReservedKeywordAndDeleteWithPartitionKeyAndClusteringKeyGiven_ShouldDeleteSingleRecordProperly()
          throws IOException, ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;
    int cKey = 0;
    Key partitionKey = new Key(columnName1, pKey);

    // Act
    Delete delete = prepareDelete(pKey, cKey);
    assertThatCode(() -> storage.delete(delete)).doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(new Scan(partitionKey));
    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get(0).getValue(columnName1).isPresent()).isTrue();
    assertThat(results.get(0).getValue(columnName1).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(0).getValue(columnName4).isPresent()).isTrue();
    assertThat(results.get(0).getValue(columnName4).get().getAsInt()).isEqualTo(cKey + 1);
    assertThat(results.get(1).getValue(columnName1).isPresent()).isTrue();
    assertThat(results.get(1).getValue(columnName1).get().getAsInt()).isEqualTo(0);
    assertThat(results.get(1).getValue(columnName4).isPresent()).isTrue();
    assertThat(results.get(1).getValue(columnName4).get().getAsInt()).isEqualTo(cKey + 2);
  }

  @Test
  public void
      delete_WithReservedKeywordAndMultipleDeleteWithDifferentConditionsGiven_ShouldDeleteProperly()
          throws IOException, ExecutionException {
    // Arrange
    List<Put> puts = preparePuts();
    List<Delete> deletes = prepareDeletes();
    storage.mutate(Arrays.asList(puts.get(0), puts.get(1), puts.get(2)));
    deletes.get(0).withCondition(new DeleteIfExists());
    deletes
        .get(1)
        .withCondition(
            new DeleteIf(
                new ConditionalExpression(
                    columnName2, new TextValue("1"), ConditionalExpression.Operator.EQ)));

    // Act
    assertThatCode(
            () -> storage.delete(Arrays.asList(deletes.get(0), deletes.get(1), deletes.get(2))))
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(new Scan(new Key(columnName1, 0)));
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  public void
      delete_WithReservedKeywordAndDeleteWithIfGivenWhenSuchRecordExists_ShouldDeleteProperly()
          throws ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;
    int cKey = 0;
    Key partitionKey = new Key(columnName1, pKey);
    Key clusteringKey = new Key(columnName4, cKey);

    // Act
    Delete delete = prepareDelete(pKey, cKey);
    delete.withCondition(
        new DeleteIf(
            new ConditionalExpression(
                columnName2,
                new TextValue(Integer.toString(pKey)),
                ConditionalExpression.Operator.EQ)));
    assertThatCode(() -> storage.delete(delete)).doesNotThrowAnyException();

    // Assert
    Optional<Result> actual = storage.get(new Get(partitionKey, clusteringKey));
    assertThat(actual.isPresent()).isFalse();
  }

  @Test
  public void get_WithReservedKeywordAndGetGivenForIndexedColumn_ShouldGet()
      throws ExecutionException {
    // Arrange
    storage.put(preparePuts().get(0)); // (0,0)
    int c3 = 0;
    Get get = new Get(new Key(columnName3, c3));

    // Act
    Optional<Result> actual = storage.get(get);

    // Assert
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getValue(columnName1))
        .isEqualTo(Optional.of(new IntValue(columnName1, 0)));
    assertThat(actual.get().getValue(columnName4))
        .isEqualTo(Optional.of(new IntValue(columnName4, 0)));
  }

  @Test
  public void scan_WithReservedKeywordAndScanGivenForIndexedColumn_ShouldScan()
      throws ExecutionException, IOException {
    // Arrange
    populateRecords();
    int c3 = 3;
    Scan scan = new Scan(new Key(columnName3, c3));

    // Act
    List<Result> actual = scanAll(scan);

    // Assert
    assertThat(actual.size()).isEqualTo(3); // (1,2), (2,1), (3,0)
    List<List<Integer>> expectedValues =
        new ArrayList<>(
            Arrays.asList(Arrays.asList(1, 2), Arrays.asList(2, 1), Arrays.asList(3, 0)));
    for (Result result : actual) {
      assertThat(result.getValue(columnName1).isPresent()).isTrue();
      assertThat(result.getValue(columnName4).isPresent()).isTrue();

      int col1Val = result.getValue(columnName1).get().getAsInt();
      int col4Val = result.getValue(columnName4).get().getAsInt();
      List<Integer> col1AndCol4 = Arrays.asList(col1Val, col4Val);
      assertThat(expectedValues).contains(col1AndCol4);
      expectedValues.remove(col1AndCol4);
    }

    assertThat(expectedValues).isEmpty();
  }

  @Test
  public void scan_WithReservedKeywordAndClusteringKeyRange_ShouldReturnProperResult()
      throws ExecutionException, IOException {
    // Arrange
    populateRecords();
    Scan scan =
        new Scan(new Key(columnName1, 1))
            .withStart(new Key(columnName4, 1), false)
            .withEnd(new Key(columnName4, 3), false)
            .withOrdering(new Ordering(columnName4, Order.DESC))
            .forNamespace(namespace)
            .forTable(tableName);

    List<Integer> expected = ImmutableList.of(2);

    // Act
    List<Result> actual = scanAll(scan);

    // Assert
    assertThat(actual.size()).isEqualTo(1);
    assertThat(actual.get(0).getValue(columnName4).isPresent()).isTrue();
    assertThat(actual.get(0).getValue(columnName4).get().getAsInt()).isEqualTo(expected.get(0));
  }

  private void populateRecords() {
    List<Put> puts = preparePuts();
    puts.forEach(p -> assertThatCode(() -> storage.put(p)).doesNotThrowAnyException());
  }

  private Get prepareGet(int pKey, int cKey) {
    Key partitionKey = new Key(columnName1, pKey);
    Key clusteringKey = new Key(columnName4, cKey);
    return new Get(partitionKey, clusteringKey);
  }

  private List<Put> preparePuts() {
    List<Put> puts = new ArrayList<>();

    IntStream.range(0, 5)
        .forEach(
            i ->
                IntStream.range(0, 3)
                    .forEach(
                        j -> {
                          Key partitionKey = new Key(columnName1, i);
                          Key clusteringKey = new Key(columnName4, j);
                          Put put =
                              new Put(partitionKey, clusteringKey)
                                  .withValue(columnName2, Integer.toString(i + j))
                                  .withValue(columnName3, i + j)
                                  .withValue(columnName5, j % 2 == 0);
                          puts.add(put);
                        }));

    return puts;
  }

  private Delete prepareDelete(int pKey, int cKey) {
    Key partitionKey = new Key(columnName1, pKey);
    Key clusteringKey = new Key(columnName4, cKey);
    return new Delete(partitionKey, clusteringKey);
  }

  private List<Delete> prepareDeletes() {
    List<Delete> deletes = new ArrayList<>();

    IntStream.range(0, 5)
        .forEach(
            i ->
                IntStream.range(0, 3)
                    .forEach(
                        j -> {
                          Key partitionKey = new Key(columnName1, i);
                          Key clusteringKey = new Key(columnName4, j);
                          Delete delete = new Delete(partitionKey, clusteringKey);
                          deletes.add(delete);
                        }));

    return deletes;
  }

  private List<Result> scanAll(Scan scan) throws ExecutionException, IOException {
    try (Scanner scanner = storage.scan(scan)) {
      return scanner.all();
    }
  }
}
