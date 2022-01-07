package com.scalar.db.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.service.StorageFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

@SuppressFBWarnings({"ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD", "MS_CANNOT_BE_FINAL"})
public abstract class StorageWithReservedKeywordIntegrationTestBase {

  protected static String NAMESPACE = "integration_testing";
  protected static String TABLE = "test_table";
  protected static String COL_NAME1 = "c1";
  protected static String COL_NAME2 = "c2";
  protected static String COL_NAME3 = "c3";
  protected static String COL_NAME4 = "c4";
  protected static String COL_NAME5 = "c5";

  private static boolean initialized;
  private static DistributedStorage storage;
  private static DistributedStorageAdmin admin;
  private static String namespace;

  @Before
  public void setUp() throws Exception {
    if (!initialized) {
      initialize();
      StorageFactory factory = new StorageFactory(getDatabaseConfig());
      admin = factory.getAdmin();
      namespace = getNamespace();
      createTable();
      storage = factory.getStorage();
      initialized = true;
    }

    truncateTable();
    storage.with(namespace, TABLE);
  }

  protected void initialize() throws Exception {}

  protected abstract DatabaseConfig getDatabaseConfig();

  protected String getNamespace() {
    return NAMESPACE;
  }

  private void createTable() throws ExecutionException {
    Map<String, String> options = getCreateOptions();
    admin.createNamespace(namespace, true, options);
    admin.createTable(
        namespace,
        TABLE,
        TableMetadata.newBuilder()
            .addColumn(COL_NAME1, DataType.INT)
            .addColumn(COL_NAME2, DataType.TEXT)
            .addColumn(COL_NAME3, DataType.INT)
            .addColumn(COL_NAME4, DataType.INT)
            .addColumn(COL_NAME5, DataType.BOOLEAN)
            .addPartitionKey(COL_NAME1)
            .addClusteringKey(COL_NAME4)
            .addSecondaryIndex(COL_NAME3)
            .build(),
        true,
        options);
  }

  protected Map<String, String> getCreateOptions() {
    return Collections.emptyMap();
  }

  private void truncateTable() throws ExecutionException {
    admin.truncateTable(namespace, TABLE);
  }

  @AfterClass
  public static void tearDownAfterClass() throws ExecutionException {
    deleteTable();
    admin.close();
    storage.close();
    initialized = false;
  }

  private static void deleteTable() throws ExecutionException {
    admin.dropTable(namespace, TABLE);
    admin.dropNamespace(namespace);
  }

  @Test
  public void get_GetWithPartitionKeyAndClusteringKeyGiven_ShouldRetrieveSingleResult()
      throws ExecutionException {
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
  public void get_GetWithProjectionsGiven_ShouldRetrieveSpecifiedValues()
      throws ExecutionException {
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
  public void scan_ScanWithProjectionsGiven_ShouldRetrieveSpecifiedValues()
      throws IOException, ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan =
        new Scan(new Key(COL_NAME1, pKey))
            .withProjection(COL_NAME1)
            .withProjection(COL_NAME2)
            .withProjection(COL_NAME3);
    List<Result> actual = scanAll(scan);

    // Assert
    assertScanResultWithoutOrdering(actual, pKey, COL_NAME4, Arrays.asList(0, 1, 2));
    actual.forEach(
        a -> {
          assertThat(a.getValue(COL_NAME1).isPresent()).isTrue();
          assertThat(a.getValue(COL_NAME2).isPresent()).isTrue();
          assertThat(a.getValue(COL_NAME3).isPresent()).isTrue();
          assertThat(a.getValue(COL_NAME4).isPresent()).isTrue(); // since it's clustering key
          assertThat(a.getValue(COL_NAME5).isPresent()).isFalse();
        });
  }

  @Test
  public void scan_ScanWithPartitionKeyGivenAndResultsIteratedWithOne_ShouldReturnWhatsPut()
      throws ExecutionException, IOException {
    // Arrange
    populateRecords();
    int pKey = 0;

    // Act
    Scan scan = new Scan(new Key(COL_NAME1, pKey));
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

    assertScanResultWithoutOrdering(results, pKey, COL_NAME4, Arrays.asList(0, 1, 2));

    scanner.close();
  }

  @Test
  public void put_SinglePutGiven_ShouldStoreProperly() throws ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    List<Put> puts = preparePuts();
    Key partitionKey = new Key(COL_NAME1, pKey);
    Key clusteringKey = new Key(COL_NAME4, cKey);
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
  public void put_PutWithIfGivenWhenSuchRecordExists_ShouldUpdateRecord()
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
                    COL_NAME3, new IntValue(pKey + cKey), ConditionalExpression.Operator.EQ)));
    puts.get(0).withValue(COL_NAME3, Integer.MAX_VALUE);
    assertThatCode(() -> storage.put(puts.get(0))).doesNotThrowAnyException();

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
  public void put_MultiplePutGiven_ShouldStoreProperly() throws IOException, ExecutionException {
    // Arrange
    int pKey = 0;
    int cKey = 0;
    List<Put> puts = preparePuts();
    Scan scan = new Scan(new Key(COL_NAME1, pKey));

    // Act
    assertThatCode(() -> storage.put(Arrays.asList(puts.get(0), puts.get(1), puts.get(2))))
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(scan);
    assertScanResultWithoutOrdering(
        results, pKey, COL_NAME4, Arrays.asList(pKey + cKey, pKey + cKey + 1, pKey + cKey + 2));
  }

  @Test
  public void put_MultiplePutWithDifferentConditionsGiven_ShouldStoreProperly()
      throws IOException, ExecutionException {
    // Arrange
    List<Put> puts = preparePuts();
    storage.put(puts.get(1));
    puts.get(0).withCondition(new PutIfNotExists());
    puts.get(1)
        .withCondition(
            new PutIf(
                new ConditionalExpression(
                    COL_NAME2, new TextValue("1"), ConditionalExpression.Operator.EQ)));

    // Act
    assertThatCode(() -> storage.put(Arrays.asList(puts.get(0), puts.get(1))))
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(new Scan(new Key(COL_NAME1, 0)));
    assertScanResultWithoutOrdering(results, 0, COL_NAME4, Arrays.asList(0, 1));
  }

  @Test
  public void delete_DeleteWithPartitionKeyAndClusteringKeyGiven_ShouldDeleteSingleRecordProperly()
      throws IOException, ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;
    int cKey = 0;
    Key partitionKey = new Key(COL_NAME1, pKey);

    // Act
    Delete delete = prepareDelete(pKey, cKey);
    assertThatCode(() -> storage.delete(delete)).doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(new Scan(partitionKey));
    assertScanResultWithoutOrdering(results, pKey, COL_NAME4, Arrays.asList(cKey + 1, cKey + 2));
  }

  @Test
  public void delete_MultipleDeleteWithDifferentConditionsGiven_ShouldDeleteProperly()
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
                    COL_NAME2, new TextValue("1"), ConditionalExpression.Operator.EQ)));

    // Act
    assertThatCode(
            () -> storage.delete(Arrays.asList(deletes.get(0), deletes.get(1), deletes.get(2))))
        .doesNotThrowAnyException();

    // Assert
    List<Result> results = scanAll(new Scan(new Key(COL_NAME1, 0)));
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  public void delete_DeleteWithIfGivenWhenSuchRecordExists_ShouldDeleteProperly()
      throws ExecutionException {
    // Arrange
    populateRecords();
    int pKey = 0;
    int cKey = 0;
    Key partitionKey = new Key(COL_NAME1, pKey);
    Key clusteringKey = new Key(COL_NAME4, cKey);

    // Act
    Delete delete = prepareDelete(pKey, cKey);
    delete.withCondition(
        new DeleteIf(
            new ConditionalExpression(
                COL_NAME2,
                new TextValue(Integer.toString(pKey)),
                ConditionalExpression.Operator.EQ)));
    assertThatCode(() -> storage.delete(delete)).doesNotThrowAnyException();

    // Assert
    Optional<Result> actual = storage.get(new Get(partitionKey, clusteringKey));
    assertThat(actual.isPresent()).isFalse();
  }

  @Test
  public void get_GetGivenForIndexedColumn_ShouldGet() throws ExecutionException {
    // Arrange
    storage.put(preparePuts().get(0)); // (0,0)
    int c3 = 0;
    Get get = new Get(new Key(COL_NAME3, c3));

    // Act
    Optional<Result> actual = storage.get(get);

    // Assert
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME1)).isEqualTo(Optional.of(new IntValue(COL_NAME1, 0)));
    assertThat(actual.get().getValue(COL_NAME4)).isEqualTo(Optional.of(new IntValue(COL_NAME4, 0)));
  }

  @Test
  public void scan_ScanGivenForIndexedColumn_ShouldScan() throws ExecutionException, IOException {
    // Arrange
    populateRecords();
    int c3 = 3;
    Scan scan = new Scan(new Key(COL_NAME3, c3));

    // Act
    List<Result> actual = scanAll(scan);

    // Assert
    assertThat(actual.size()).isEqualTo(3); // (1,2), (2,1), (3,0)
    List<List<Integer>> expectedValues =
        new ArrayList<>(
            Arrays.asList(Arrays.asList(1, 2), Arrays.asList(2, 1), Arrays.asList(3, 0)));
    for (Result result : actual) {
      assertThat(result.getValue(COL_NAME1).isPresent()).isTrue();
      assertThat(result.getValue(COL_NAME4).isPresent()).isTrue();

      int col1Val = result.getValue(COL_NAME1).get().getAsInt();
      int col4Val = result.getValue(COL_NAME4).get().getAsInt();
      List<Integer> col1AndCol4 = Arrays.asList(col1Val, col4Val);
      assertThat(expectedValues).contains(col1AndCol4);
      expectedValues.remove(col1AndCol4);
    }

    assertThat(expectedValues).isEmpty();
  }

  private void populateRecords() {
    List<Put> puts = preparePuts();
    puts.forEach(p -> assertThatCode(() -> storage.put(p)).doesNotThrowAnyException());
  }

  private Get prepareGet(int pKey, int cKey) {
    Key partitionKey = new Key(COL_NAME1, pKey);
    Key clusteringKey = new Key(COL_NAME4, cKey);
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
                          Key partitionKey = new Key(COL_NAME1, i);
                          Key clusteringKey = new Key(COL_NAME4, j);
                          Put put =
                              new Put(partitionKey, clusteringKey)
                                  .withValue(COL_NAME2, Integer.toString(i + j))
                                  .withValue(COL_NAME3, i + j)
                                  .withValue(COL_NAME5, j % 2 == 0);
                          puts.add(put);
                        }));

    return puts;
  }

  private Delete prepareDelete(int pKey, int cKey) {
    Key partitionKey = new Key(COL_NAME1, pKey);
    Key clusteringKey = new Key(COL_NAME4, cKey);
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
                          Key partitionKey = new Key(COL_NAME1, i);
                          Key clusteringKey = new Key(COL_NAME4, j);
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

  private void assertScanResultWithoutOrdering(
      List<Result> actual,
      int expectedPartitionKeyValue,
      String checkedColumn,
      List<Integer> expectedValues) {
    Set<Integer> expectedValuesSet = new HashSet<>(expectedValues);
    assertThat(actual.size()).isEqualTo(expectedValues.size());

    for (Result result : actual) {
      assertThat(result.getValue(COL_NAME1))
          .isEqualTo(Optional.of(new IntValue(COL_NAME1, expectedPartitionKeyValue)));
      assertThat(result.getValue(checkedColumn).isPresent()).isTrue();

      int actualClusteringKeyValue = result.getValue(checkedColumn).get().getAsInt();
      assertThat(expectedValuesSet).contains(actualClusteringKeyValue);
      expectedValuesSet.remove(actualClusteringKeyValue);
    }

    assertThat(expectedValuesSet).isEmpty();
  }
}
