package com.scalar.db.api;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.ScanBuilder.BuildableScanAll;
import com.scalar.db.api.ScanBuilder.BuildableScanAllWithOngoingWhereAnd;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.util.ScalarDbUtils;
import com.scalar.db.util.TestUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DistributedStorageCrossPartitionScanIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(DistributedStorageCrossPartitionScanIntegrationTestBase.class);

  private static final String TEST_NAME = "storage_cross_partition_scan";
  private static final String NAMESPACE_BASE_NAME = "int_test_" + TEST_NAME + "_";
  private static final String CONDITION_TEST_TABLE = "condition_test_table";
  private static final String PARTITION_KEY_NAME = "pk";
  private static final String COL_NAME1 = "c1";
  private static final String COL_NAME2 = "c2";
  private static final String COL_NAME3 = "c3";
  private static final String COL_NAME4 = "c4";
  private static final String COL_NAME5 = "c5";
  private static final String COL_NAME6 = "c6";
  private static final String COL_NAME7 = "c7";
  private static final int CONDITION_TEST_TABLE_NUM_ROWS = 3;
  private static final int CONDITION_TEST_PREDICATE_VALUE = 2;
  private static final int FIRST_COLUMN_CARDINALITY = 5;
  private static final int SECOND_COLUMN_CARDINALITY_PER_SAME_FIRST_COLUMN = 20;

  private static final int THREAD_NUM = 10;

  private ExecutorService executorService;
  private long seed;
  private ThreadLocal<Random> random;

  private DistributedStorage storage;
  private DistributedStorageAdmin admin;
  private String namespaceBaseName;

  // Key: firstColumnType, Value: secondColumnType
  private ListMultimap<DataType, DataType> columnTypes;

  @BeforeAll
  public void beforeAll() throws Exception {
    executorService = Executors.newFixedThreadPool(getThreadNum());
    StorageFactory factory = StorageFactory.create(getProperties(TEST_NAME));
    admin = factory.getStorageAdmin();
    storage = factory.getStorage();
    namespaceBaseName = getNamespaceBaseName();
    columnTypes = getColumnTypes();
    createTableForConditionTests();
    createTablesForOrderingTests();
    seed = System.currentTimeMillis();
    System.out.println("The seed used in the cross-partition scan integration test is " + seed);
    random = ThreadLocal.withInitial(Random::new);
  }

  protected abstract Properties getProperties(String testName);

  protected String getNamespaceBaseName() {
    return NAMESPACE_BASE_NAME;
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  protected int getThreadNum() {
    return THREAD_NUM;
  }

  protected Column<?> getRandomColumn(Random random, String columnName, DataType dataType) {
    return ScalarDbUtils.toColumn(TestUtils.getRandomValue(random, columnName, dataType));
  }

  private String getNamespaceName() {
    return namespaceBaseName;
  }

  private String getNamespaceName(DataType firstColumnType) {
    return namespaceBaseName + firstColumnType;
  }

  protected boolean isParallelDdlSupported() {
    return true;
  }

  private void createTableForConditionTests() throws ExecutionException {
    Map<String, String> options = getCreationOptions();
    admin.createNamespace(getNamespaceName(), true, options);
    admin.createTable(
        getNamespaceName(),
        CONDITION_TEST_TABLE,
        TableMetadata.newBuilder()
            .addColumn(PARTITION_KEY_NAME, DataType.INT)
            .addColumn(COL_NAME1, DataType.INT)
            .addColumn(COL_NAME2, DataType.BIGINT)
            .addColumn(COL_NAME3, DataType.FLOAT)
            .addColumn(COL_NAME4, DataType.DOUBLE)
            .addColumn(COL_NAME5, DataType.TEXT)
            .addColumn(COL_NAME6, DataType.BOOLEAN)
            .addColumn(COL_NAME7, DataType.BLOB)
            .addPartitionKey(PARTITION_KEY_NAME)
            .build(),
        true,
        options);
  }

  private void createTablesForOrderingTests()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    List<Callable<Void>> testCallables = new ArrayList<>();

    Map<String, String> options = getCreationOptions();
    for (DataType firstColumnType : columnTypes.keySet()) {
      Callable<Void> testCallable =
          () -> {
            admin.createNamespace(getNamespaceName(firstColumnType), true, options);
            for (DataType secondColumnType : columnTypes.get(firstColumnType)) {
              createTable(firstColumnType, secondColumnType, options);
            }
            return null;
          };
      testCallables.add(testCallable);
    }

    executeDdls(testCallables);
  }

  private void createTable(
      DataType firstColumnType, DataType secondColumnType, Map<String, String> options)
      throws ExecutionException {
    admin.createTable(
        getNamespaceName(firstColumnType),
        getTableName(firstColumnType, secondColumnType),
        TableMetadata.newBuilder()
            .addColumn(PARTITION_KEY_NAME, DataType.INT)
            .addColumn(COL_NAME1, firstColumnType)
            .addColumn(COL_NAME2, secondColumnType)
            .addPartitionKey(PARTITION_KEY_NAME)
            .build(),
        true,
        options);
  }

  private String getTableName(DataType firstColumnType, DataType secondColumnType) {
    return String.join("_", firstColumnType.toString(), secondColumnType.toString());
  }

  private ListMultimap<DataType, DataType> getColumnTypes() {
    ListMultimap<DataType, DataType> columnTypes = ArrayListMultimap.create();
    for (DataType firstColumnType : DataType.values()) {
      for (DataType secondColumnType : DataType.values()) {
        columnTypes.put(firstColumnType, secondColumnType);
      }
    }
    return columnTypes;
  }

  private void prepareRecords() {
    IntStream.range(1, CONDITION_TEST_TABLE_NUM_ROWS + 1)
        .forEach(
            i -> {
              Key key = Key.ofInt(PARTITION_KEY_NAME, i);
              try {
                storage.put(preparePut(key, prepareNonKeyColumns(i)));
              } catch (ExecutionException e) {
                throw new RuntimeException("Put data to database failed", e);
              }
            });
  }

  private void prepareNullRecords() {
    IntStream.range(1, CONDITION_TEST_TABLE_NUM_ROWS + 1)
        .forEach(
            i -> {
              Key key = Key.ofInt(PARTITION_KEY_NAME, i);
              try {
                storage.put(
                    preparePut(key, i % 2 == 0 ? prepareNullColumns() : prepareNonKeyColumns(i)));
              } catch (ExecutionException e) {
                throw new RuntimeException("Put data to database failed", e);
              }
            });
  }

  private List<Tuple> prepareRecords(DataType firstColumnType, DataType secondColumnType)
      throws ExecutionException {
    List<Tuple> ret = new ArrayList<>();
    List<Put> puts = new ArrayList<>();

    if (firstColumnType == DataType.BOOLEAN) {
      TestUtils.booleanValues(COL_NAME1).stream()
          .map(ScalarDbUtils::toColumn)
          .forEach(
              firstColumn ->
                  prepareRecords(firstColumnType, firstColumn, secondColumnType, puts, ret));
    } else {
      Set<Column<?>> columnSet = new HashSet<>();
      IntStream.range(0, FIRST_COLUMN_CARDINALITY)
          .forEach(
              i -> {
                Column<?> firstColumn;
                while (true) {
                  firstColumn = getRandomColumn(random.get(), COL_NAME1, firstColumnType);
                  // reject duplication
                  if (!columnSet.contains(firstColumn)) {
                    columnSet.add(firstColumn);
                    break;
                  }
                }

                prepareRecords(firstColumnType, firstColumn, secondColumnType, puts, ret);
              });
    }
    try {
      for (Put put : puts) {
        storage.put(put);
      }
    } catch (ExecutionException e) {
      throw new ExecutionException("Put data to database failed", e);
    }
    return ret;
  }

  private void prepareRecords(
      DataType firstColumnType,
      Column<?> firstColumn,
      DataType secondColumnType,
      List<Put> puts,
      List<Tuple> ret) {
    if (secondColumnType == DataType.BOOLEAN) {
      TestUtils.booleanValues(COL_NAME2).stream()
          .map(ScalarDbUtils::toColumn)
          .forEach(
              secondColumn -> {
                ret.add(new Tuple(firstColumn, secondColumn));
                puts.add(
                    preparePut(
                        Key.ofInt(PARTITION_KEY_NAME, ret.size()),
                        firstColumnType,
                        firstColumn,
                        secondColumnType,
                        secondColumn));
              });
    } else {
      Set<Column<?>> columnSet = new HashSet<>();
      for (int i = 0; i < SECOND_COLUMN_CARDINALITY_PER_SAME_FIRST_COLUMN; i++) {
        Column<?> secondColumn;
        while (true) {
          secondColumn = getRandomColumn(random.get(), COL_NAME2, secondColumnType);
          // reject duplication
          if (!columnSet.contains(secondColumn)) {
            columnSet.add(secondColumn);
            break;
          }
        }

        ret.add(new Tuple(firstColumn, secondColumn));
        puts.add(
            preparePut(
                Key.ofInt(PARTITION_KEY_NAME, ret.size()),
                firstColumnType,
                firstColumn,
                secondColumnType,
                secondColumn));
      }
    }
  }

  private Put preparePut(Key key, List<Column<?>> columns) {
    PutBuilder.Buildable builder =
        Put.newBuilder()
            .namespace(getNamespaceName())
            .table(CONDITION_TEST_TABLE)
            .partitionKey(key);
    columns.forEach(builder::value);
    return builder.build();
  }

  private Put preparePut(
      Key key,
      DataType firstColumnType,
      Column<?> firstColumn,
      DataType secondColumnType,
      Column<?> secondColumn) {
    return Put.newBuilder()
        .namespace(getNamespaceName(firstColumnType))
        .table(getTableName(firstColumnType, secondColumnType))
        .partitionKey(key)
        .value(firstColumn)
        .value(secondColumn)
        .build();
  }

  private Put preparePut(int key, String text) {
    return Put.newBuilder()
        .namespace(getNamespaceName())
        .table(CONDITION_TEST_TABLE)
        .partitionKey(Key.ofInt(PARTITION_KEY_NAME, key))
        .textValue(COL_NAME5, text)
        .build();
  }

  private List<Column<?>> prepareNonKeyColumns(int i) {
    List<Column<?>> columns = new ArrayList<>();
    columns.add(IntColumn.of(COL_NAME1, i));
    columns.add(BigIntColumn.of(COL_NAME2, i));
    columns.add(FloatColumn.of(COL_NAME3, i));
    columns.add(DoubleColumn.of(COL_NAME4, i));
    columns.add(TextColumn.of(COL_NAME5, String.valueOf(i)));
    columns.add(BooleanColumn.of(COL_NAME6, i % 2 == 0));
    columns.add(BlobColumn.of(COL_NAME7, String.valueOf(i).getBytes(StandardCharsets.UTF_8)));
    return columns;
  }

  private List<Column<?>> prepareNullColumns() {
    List<Column<?>> columns = new ArrayList<>();
    columns.add(IntColumn.ofNull(COL_NAME1));
    columns.add(BigIntColumn.ofNull(COL_NAME2));
    columns.add(FloatColumn.ofNull(COL_NAME3));
    columns.add(DoubleColumn.ofNull(COL_NAME4));
    columns.add(TextColumn.ofNull(COL_NAME5));
    columns.add(BooleanColumn.ofNull(COL_NAME6));
    columns.add(BlobColumn.ofNull(COL_NAME7));
    return columns;
  }

  private List<Column<?>> prepareAllColumns(int value) {
    List<Column<?>> columns = prepareNonKeyColumns(value);
    columns.add(0, IntColumn.of(PARTITION_KEY_NAME, value));
    // columns.add(0, IntColumn.of(COL_NAME1, value));
    return columns;
  }

  private OrConditionSet prepareOrConditionSet(List<Column<?>> columns) {
    return ConditionSetBuilder.orConditionSet(
            columns.stream()
                .map(column -> ConditionBuilder.buildConditionalExpression(column, Operator.EQ))
                .collect(Collectors.toSet()))
        .build();
  }

  private AndConditionSet prepareAndConditionSet(List<Column<?>> columns) {
    return ConditionSetBuilder.andConditionSet(
            columns.stream()
                .map(column -> ConditionBuilder.buildConditionalExpression(column, Operator.EQ))
                .collect(Collectors.toSet()))
        .build();
  }

  private List<Integer> getExpectedResults(
      DataType type, ConditionalExpression.Operator operator, int value) {
    List<Integer> expected = new ArrayList<>();

    if (type.equals(DataType.BOOLEAN)) {
      return getExpectedResultsForBoolean(operator, value % 2 == 0);
    }

    switch (operator) {
      case EQ:
        for (int i = 1; i <= CONDITION_TEST_TABLE_NUM_ROWS; i++) {
          if (i == value) {
            expected.add(i);
          }
        }
        break;
      case NE:
        for (int i = 1; i <= CONDITION_TEST_TABLE_NUM_ROWS; i++) {
          if (i != value) {
            expected.add(i);
          }
        }
        break;
      case GT:
        for (int i = value + 1; i <= CONDITION_TEST_TABLE_NUM_ROWS; i++) {
          expected.add(i);
        }
        break;
      case GTE:
        for (int i = value; i <= CONDITION_TEST_TABLE_NUM_ROWS; i++) {
          expected.add(i);
        }
        break;
      case LT:
        for (int i = 1; i < value; i++) {
          expected.add(i);
        }
        break;
      case LTE:
        for (int i = 1; i <= value; i++) {
          expected.add(i);
        }
        break;
      default:
        break;
    }

    return expected;
  }

  private List<Integer> getExpectedResultsForBoolean(
      ConditionalExpression.Operator operator, boolean isEven) {
    List<Integer> expected = new ArrayList<>();

    switch (operator) {
      case EQ:
        for (int i = 1; i <= CONDITION_TEST_TABLE_NUM_ROWS; i++) {
          if ((isEven && i % 2 == 0) || (!isEven && i % 2 != 0)) {
            expected.add(i);
          }
        }
        break;
      case NE:
        for (int i = 1; i <= CONDITION_TEST_TABLE_NUM_ROWS; i++) {
          if ((isEven && i % 2 != 0) || (!isEven && i % 2 == 0)) {
            expected.add(i);
          }
        }
        break;
      case GT:
        for (int i = 1; i <= CONDITION_TEST_TABLE_NUM_ROWS; i++) {
          if (!isEven && i % 2 == 0) {
            expected.add(i);
          }
        }
        break;
      case GTE:
        for (int i = 1; i <= CONDITION_TEST_TABLE_NUM_ROWS; i++) {
          if (!isEven || i % 2 == 0) {
            expected.add(i);
          }
        }
        break;
      case LT:
        for (int i = 1; i <= CONDITION_TEST_TABLE_NUM_ROWS; i++) {
          if (isEven && i % 2 != 0) {
            expected.add(i);
          }
        }
        break;
      case LTE:
        for (int i = 1; i <= CONDITION_TEST_TABLE_NUM_ROWS; i++) {
          if (isEven || i % 2 == 0) {
            expected.add(i);
          }
        }
        break;
      default:
        break;
    }

    return expected;
  }

  private List<Integer> getExpectedNullResults(Operator operator) {
    List<Integer> expected = new ArrayList<>();

    for (int i = 1; i <= CONDITION_TEST_TABLE_NUM_ROWS; i++) {
      if ((operator.equals(Operator.IS_NULL) && i % 2 == 0)
          || (operator.equals(Operator.IS_NOT_NULL) && i % 2 != 0)) {
        expected.add(i);
      }
    }

    return expected;
  }

  private List<Tuple> getExpectedOrderedResults(
      List<Tuple> tuples, Order firstColumnOrder, Order secondColumnOrder) {
    List<Tuple> ret = new ArrayList<>(tuples);
    ret.sort(getTupleComparator(firstColumnOrder, secondColumnOrder));
    return ret;
  }

  private int getLimit(boolean withLimit, List<Tuple> expected) {
    int limit = 0;
    if (withLimit && !expected.isEmpty()) {
      if (expected.size() == 1) {
        limit = 1;
      } else {
        limit = random.get().nextInt(expected.size() - 1) + 1;
      }
    }
    return limit;
  }

  private Ordering prepareOrdering(String column, Ordering.Order order) {
    return order.equals(Order.ASC) ? Ordering.asc(column) : Ordering.desc(column);
  }

  private Scan prepareScan(
      DataType firstColumnType,
      Order firstColumnOrder,
      DataType secondColumnType,
      Order secondColumnOrder,
      int limit) {
    BuildableScanAll builder =
        Scan.newBuilder()
            .namespace(getNamespaceName(firstColumnType))
            .table(getTableName(firstColumnType, secondColumnType))
            .all()
            .ordering(prepareOrdering(COL_NAME1, firstColumnOrder))
            .ordering(prepareOrdering(COL_NAME2, secondColumnOrder));
    if (limit > 0) {
      builder.limit(limit);
    }
    return builder.build();
  }

  private Scan prepareScanWithLike(boolean isLike, String pattern) {
    LikeExpression condition =
        isLike
            ? ConditionBuilder.column(COL_NAME5).isLikeText(pattern)
            : ConditionBuilder.column(COL_NAME5).isNotLikeText(pattern);
    return Scan.newBuilder()
        .namespace(getNamespaceName())
        .table(CONDITION_TEST_TABLE)
        .all()
        .where(condition)
        .build();
  }

  private Scan prepareScanWithLike(boolean isLike, String pattern, String escape) {
    LikeExpression condition =
        isLike
            ? ConditionBuilder.column(COL_NAME5).isLikeText(pattern, escape)
            : ConditionBuilder.column(COL_NAME5).isNotLikeText(pattern, escape);
    return Scan.newBuilder()
        .namespace(getNamespaceName())
        .table(CONDITION_TEST_TABLE)
        .all()
        .where(condition)
        .build();
  }

  private void assertScanResult(
      List<Result> actualResults, List<Integer> expected, String description) {
    List<Integer> actual = new ArrayList<>();
    for (Result actualResult : actualResults) {
      actual.add(actualResult.getInt(PARTITION_KEY_NAME));
    }
    assertThat(actual).describedAs(description).containsExactlyInAnyOrderElementsOf(expected);
  }

  private void assertOrderedScanResult(
      List<Result> actualResults, List<Tuple> expected, String description) {
    List<Tuple> actual = new ArrayList<>();
    for (Result actualResult : actualResults) {
      assertThat(actualResult.getColumns().containsKey(COL_NAME1)).isTrue();
      assertThat(actualResult.getColumns().containsKey(COL_NAME2)).isTrue();
      actual.add(
          new Tuple(
              actualResult.getColumns().get(COL_NAME1), actualResult.getColumns().get(COL_NAME2)));
    }
    assertThat(actual).describedAs(description).isEqualTo(expected);
  }

  private void assertLimitedScanResult(
      List<Result> actualResults, List<Integer> expected, int limit, String description) {
    List<Integer> actual = new ArrayList<>();
    for (Result actualResult : actualResults) {
      actual.add(actualResult.getInt(PARTITION_KEY_NAME));
    }
    assertThat(actual.size()).describedAs(description).isLessThanOrEqualTo(limit);
    assertThat(actual).describedAs(description).containsAnyElementsOf(expected);
  }

  private void assertProjectedResult(
      List<Result> actualResults,
      List<Integer> expected,
      List<String> projections,
      String description) {
    List<Integer> actual = new ArrayList<>();
    for (Result actualResult : actualResults) {
      assertThat(actualResult.getContainedColumnNames())
          .containsExactlyInAnyOrderElementsOf(projections);
      actual.add(actualResult.getInt(PARTITION_KEY_NAME));
    }
    assertThat(actual).describedAs(description).containsExactlyInAnyOrderElementsOf(expected);
  }

  private String description(Column<?> column, Operator operator) {
    return String.format("failed with column: %s, operator: %s", column, operator);
  }

  private String description(Column<?> column, Operator operator, int value) {
    return description(column, operator) + String.format(", value: %s", value);
  }

  private String descriptionForLimitTests(Column<?> column, Operator operator, int limit) {
    return description(column, operator) + String.format(", limit: %s", limit);
  }

  private String description(
      DataType firstColumnType,
      Order firstColumnOrder,
      DataType secondColumnType,
      Order secondColumnOrder,
      boolean withLimit) {
    return String.format(
        "failed with firstColumnType: %s, firstColumnOrder: %s"
            + ", secondColumnType: %s, secondColumnOrder: %s, withLimit: %b",
        firstColumnType, firstColumnOrder, secondColumnType, secondColumnOrder, withLimit);
  }

  private List<Result> scanAll(Scan scan) throws ExecutionException, IOException {
    try (Scanner scanner = storage.scan(scan)) {
      return scanner.all();
    }
  }

  @BeforeEach
  public void setUp() throws Exception {
    truncateTable();
  }

  private void truncateTable() throws ExecutionException {
    admin.truncateTable(getNamespaceName(), CONDITION_TEST_TABLE);
  }

  private void truncateTable(DataType firstColumnType, DataType secondColumnType)
      throws ExecutionException {
    admin.truncateTable(
        getNamespaceName(firstColumnType), getTableName(firstColumnType, secondColumnType));
  }

  @AfterAll
  public void afterAll() throws Exception {
    try {
      dropTablesForOrderingTests();
    } catch (Exception e) {
      logger.warn("Failed to drop tables for ordering tests", e);
    }

    try {
      dropTableForConditionTests();
    } catch (Exception e) {
      logger.warn("Failed to drop tables for condition tests", e);
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

    try {
      if (executorService != null) {
        executorService.shutdown();
      }
    } catch (Exception e) {
      logger.warn("Failed to shutdown executor service", e);
    }
  }

  private void dropTableForConditionTests() throws ExecutionException {
    admin.dropTable(getNamespaceName(), CONDITION_TEST_TABLE);
    admin.dropNamespace(getNamespaceName());
  }

  private void dropTablesForOrderingTests()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    List<Callable<Void>> testCallables = new ArrayList<>();
    for (DataType firstColumnType : columnTypes.keySet()) {
      Callable<Void> testCallable =
          () -> {
            for (DataType secondColumnType : columnTypes.get(firstColumnType)) {
              admin.dropTable(
                  getNamespaceName(firstColumnType),
                  getTableName(firstColumnType, secondColumnType));
            }
            admin.dropNamespace(getNamespaceName(firstColumnType));
            return null;
          };
      testCallables.add(testCallable);
    }

    executeDdls(testCallables);
  }

  @Test
  public void scan_WithCondition_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    prepareRecords();

    List<Callable<Void>> testCallables = new ArrayList<>();
    ImmutableList.of(Operator.EQ, Operator.NE, Operator.GT, Operator.GTE, Operator.LT, Operator.LTE)
        .forEach(
            operator -> {
              for (Column<?> column : prepareAllColumns(CONDITION_TEST_PREDICATE_VALUE)) {
                testCallables.add(
                    () -> {
                      scan_WithCondition_ShouldReturnProperResult(
                          column, operator, CONDITION_TEST_PREDICATE_VALUE);
                      return null;
                    });
              }
            });

    executeInParallel(testCallables);
  }

  @Test
  public void scan_WithNullCondition_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    prepareNullRecords();

    List<Callable<Void>> testCallables = new ArrayList<>();
    ImmutableList.of(Operator.IS_NULL, Operator.IS_NOT_NULL)
        .forEach(
            operator -> {
              for (Column<?> column : prepareNullColumns()) {
                testCallables.add(
                    () -> {
                      scan_WithNullCondition_ShouldReturnProperResult(column, operator);
                      return null;
                    });
              }
            });

    executeInParallel(testCallables);
  }

  private void scan_WithCondition_ShouldReturnProperResult(
      Column<?> column, Operator operator, int value) throws IOException, ExecutionException {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace(getNamespaceName())
            .table(CONDITION_TEST_TABLE)
            .all()
            .where(ConditionBuilder.buildConditionalExpression(column, operator))
            .build();

    // Act
    List<Result> actual = scanAll(scan);

    // Assert
    assertScanResult(
        actual,
        getExpectedResults(column.getDataType(), operator, value),
        description(column, operator, value));
  }

  private void scan_WithNullCondition_ShouldReturnProperResult(Column<?> column, Operator operator)
      throws IOException, ExecutionException {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace(getNamespaceName())
            .table(CONDITION_TEST_TABLE)
            .all()
            .where(ConditionBuilder.buildConditionalExpression(column, operator))
            .build();

    // Act
    List<Result> actual = scanAll(scan);

    // Assert
    assertScanResult(actual, getExpectedNullResults(operator), description(column, operator));
  }

  @Test
  public void scan_WithConjunctiveNormalFormConditionsShouldReturnProperResult()
      throws IOException, ExecutionException {
    // Arrange
    prepareRecords();
    BuildableScanAllWithOngoingWhereAnd builder =
        Scan.newBuilder()
            .namespace(getNamespaceName())
            .table(CONDITION_TEST_TABLE)
            .all()
            .where(
                prepareOrConditionSet(
                    ImmutableList.of(
                        IntColumn.of(PARTITION_KEY_NAME, 1), IntColumn.of(PARTITION_KEY_NAME, 2))));
    List<Column<?>> columns1 = prepareNonKeyColumns(1);
    List<Column<?>> columns2 = prepareNonKeyColumns(2);
    List<OrConditionSet> orConditionSets =
        IntStream.range(0, columns1.size())
            .boxed()
            .map(i -> prepareOrConditionSet(ImmutableList.of(columns1.get(i), columns2.get(i))))
            .collect(Collectors.toList());
    orConditionSets.forEach(builder::and);

    // Act
    List<Result> actual = scanAll(builder.build());

    // Assert
    assertScanResult(
        actual,
        getExpectedResults(DataType.INT, Operator.LTE, CONDITION_TEST_PREDICATE_VALUE),
        "failed with CNF conditions");
  }

  @Test
  public void scan_WithDisjunctiveNormalFormConditionsShouldReturnProperResult()
      throws IOException, ExecutionException {
    // Arrange
    prepareRecords();
    Scan scan =
        Scan.newBuilder()
            .namespace(getNamespaceName())
            .table(CONDITION_TEST_TABLE)
            .all()
            .where(prepareAndConditionSet(prepareNonKeyColumns(1)))
            .or(prepareAndConditionSet(prepareNonKeyColumns(2)))
            .build();

    // Act
    List<Result> actual = scanAll(scan);

    // Assert
    assertScanResult(
        actual,
        getExpectedResults(DataType.INT, Operator.LTE, CONDITION_TEST_PREDICATE_VALUE),
        "failed with DNF conditions");
  }

  @Test
  public void scan_WithOrderingForNonPrimaryColumns_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {

    List<Callable<Void>> testCallables = new ArrayList<>();
    for (DataType firstColumnType : columnTypes.keySet()) {
      for (DataType secondColumnType : columnTypes.get(firstColumnType)) {
        testCallables.add(
            () -> {
              random.get().setSeed(seed);
              truncateTable(firstColumnType, secondColumnType);
              List<Tuple> tuples = prepareRecords(firstColumnType, secondColumnType);
              for (Order firstColumnOrder : Order.values()) {
                for (Order secondColumnOrder : Order.values()) {
                  for (boolean withLimit : Arrays.asList(false, true)) {
                    scan_WithOrderingForNonPrimaryColumns_ShouldReturnProperResult(
                        tuples,
                        firstColumnType,
                        firstColumnOrder,
                        secondColumnType,
                        secondColumnOrder,
                        withLimit);
                  }
                }
              }
              return null;
            });
      }
    }

    executeInParallel(testCallables);
  }

  private void scan_WithOrderingForNonPrimaryColumns_ShouldReturnProperResult(
      List<Tuple> tuples,
      DataType firstColumnType,
      Order firstColumnOrder,
      DataType secondColumnType,
      Order secondColumnOrder,
      boolean withLimit)
      throws IOException, ExecutionException {
    // Arrange
    List<Tuple> expected = getExpectedOrderedResults(tuples, firstColumnOrder, secondColumnOrder);
    int limit = getLimit(withLimit, expected);
    if (limit > 0) {
      expected = expected.subList(0, limit);
    }
    Scan scan =
        prepareScan(firstColumnType, firstColumnOrder, secondColumnType, secondColumnOrder, limit);

    // Act
    List<Result> actual = scanAll(scan);

    // Assert
    assertOrderedScanResult(
        actual,
        expected,
        description(
            firstColumnType, firstColumnOrder, secondColumnType, secondColumnOrder, withLimit));
  }

  @Test
  public void scan_WithLikeCondition_ShouldReturnProperResult()
      throws ExecutionException, java.util.concurrent.ExecutionException, InterruptedException {
    // Arrange
    storage.put(preparePut(1, "@scalar[$]"));
    storage.put(preparePut(2, "@@scalar[$]"));
    storage.put(preparePut(3, "%scalar[$]"));
    storage.put(preparePut(4, "_scalar[$]"));
    storage.put(preparePut(5, "\\scalar[$]"));
    storage.put(preparePut(6, "\\\\scalar[$]"));
    storage.put(preparePut(7, "scalar$"));

    List<Callable<Void>> testCallables = new ArrayList<>();
    testCallables.add(
        () -> {
          scan_WithLikeCondition_ShouldReturnProperResult(
              prepareScanWithLike(true, "%scalar[$]"), ImmutableList.of(1, 2, 3, 4, 5, 6), "all");
          return null;
        });
    testCallables.add(
        () -> {
          scan_WithLikeCondition_ShouldReturnProperResult(
              prepareScanWithLike(true, "_scalar[$]"),
              ImmutableList.of(1, 3, 4, 5),
              "single character");
          return null;
        });
    testCallables.add(
        () -> {
          scan_WithLikeCondition_ShouldReturnProperResult(
              prepareScanWithLike(true, "\\%scalar[$]"),
              ImmutableList.of(3),
              "escape % with default escape");
          return null;
        });
    testCallables.add(
        () -> {
          scan_WithLikeCondition_ShouldReturnProperResult(
              prepareScanWithLike(true, "\\_scalar[$]"),
              ImmutableList.of(4),
              "escape _ with default escape");
          return null;
        });
    testCallables.add(
        () -> {
          scan_WithLikeCondition_ShouldReturnProperResult(
              prepareScanWithLike(true, "+%scalar[$]", "+"),
              ImmutableList.of(3),
              "escape % with specified escape");
          return null;
        });
    testCallables.add(
        () -> {
          scan_WithLikeCondition_ShouldReturnProperResult(
              prepareScanWithLike(true, "+_scalar[$]", "+"),
              ImmutableList.of(4),
              "escape _ with specified escape");
          return null;
        });
    testCallables.add(
        () -> {
          scan_WithLikeCondition_ShouldReturnProperResult(
              prepareScanWithLike(true, "\\%scalar[$]", ""),
              ImmutableList.of(5, 6),
              "no escape character");
          return null;
        });
    testCallables.add(
        () -> {
          scan_WithLikeCondition_ShouldReturnProperResult(
              prepareScanWithLike(true, "\\_scalar[$]", ""),
              ImmutableList.of(6),
              "no escape character");
          return null;
        });
    testCallables.add(
        () -> {
          scan_WithLikeCondition_ShouldReturnProperResult(
              prepareScanWithLike(false, "\\%scalar[$]"),
              ImmutableList.of(1, 2, 4, 5, 6, 7),
              "not like and escape % with default escape");
          return null;
        });
    testCallables.add(
        () -> {
          scan_WithLikeCondition_ShouldReturnProperResult(
              prepareScanWithLike(false, "\\_scalar[$]"),
              ImmutableList.of(1, 2, 3, 5, 6, 7),
              "not like and escape _ with default escape");
          return null;
        });
    testCallables.add(
        () -> {
          scan_WithLikeCondition_ShouldReturnProperResult(
              prepareScanWithLike(false, "+%scalar[$]", "+"),
              ImmutableList.of(1, 2, 4, 5, 6, 7),
              "not like and escape % with specified escape");
          return null;
        });
    testCallables.add(
        () -> {
          scan_WithLikeCondition_ShouldReturnProperResult(
              prepareScanWithLike(false, "\\_scalar[$]", ""),
              ImmutableList.of(1, 2, 3, 4, 5, 7),
              "not like with no escape character");
          return null;
        });

    executeInParallel(testCallables);
  }

  private void scan_WithLikeCondition_ShouldReturnProperResult(
      Scan scan, List<Integer> expected, String description)
      throws IOException, ExecutionException {
    // Arrange Act
    List<Result> actual = scanAll(scan);

    // Assert
    assertScanResult(actual, expected, description);
  }

  @Test
  public void scan_WithConditionAndLimit_ShouldReturnProperResult()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    prepareRecords();

    List<Callable<Void>> testCallables = new ArrayList<>();
    IntStream.range(1, CONDITION_TEST_TABLE_NUM_ROWS + 1)
        .forEach(
            limit -> {
              testCallables.add(
                  () -> {
                    scan_WithConditionAndLimit_ShouldReturnProperResult(limit);
                    return null;
                  });
            });

    executeInParallel(testCallables);
  }

  private void scan_WithConditionAndLimit_ShouldReturnProperResult(int limit)
      throws IOException, ExecutionException {
    // Arrange
    IntColumn column = IntColumn.of(COL_NAME1, CONDITION_TEST_PREDICATE_VALUE);
    Scan scan =
        Scan.newBuilder()
            .namespace(getNamespaceName())
            .table(CONDITION_TEST_TABLE)
            .all()
            .where(ConditionBuilder.buildConditionalExpression(column, Operator.LTE))
            .limit(limit)
            .build();

    // Act
    List<Result> actual = scanAll(scan);

    // Assert
    assertLimitedScanResult(
        actual,
        // expected full (not-limited) results
        getExpectedResults(column.getDataType(), Operator.LTE, column.getIntValue()),
        limit,
        descriptionForLimitTests(column, Operator.LTE, limit));
  }

  @Test
  public void scan_WithConditionButColumnsNotAppearedInProjections_ShouldReturnProjectedResult()
      throws IOException, ExecutionException {
    prepareRecords();
    // Arrange
    IntColumn column = IntColumn.of(COL_NAME1, CONDITION_TEST_PREDICATE_VALUE);
    Scan scan =
        Scan.newBuilder()
            .namespace(getNamespaceName())
            .table(CONDITION_TEST_TABLE)
            .all()
            .projections(PARTITION_KEY_NAME, COL_NAME2)
            .where(ConditionBuilder.buildConditionalExpression(column, Operator.LTE))
            .build();

    // Act
    List<Result> actual = scanAll(scan);
    assertProjectedResult(
        actual,
        getExpectedResults(column.getDataType(), Operator.LTE, column.getIntValue()),
        ImmutableList.of(PARTITION_KEY_NAME, COL_NAME2),
        description(column, Operator.LTE));
  }

  private void executeDdls(List<Callable<Void>> ddls)
      throws InterruptedException, java.util.concurrent.ExecutionException {
    if (isParallelDdlSupported()) {
      executeInParallel(ddls);
    } else {
      ddls.forEach(
          ddl -> {
            try {
              ddl.call();
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
    }
  }

  private void executeInParallel(List<Callable<Void>> testCallables)
      throws InterruptedException, java.util.concurrent.ExecutionException {
    List<Future<Void>> futures = executorService.invokeAll(testCallables);
    for (Future<Void> future : futures) {
      future.get();
    }
  }

  private Comparator<Tuple> getTupleComparator(Order firstColumnOrder, Order secondColumnOrder) {
    return (l, r) ->
        ComparisonChain.start()
            .compare(
                l.first,
                r.first,
                firstColumnOrder == Order.ASC
                    ? com.google.common.collect.Ordering.natural()
                    : com.google.common.collect.Ordering.natural().reverse())
            .compare(
                l.second,
                r.second,
                secondColumnOrder == Order.ASC
                    ? com.google.common.collect.Ordering.natural()
                    : com.google.common.collect.Ordering.natural().reverse())
            .result();
  }

  private static class Tuple implements Comparable<Tuple> {
    @Nonnull public final Column<?> first;
    @Nonnull public final Column<?> second;

    public Tuple(Column<?> first, Column<?> second) {
      checkNotNull(first);
      checkNotNull(second);
      this.first = first;
      this.second = second;
    }

    @Override
    public int compareTo(Tuple o) {
      return ComparisonChain.start().compare(first, o.first).compare(second, o.second).result();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Tuple)) {
        return false;
      }
      Tuple other = (Tuple) o;
      return first.equals(other.first) && second.equals(other.second);
    }

    @Override
    public int hashCode() {
      return Objects.hash(first, second);
    }

    @Override
    public String toString() {
      return "Tuple{" + "first=" + first + ", second=" + second + '}';
    }
  }
}
