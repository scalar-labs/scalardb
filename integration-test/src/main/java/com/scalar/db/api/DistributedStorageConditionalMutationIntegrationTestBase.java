package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
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
import com.scalar.db.util.TestUtils;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DistributedStorageConditionalMutationIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(DistributedStorageConditionalMutationIntegrationTestBase.class);

  private static final String TEST_NAME = "storage_cond_mutation";
  private static final String NAMESPACE = "int_test_" + TEST_NAME;
  private static final String TABLE = "test_table";
  private static final String PARTITION_KEY = "pkey";
  private static final String COL_NAME1 = "c1";
  private static final String COL_NAME2 = "c2";
  private static final String COL_NAME3 = "c3";
  private static final String COL_NAME4 = "c4";
  private static final String COL_NAME5 = "c5";
  private static final String COL_NAME6 = "c6";
  private static final String COL_NAME7 = "c7";

  private static final TableMetadata TABLE_METADATA =
      TableMetadata.newBuilder()
          .addColumn(PARTITION_KEY, DataType.TEXT)
          .addColumn(COL_NAME1, DataType.BOOLEAN)
          .addColumn(COL_NAME2, DataType.INT)
          .addColumn(COL_NAME3, DataType.BIGINT)
          .addColumn(COL_NAME4, DataType.FLOAT)
          .addColumn(COL_NAME5, DataType.DOUBLE)
          .addColumn(COL_NAME6, DataType.TEXT)
          .addColumn(COL_NAME7, DataType.BLOB)
          .addPartitionKey(PARTITION_KEY)
          .build();

  private static final int ATTEMPT_COUNT = 5;
  private static final int THREAD_NUM = 10;

  private DistributedStorageAdmin admin;
  private DistributedStorage storage;
  private String namespace;

  private long seed;
  private ThreadLocal<Random> random;

  private List<OperatorAndDataType> operatorAndDataTypeList;

  private ExecutorService executorService;

  @BeforeAll
  public void beforeAll() throws Exception {
    initialize(TEST_NAME);
    StorageFactory factory = StorageFactory.create(getProperties(TEST_NAME));
    admin = factory.getAdmin();
    namespace = getNamespace();
    createTable();
    storage = factory.getStorage();
    seed = System.currentTimeMillis();
    System.out.println("The seed used in the conditional mutation integration test is " + seed);
    random = ThreadLocal.withInitial(Random::new);
    operatorAndDataTypeList = getOperatorAndDataTypeListForTest();
    executorService = Executors.newFixedThreadPool(getThreadNum());
  }

  protected void initialize(String testName) throws Exception {}

  protected abstract Properties getProperties(String testName);

  protected String getNamespace() {
    return NAMESPACE;
  }

  protected int getThreadNum() {
    return THREAD_NUM;
  }

  private void createTable() throws ExecutionException {
    Map<String, String> options = getCreationOptions();
    admin.createNamespace(namespace, true, options);
    admin.createTable(namespace, TABLE, TABLE_METADATA, true, options);
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  @BeforeEach
  public void setUp() throws Exception {
    admin.truncateTable(namespace, TABLE);
  }

  @AfterAll
  public void afterAll() throws Exception {
    try {
      dropTable();
    } catch (Exception e) {
      logger.warn("Failed to drop table", e);
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
    admin.dropTable(namespace, TABLE);
    admin.dropNamespace(namespace);
  }

  protected List<OperatorAndDataType> getOperatorAndDataTypeListForTest() {
    List<OperatorAndDataType> ret = new ArrayList<>();
    for (Operator operator : Operator.values()) {
      if (operator != Operator.LIKE && operator != Operator.NOT_LIKE) {
        for (DataType dataType : DataType.values()) {
          ret.add(new OperatorAndDataType(operator, dataType));
        }
      }
    }
    return ret;
  }

  @Test
  public void put_withPutIfWithSingleCondition_shouldPutProperly()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    executeInParallel(
        (operator, dataType) -> {
          put_withPutIfWithSingleConditionWithSameValue_shouldPutProperly(operator, dataType);
          put_withPutIfWithSingleConditionWithRandomValue_shouldPutProperly(operator, dataType);
          put_withPutIfWithInitialDataWithNullValuesWithSingleCondition_shouldPutProperly(
              operator, dataType);
          put_withPutIfWithInitialDataWithoutValuesWithSingleCondition_shouldPutProperly(
              operator, dataType);
        });
  }

  private void put_withPutIfWithSingleConditionWithSameValue_shouldPutProperly(
      Operator operator, DataType dataType) throws ExecutionException {
    // Arrange
    Map<String, Column<?>> initialData = putInitialDataWithRandomValues(operator, dataType);

    String columnName = getColumnName(dataType);
    Column<?> columnToCompare = initialData.get(columnName);

    MutationCondition condition =
        ConditionBuilder.putIf(buildConditionalExpression(columnToCompare, operator)).build();
    Put put = preparePutWithRandomValues(operator, dataType).withCondition(condition);

    boolean shouldMutate = shouldMutate(initialData.get(columnName), columnToCompare, operator);

    // Act Assert
    put_withPutIf_shouldPutProperly(
        initialData,
        put,
        shouldMutate,
        prepareGet(operator, dataType),
        description(initialData, put, columnToCompare, operator));
  }

  private void put_withPutIfWithSingleConditionWithRandomValue_shouldPutProperly(
      Operator operator, DataType dataType) throws ExecutionException {
    for (int i = 0; i < ATTEMPT_COUNT; i++) {
      // Arrange
      Map<String, Column<?>> initialData = putInitialDataWithRandomValues(operator, dataType);

      String columnName = getColumnName(dataType);
      Column<?> columnToCompare = getColumnWithRandomValue(random.get(), columnName, dataType);

      MutationCondition condition =
          ConditionBuilder.putIf(buildConditionalExpression(columnToCompare, operator)).build();
      Put put = preparePutWithRandomValues(operator, dataType).withCondition(condition);

      boolean shouldMutate = shouldMutate(initialData.get(columnName), columnToCompare, operator);

      // Act Assert
      put_withPutIf_shouldPutProperly(
          initialData,
          put,
          shouldMutate,
          prepareGet(operator, dataType),
          description(initialData, put, columnToCompare, operator));
    }
  }

  private void put_withPutIfWithInitialDataWithNullValuesWithSingleCondition_shouldPutProperly(
      Operator operator, DataType dataType) throws ExecutionException {
    // Arrange
    Map<String, Column<?>> initialData = putInitialDataWithNullValues(operator, dataType);

    String columnName = getColumnName(dataType);
    Column<?> columnToCompare = getColumnWithRandomValue(random.get(), columnName, dataType);

    MutationCondition condition =
        ConditionBuilder.putIf(buildConditionalExpression(columnToCompare, operator)).build();
    Put put = preparePutWithRandomValues(operator, dataType).withCondition(condition);

    boolean shouldMutate = shouldMutate(initialData.get(columnName), columnToCompare, operator);

    // Act Assert
    put_withPutIf_shouldPutProperly(
        initialData,
        put,
        shouldMutate,
        prepareGet(operator, dataType),
        description(initialData, put, columnToCompare, operator));
  }

  private void put_withPutIfWithInitialDataWithoutValuesWithSingleCondition_shouldPutProperly(
      Operator operator, DataType dataType) throws ExecutionException {
    // Arrange
    Map<String, Column<?>> initialData = putInitialDataWithoutValues(operator, dataType);

    String columnName = getColumnName(dataType);
    Column<?> columnToCompare = getColumnWithRandomValue(random.get(), columnName, dataType);

    MutationCondition condition =
        ConditionBuilder.putIf(buildConditionalExpression(columnToCompare, operator)).build();
    Put put = preparePutWithRandomValues(operator, dataType).withCondition(condition);

    boolean shouldMutate = shouldMutate(initialData.get(columnName), columnToCompare, operator);

    // Act Assert
    put_withPutIf_shouldPutProperly(
        initialData,
        put,
        shouldMutate,
        prepareGet(operator, dataType),
        description(initialData, put, columnToCompare, operator));
  }

  @Test
  public void put_withPutIfWithMultipleConditions_shouldPutProperly()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    executeInParallel(
        (firstOperator, firstDataType, secondOperator, secondDataType) -> {
          put_withPutIfWithMultipleConditionsWithSameValue_shouldPutProperly(
              firstOperator, firstDataType, secondOperator, secondDataType);
          put_withPutIfWithMultipleConditionsWithRandomValue_shouldPutProperly(
              firstOperator, firstDataType, secondOperator, secondDataType);
          put_withPutIfWithInitialDataWithNullValuesWithMultipleConditions_shouldPutProperly(
              firstOperator, firstDataType, secondOperator, secondDataType);
          put_withPutIfWithInitialDataWithoutValuesWithMultipleConditions_shouldPutProperly(
              firstOperator, firstDataType, secondOperator, secondDataType);
        });
  }

  private void put_withPutIfWithMultipleConditionsWithSameValue_shouldPutProperly(
      Operator firstOperator,
      DataType firstDataType,
      Operator secondOperator,
      DataType secondDataType)
      throws ExecutionException {
    // Arrange
    Map<String, Column<?>> initialData =
        putInitialDataWithRandomValues(
            firstOperator, firstDataType, secondOperator, secondDataType);

    String firstColumnName = getColumnName(firstDataType);
    Column<?> firstColumnToCompare = initialData.get(firstColumnName);

    String secondColumnName = getColumnName(secondDataType);
    Column<?> secondColumnToCompare = initialData.get(secondColumnName);

    MutationCondition condition =
        ConditionBuilder.putIf(buildConditionalExpression(firstColumnToCompare, firstOperator))
            .and(buildConditionalExpression(secondColumnToCompare, secondOperator))
            .build();
    Put put =
        preparePutWithRandomValues(firstOperator, firstDataType, secondOperator, secondDataType)
            .withCondition(condition);

    boolean shouldMutate =
        shouldMutate(initialData.get(firstColumnName), firstColumnToCompare, firstOperator)
            && shouldMutate(
                initialData.get(secondColumnName), secondColumnToCompare, secondOperator);

    // Act Assert
    put_withPutIf_shouldPutProperly(
        initialData,
        put,
        shouldMutate,
        prepareGet(firstOperator, firstDataType, secondOperator, secondDataType),
        description(
            initialData,
            put,
            firstColumnToCompare,
            firstOperator,
            secondColumnToCompare,
            secondOperator));
  }

  private void put_withPutIfWithMultipleConditionsWithRandomValue_shouldPutProperly(
      Operator firstOperator,
      DataType firstDataType,
      Operator secondOperator,
      DataType secondDataType)
      throws ExecutionException {
    for (int i = 0; i < ATTEMPT_COUNT; i++) {
      // Arrange
      Map<String, Column<?>> initialData =
          putInitialDataWithRandomValues(
              firstOperator, firstDataType, secondOperator, secondDataType);

      String firstColumnName = getColumnName(firstDataType);
      Column<?> firstColumnToCompare =
          getColumnWithRandomValue(random.get(), firstColumnName, firstDataType);

      String secondColumnName = getColumnName(secondDataType);
      Column<?> secondColumnToCompare =
          getColumnWithRandomValue(random.get(), secondColumnName, secondDataType);

      MutationCondition condition =
          ConditionBuilder.putIf(buildConditionalExpression(firstColumnToCompare, firstOperator))
              .and(buildConditionalExpression(secondColumnToCompare, secondOperator))
              .build();
      Put put =
          preparePutWithRandomValues(firstOperator, firstDataType, secondOperator, secondDataType)
              .withCondition(condition);

      boolean shouldMutate =
          shouldMutate(initialData.get(firstColumnName), firstColumnToCompare, firstOperator)
              && shouldMutate(
                  initialData.get(secondColumnName), secondColumnToCompare, secondOperator);

      // Act Assert
      put_withPutIf_shouldPutProperly(
          initialData,
          put,
          shouldMutate,
          prepareGet(firstOperator, firstDataType, secondOperator, secondDataType),
          description(
              initialData,
              put,
              firstColumnToCompare,
              firstOperator,
              secondColumnToCompare,
              secondOperator));
    }
  }

  private void put_withPutIfWithInitialDataWithNullValuesWithMultipleConditions_shouldPutProperly(
      Operator firstOperator,
      DataType firstDataType,
      Operator secondOperator,
      DataType secondDataType)
      throws ExecutionException {
    // Arrange
    Map<String, Column<?>> initialData =
        putInitialDataWithNullValues(firstOperator, firstDataType, secondOperator, secondDataType);

    String firstColumnName = getColumnName(firstDataType);
    Column<?> firstColumnToCompare =
        getColumnWithRandomValue(random.get(), firstColumnName, firstDataType);

    String secondColumnName = getColumnName(secondDataType);
    Column<?> secondColumnToCompare =
        getColumnWithRandomValue(random.get(), secondColumnName, secondDataType);

    MutationCondition condition =
        ConditionBuilder.putIf(buildConditionalExpression(firstColumnToCompare, firstOperator))
            .and(buildConditionalExpression(secondColumnToCompare, secondOperator))
            .build();
    Put put =
        preparePutWithRandomValues(firstOperator, firstDataType, secondOperator, secondDataType)
            .withCondition(condition);

    boolean shouldMutate =
        shouldMutate(initialData.get(firstColumnName), firstColumnToCompare, firstOperator)
            && shouldMutate(
                initialData.get(secondColumnName), secondColumnToCompare, secondOperator);

    // Act Assert
    put_withPutIf_shouldPutProperly(
        initialData,
        put,
        shouldMutate,
        prepareGet(firstOperator, firstDataType, secondOperator, secondDataType),
        description(
            initialData,
            put,
            firstColumnToCompare,
            firstOperator,
            secondColumnToCompare,
            secondOperator));
  }

  private void put_withPutIfWithInitialDataWithoutValuesWithMultipleConditions_shouldPutProperly(
      Operator firstOperator,
      DataType firstDataType,
      Operator secondOperator,
      DataType secondDataType)
      throws ExecutionException {
    // Arrange
    Map<String, Column<?>> initialData =
        putInitialDataWithoutValues(firstOperator, firstDataType, secondOperator, secondDataType);

    String firstColumnName = getColumnName(firstDataType);
    Column<?> firstColumnToCompare =
        getColumnWithRandomValue(random.get(), firstColumnName, firstDataType);

    String secondColumnName = getColumnName(secondDataType);
    Column<?> secondColumnToCompare =
        getColumnWithRandomValue(random.get(), secondColumnName, secondDataType);

    MutationCondition condition =
        ConditionBuilder.putIf(buildConditionalExpression(firstColumnToCompare, firstOperator))
            .and(buildConditionalExpression(secondColumnToCompare, secondOperator))
            .build();
    Put put =
        preparePutWithRandomValues(firstOperator, firstDataType, secondOperator, secondDataType)
            .withCondition(condition);

    boolean shouldMutate =
        shouldMutate(initialData.get(firstColumnName), firstColumnToCompare, firstOperator)
            && shouldMutate(
                initialData.get(secondColumnName), secondColumnToCompare, secondOperator);

    // Act Assert
    put_withPutIf_shouldPutProperly(
        initialData,
        put,
        shouldMutate,
        prepareGet(firstOperator, firstDataType, secondOperator, secondDataType),
        description(
            initialData,
            put,
            firstColumnToCompare,
            firstOperator,
            secondColumnToCompare,
            secondOperator));
  }

  private void put_withPutIf_shouldPutProperly(
      Map<String, Column<?>> initialData,
      Put put,
      boolean shouldMutate,
      Get get,
      String description)
      throws ExecutionException {
    Throwable thrown = catchThrowable(() -> storage.put(put));
    if (shouldMutate) {
      assertThat(thrown).describedAs(description).isNull();
    } else {
      assertThat(thrown).describedAs(description).isInstanceOf(NoMutationException.class);
    }

    Optional<Result> result = storage.get(get);
    assertThat(result).describedAs(description).isPresent();
    assertThat(result.get().getContainedColumnNames())
        .describedAs(description)
        .isEqualTo(
            ImmutableSet.of(
                PARTITION_KEY,
                COL_NAME1,
                COL_NAME2,
                COL_NAME3,
                COL_NAME4,
                COL_NAME5,
                COL_NAME6,
                COL_NAME7));

    Map<String, Column<?>> expected = shouldMutate ? put.getColumns() : initialData;
    assertThat(result.get().isNull(COL_NAME1))
        .describedAs(description)
        .isEqualTo(expected.get(COL_NAME1).hasNullValue());
    assertThat(result.get().getBoolean(COL_NAME1))
        .describedAs(description)
        .isEqualTo(expected.get(COL_NAME1).getBooleanValue());
    assertThat(result.get().isNull(COL_NAME2))
        .describedAs(description)
        .isEqualTo(expected.get(COL_NAME2).hasNullValue());
    assertThat(result.get().getInt(COL_NAME2))
        .describedAs(description)
        .isEqualTo(expected.get(COL_NAME2).getIntValue());
    assertThat(result.get().isNull(COL_NAME3))
        .describedAs(description)
        .isEqualTo(expected.get(COL_NAME3).hasNullValue());
    assertThat(result.get().getBigInt(COL_NAME3))
        .describedAs(description)
        .isEqualTo(expected.get(COL_NAME3).getBigIntValue());
    assertThat(result.get().isNull(COL_NAME4))
        .describedAs(description)
        .isEqualTo(expected.get(COL_NAME4).hasNullValue());
    assertThat(result.get().getFloat(COL_NAME4))
        .describedAs(description)
        .isEqualTo(expected.get(COL_NAME4).getFloatValue());
    assertThat(result.get().isNull(COL_NAME5))
        .describedAs(description)
        .isEqualTo(expected.get(COL_NAME5).hasNullValue());
    assertThat(result.get().getDouble(COL_NAME5))
        .describedAs(description)
        .isEqualTo(expected.get(COL_NAME5).getDoubleValue());
    assertThat(result.get().getText(COL_NAME6))
        .describedAs(description)
        .isEqualTo(expected.get(COL_NAME6).getTextValue());
    assertThat(result.get().getBlob(COL_NAME7))
        .describedAs(description)
        .isEqualTo(expected.get(COL_NAME7).getBlobValue());
  }

  @Test
  public void put_withPutIfExistsWhenRecordExists_shouldPutProperly() throws ExecutionException {
    random.get().setSeed(seed);

    // Arrange
    putInitialDataWithRandomValues();

    Put put = preparePutWithRandomValues().withCondition(ConditionBuilder.putIfExists());

    // Act
    storage.put(put);

    // Assert
    Optional<Result> result = storage.get(prepareGet());
    assertThat(result).isPresent();
    assertThat(result.get().getContainedColumnNames())
        .isEqualTo(
            ImmutableSet.of(
                PARTITION_KEY,
                COL_NAME1,
                COL_NAME2,
                COL_NAME3,
                COL_NAME4,
                COL_NAME5,
                COL_NAME6,
                COL_NAME7));
    assertThat(result.get().getBoolean(COL_NAME1)).isEqualTo(put.getBooleanValue(COL_NAME1));
    assertThat(result.get().getInt(COL_NAME2)).isEqualTo(put.getIntValue(COL_NAME2));
    assertThat(result.get().getBigInt(COL_NAME3)).isEqualTo(put.getBigIntValue(COL_NAME3));
    assertThat(result.get().getFloat(COL_NAME4)).isEqualTo(put.getFloatValue(COL_NAME4));
    assertThat(result.get().getDouble(COL_NAME5)).isEqualTo(put.getDoubleValue(COL_NAME5));
    assertThat(result.get().getText(COL_NAME6)).isEqualTo(put.getTextValue(COL_NAME6));
    assertThat(result.get().getBlob(COL_NAME7)).isEqualTo(put.getBlobValue(COL_NAME7));
  }

  @Test
  public void put_withPutIfExistsWhenRecordDoesNotExist_shouldThrowNoMutationException()
      throws ExecutionException {
    random.get().setSeed(seed);

    // Arrange
    Put put = preparePutWithRandomValues().withCondition(ConditionBuilder.putIfExists());

    // Act Assert
    assertThatThrownBy(() -> storage.put(put)).isInstanceOf(NoMutationException.class);

    Optional<Result> result = storage.get(prepareGet());
    assertThat(result).isNotPresent();
  }

  @Test
  public void put_withPutIfNotExistsWhenRecordDoesNotExist_shouldPutProperly()
      throws ExecutionException {
    random.get().setSeed(seed);

    // Arrange
    Put put = preparePutWithRandomValues().withCondition(ConditionBuilder.putIfNotExists());

    // Act
    storage.put(put);

    // Assert
    Optional<Result> result = storage.get(prepareGet());
    assertThat(result).isPresent();
    assertThat(result.get().getContainedColumnNames())
        .isEqualTo(
            ImmutableSet.of(
                PARTITION_KEY,
                COL_NAME1,
                COL_NAME2,
                COL_NAME3,
                COL_NAME4,
                COL_NAME5,
                COL_NAME6,
                COL_NAME7));
    assertThat(result.get().getBoolean(COL_NAME1)).isEqualTo(put.getBooleanValue(COL_NAME1));
    assertThat(result.get().getInt(COL_NAME2)).isEqualTo(put.getIntValue(COL_NAME2));
    assertThat(result.get().getBigInt(COL_NAME3)).isEqualTo(put.getBigIntValue(COL_NAME3));
    assertThat(result.get().getFloat(COL_NAME4)).isEqualTo(put.getFloatValue(COL_NAME4));
    assertThat(result.get().getDouble(COL_NAME5)).isEqualTo(put.getDoubleValue(COL_NAME5));
    assertThat(result.get().getText(COL_NAME6)).isEqualTo(put.getTextValue(COL_NAME6));
    assertThat(result.get().getBlob(COL_NAME7)).isEqualTo(put.getBlobValue(COL_NAME7));
  }

  @Test
  public void put_withPutIfNotExistsWhenRecordExists_shouldThrowNoMutationException()
      throws ExecutionException {
    random.get().setSeed(seed);

    // Arrange
    Map<String, Column<?>> initialData = putInitialDataWithRandomValues();

    Put put = preparePutWithRandomValues().withCondition(ConditionBuilder.putIfNotExists());

    // Act Assert
    assertThatThrownBy(() -> storage.put(put)).isInstanceOf(NoMutationException.class);

    Optional<Result> result = storage.get(prepareGet());
    assertThat(result).isPresent();
    assertThat(result.get().getContainedColumnNames())
        .isEqualTo(
            ImmutableSet.of(
                PARTITION_KEY,
                COL_NAME1,
                COL_NAME2,
                COL_NAME3,
                COL_NAME4,
                COL_NAME5,
                COL_NAME6,
                COL_NAME7));
    assertThat(result.get().getBoolean(COL_NAME1))
        .isEqualTo(initialData.get(COL_NAME1).getBooleanValue());
    assertThat(result.get().getInt(COL_NAME2)).isEqualTo(initialData.get(COL_NAME2).getIntValue());
    assertThat(result.get().getBigInt(COL_NAME3))
        .isEqualTo(initialData.get(COL_NAME3).getBigIntValue());
    assertThat(result.get().getFloat(COL_NAME4))
        .isEqualTo(initialData.get(COL_NAME4).getFloatValue());
    assertThat(result.get().getDouble(COL_NAME5))
        .isEqualTo(initialData.get(COL_NAME5).getDoubleValue());
    assertThat(result.get().getText(COL_NAME6))
        .isEqualTo(initialData.get(COL_NAME6).getTextValue());
    assertThat(result.get().getBlob(COL_NAME7))
        .isEqualTo(initialData.get(COL_NAME7).getBlobValue());
  }

  @Test
  public void delete_withDeleteIfWithSingleCondition_shouldPutProperly()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    executeInParallel(
        (operator, dataType) -> {
          delete_withDeleteIfWithSingleConditionWithSameValue_shouldPutProperly(operator, dataType);
          delete_withDeleteWithSingleConditionWithRandomValue_shouldPutProperly(operator, dataType);
          delete_withDeleteIfWithInitialDataWithNullValuesWithSingleCondition_shouldPutProperly(
              operator, dataType);
          delete_withDeleteIfWithInitialDataWithoutValuesWithSingleCondition_shouldPutProperly(
              operator, dataType);
        });
  }

  private void delete_withDeleteIfWithSingleConditionWithSameValue_shouldPutProperly(
      Operator operator, DataType dataType) throws ExecutionException {
    // Arrange
    Map<String, Column<?>> initialData = putInitialDataWithRandomValues(operator, dataType);

    String columnName = getColumnName(dataType);
    Column<?> columnToCompare = initialData.get(columnName);

    MutationCondition condition =
        ConditionBuilder.deleteIf(buildConditionalExpression(columnToCompare, operator)).build();
    Delete delete = prepareDelete(operator, dataType).withCondition(condition);

    boolean shouldMutate = shouldMutate(initialData.get(columnName), columnToCompare, operator);

    // Act Assert
    delete_withDeleteIf_shouldPutProperly(
        initialData,
        delete,
        shouldMutate,
        prepareGet(operator, dataType),
        description(initialData, delete, columnToCompare, operator));
  }

  private void delete_withDeleteWithSingleConditionWithRandomValue_shouldPutProperly(
      Operator operator, DataType dataType) throws ExecutionException {
    for (int i = 0; i < ATTEMPT_COUNT; i++) {
      // Arrange
      Map<String, Column<?>> initialData = putInitialDataWithRandomValues(operator, dataType);

      String columnName = getColumnName(dataType);
      Column<?> columnToCompare = getColumnWithRandomValue(random.get(), columnName, dataType);

      MutationCondition condition =
          ConditionBuilder.deleteIf(buildConditionalExpression(columnToCompare, operator)).build();
      Delete delete = prepareDelete(operator, dataType).withCondition(condition);

      boolean shouldMutate = shouldMutate(initialData.get(columnName), columnToCompare, operator);

      // Act Assert
      delete_withDeleteIf_shouldPutProperly(
          initialData,
          delete,
          shouldMutate,
          prepareGet(operator, dataType),
          description(initialData, delete, columnToCompare, operator));
    }
  }

  private void
      delete_withDeleteIfWithInitialDataWithNullValuesWithSingleCondition_shouldPutProperly(
          Operator operator, DataType dataType) throws ExecutionException {
    // Arrange
    Map<String, Column<?>> initialData = putInitialDataWithNullValues(operator, dataType);

    String columnName = getColumnName(dataType);
    Column<?> columnToCompare = getColumnWithRandomValue(random.get(), columnName, dataType);

    MutationCondition condition =
        ConditionBuilder.deleteIf(buildConditionalExpression(columnToCompare, operator)).build();
    Delete delete = prepareDelete(operator, dataType).withCondition(condition);

    boolean shouldMutate = shouldMutate(initialData.get(columnName), columnToCompare, operator);

    // Act Assert
    delete_withDeleteIf_shouldPutProperly(
        initialData,
        delete,
        shouldMutate,
        prepareGet(operator, dataType),
        description(initialData, delete, columnToCompare, operator));
  }

  private void delete_withDeleteIfWithInitialDataWithoutValuesWithSingleCondition_shouldPutProperly(
      Operator operator, DataType dataType) throws ExecutionException {
    // Arrange
    Map<String, Column<?>> initialData = putInitialDataWithoutValues(operator, dataType);

    String columnName = getColumnName(dataType);
    Column<?> columnToCompare = getColumnWithRandomValue(random.get(), columnName, dataType);

    MutationCondition condition =
        ConditionBuilder.deleteIf(buildConditionalExpression(columnToCompare, operator)).build();
    Delete delete = prepareDelete(operator, dataType).withCondition(condition);

    boolean shouldMutate = shouldMutate(initialData.get(columnName), columnToCompare, operator);

    // Act Assert
    delete_withDeleteIf_shouldPutProperly(
        initialData,
        delete,
        shouldMutate,
        prepareGet(operator, dataType),
        description(initialData, delete, columnToCompare, operator));
  }

  @Test
  public void delete_withDeleteIfWithMultipleConditions_shouldPutProperly()
      throws java.util.concurrent.ExecutionException, InterruptedException {
    executeInParallel(
        (firstOperator, firstDataType, secondOperator, secondDataType) -> {
          delete_withDeleteIfWithMultipleConditionsWithSameValue_shouldPutProperly(
              firstOperator, firstDataType, secondOperator, secondDataType);
          delete_withDeleteIfWithMultipleConditionsWithRandomValue_shouldPutProperly(
              firstOperator, firstDataType, secondOperator, secondDataType);
          delete_withDeleteIfWithInitialDataWithNullValuesWithMultipleConditions_shouldPutProperly(
              firstOperator, firstDataType, secondOperator, secondDataType);
          delete_withDeleteIfWithInitialDataWithoutValuesWithMultipleConditions_shouldPutProperly(
              firstOperator, firstDataType, secondOperator, secondDataType);
        });
  }

  private void delete_withDeleteIfWithMultipleConditionsWithSameValue_shouldPutProperly(
      Operator firstOperator,
      DataType firstDataType,
      Operator secondOperator,
      DataType secondDataType)
      throws ExecutionException {
    // Arrange
    Map<String, Column<?>> initialData =
        putInitialDataWithRandomValues(
            firstOperator, firstDataType, secondOperator, secondDataType);

    String firstColumnName = getColumnName(firstDataType);
    Column<?> firstColumnToCompare = initialData.get(firstColumnName);

    String secondColumnName = getColumnName(secondDataType);
    Column<?> secondColumnToCompare = initialData.get(secondColumnName);

    MutationCondition condition =
        ConditionBuilder.deleteIf(buildConditionalExpression(firstColumnToCompare, firstOperator))
            .and(buildConditionalExpression(secondColumnToCompare, secondOperator))
            .build();

    boolean shouldMutate =
        shouldMutate(initialData.get(firstColumnName), firstColumnToCompare, firstOperator)
            && shouldMutate(
                initialData.get(secondColumnName), secondColumnToCompare, secondOperator);

    Delete delete =
        prepareDelete(firstOperator, firstDataType, secondOperator, secondDataType)
            .withCondition(condition);

    // Act Assert
    delete_withDeleteIf_shouldPutProperly(
        initialData,
        delete,
        shouldMutate,
        prepareGet(firstOperator, firstDataType, secondOperator, secondDataType),
        description(
            initialData,
            delete,
            firstColumnToCompare,
            firstOperator,
            secondColumnToCompare,
            secondOperator));
  }

  private void delete_withDeleteIfWithMultipleConditionsWithRandomValue_shouldPutProperly(
      Operator firstOperator,
      DataType firstDataType,
      Operator secondOperator,
      DataType secondDataType)
      throws ExecutionException {
    for (int i = 0; i < ATTEMPT_COUNT; i++) {
      // Arrange
      Map<String, Column<?>> initialData =
          putInitialDataWithRandomValues(
              firstOperator, firstDataType, secondOperator, secondDataType);

      String firstColumnName = getColumnName(firstDataType);
      Column<?> firstColumnToCompare =
          getColumnWithRandomValue(random.get(), firstColumnName, firstDataType);

      String secondColumnName = getColumnName(secondDataType);
      Column<?> secondColumnToCompare =
          getColumnWithRandomValue(random.get(), secondColumnName, secondDataType);

      MutationCondition condition =
          ConditionBuilder.deleteIf(buildConditionalExpression(firstColumnToCompare, firstOperator))
              .and(buildConditionalExpression(secondColumnToCompare, secondOperator))
              .build();

      boolean shouldMutate =
          shouldMutate(initialData.get(firstColumnName), firstColumnToCompare, firstOperator)
              && shouldMutate(
                  initialData.get(secondColumnName), secondColumnToCompare, secondOperator);

      Delete delete =
          prepareDelete(firstOperator, firstDataType, secondOperator, secondDataType)
              .withCondition(condition);

      // Act Assert
      delete_withDeleteIf_shouldPutProperly(
          initialData,
          delete,
          shouldMutate,
          prepareGet(firstOperator, firstDataType, secondOperator, secondDataType),
          description(
              initialData,
              delete,
              firstColumnToCompare,
              firstOperator,
              secondColumnToCompare,
              secondOperator));
    }
  }

  private void
      delete_withDeleteIfWithInitialDataWithNullValuesWithMultipleConditions_shouldPutProperly(
          Operator firstOperator,
          DataType firstDataType,
          Operator secondOperator,
          DataType secondDataType)
          throws ExecutionException {
    // Arrange
    Map<String, Column<?>> initialData =
        putInitialDataWithNullValues(firstOperator, firstDataType, secondOperator, secondDataType);

    String firstColumnName = getColumnName(firstDataType);
    Column<?> firstColumnToCompare =
        getColumnWithRandomValue(random.get(), firstColumnName, firstDataType);

    String secondColumnName = getColumnName(secondDataType);
    Column<?> secondColumnToCompare =
        getColumnWithRandomValue(random.get(), secondColumnName, secondDataType);

    MutationCondition condition =
        ConditionBuilder.deleteIf(buildConditionalExpression(firstColumnToCompare, firstOperator))
            .and(buildConditionalExpression(secondColumnToCompare, secondOperator))
            .build();

    boolean shouldMutate =
        shouldMutate(initialData.get(firstColumnName), firstColumnToCompare, firstOperator)
            && shouldMutate(
                initialData.get(secondColumnName), secondColumnToCompare, secondOperator);

    Delete delete =
        prepareDelete(firstOperator, firstDataType, secondOperator, secondDataType)
            .withCondition(condition);

    // Act Assert
    delete_withDeleteIf_shouldPutProperly(
        initialData,
        delete,
        shouldMutate,
        prepareGet(firstOperator, firstDataType, secondOperator, secondDataType),
        description(
            initialData,
            delete,
            firstColumnToCompare,
            firstOperator,
            secondColumnToCompare,
            secondOperator));
  }

  private void
      delete_withDeleteIfWithInitialDataWithoutValuesWithMultipleConditions_shouldPutProperly(
          Operator firstOperator,
          DataType firstDataType,
          Operator secondOperator,
          DataType secondDataType)
          throws ExecutionException {
    // Arrange
    Map<String, Column<?>> initialData =
        putInitialDataWithoutValues(firstOperator, firstDataType, secondOperator, secondDataType);

    String firstColumnName = getColumnName(firstDataType);
    Column<?> firstColumnToCompare =
        getColumnWithRandomValue(random.get(), firstColumnName, firstDataType);

    String secondColumnName = getColumnName(secondDataType);
    Column<?> secondColumnToCompare =
        getColumnWithRandomValue(random.get(), secondColumnName, secondDataType);

    MutationCondition condition =
        ConditionBuilder.deleteIf(buildConditionalExpression(firstColumnToCompare, firstOperator))
            .and(buildConditionalExpression(secondColumnToCompare, secondOperator))
            .build();

    boolean shouldMutate =
        shouldMutate(initialData.get(firstColumnName), firstColumnToCompare, firstOperator)
            && shouldMutate(
                initialData.get(secondColumnName), secondColumnToCompare, secondOperator);

    Delete delete =
        prepareDelete(firstOperator, firstDataType, secondOperator, secondDataType)
            .withCondition(condition);

    // Act Assert
    delete_withDeleteIf_shouldPutProperly(
        initialData,
        delete,
        shouldMutate,
        prepareGet(firstOperator, firstDataType, secondOperator, secondDataType),
        description(
            initialData,
            delete,
            firstColumnToCompare,
            firstOperator,
            secondColumnToCompare,
            secondOperator));
  }

  private void delete_withDeleteIf_shouldPutProperly(
      Map<String, Column<?>> initialData,
      Delete delete,
      boolean shouldMutate,
      Get get,
      String description)
      throws ExecutionException {
    Throwable thrown = catchThrowable(() -> storage.delete(delete));
    if (shouldMutate) {
      assertThat(thrown).describedAs(description).isNull();

      Optional<Result> result = storage.get(get);
      assertThat(result).describedAs(description).isNotPresent();
    } else {
      assertThat(thrown).describedAs(description).isInstanceOf(NoMutationException.class);

      Optional<Result> result = storage.get(get);
      assertThat(result).describedAs(description).isPresent();
      assertThat(result.get().getContainedColumnNames())
          .describedAs(description)
          .isEqualTo(
              ImmutableSet.of(
                  PARTITION_KEY,
                  COL_NAME1,
                  COL_NAME2,
                  COL_NAME3,
                  COL_NAME4,
                  COL_NAME5,
                  COL_NAME6,
                  COL_NAME7));

      assertThat(result.get().isNull(COL_NAME1))
          .describedAs(description)
          .isEqualTo(initialData.get(COL_NAME1).hasNullValue());
      assertThat(result.get().getBoolean(COL_NAME1))
          .describedAs(description)
          .isEqualTo(initialData.get(COL_NAME1).getBooleanValue());
      assertThat(result.get().isNull(COL_NAME2))
          .describedAs(description)
          .isEqualTo(initialData.get(COL_NAME2).hasNullValue());
      assertThat(result.get().getInt(COL_NAME2))
          .describedAs(description)
          .isEqualTo(initialData.get(COL_NAME2).getIntValue());
      assertThat(result.get().isNull(COL_NAME3))
          .describedAs(description)
          .isEqualTo(initialData.get(COL_NAME3).hasNullValue());
      assertThat(result.get().getBigInt(COL_NAME3))
          .describedAs(description)
          .isEqualTo(initialData.get(COL_NAME3).getBigIntValue());
      assertThat(result.get().isNull(COL_NAME4))
          .describedAs(description)
          .isEqualTo(initialData.get(COL_NAME4).hasNullValue());
      assertThat(result.get().getFloat(COL_NAME4))
          .describedAs(description)
          .isEqualTo(initialData.get(COL_NAME4).getFloatValue());
      assertThat(result.get().isNull(COL_NAME5))
          .describedAs(description)
          .isEqualTo(initialData.get(COL_NAME5).hasNullValue());
      assertThat(result.get().getDouble(COL_NAME5))
          .describedAs(description)
          .isEqualTo(initialData.get(COL_NAME5).getDoubleValue());
      assertThat(result.get().getText(COL_NAME6))
          .describedAs(description)
          .isEqualTo(initialData.get(COL_NAME6).getTextValue());
      assertThat(result.get().getBlob(COL_NAME7))
          .describedAs(description)
          .isEqualTo(initialData.get(COL_NAME7).getBlobValue());
    }
  }

  @Test
  public void delete_withDeleteIfExistsWhenRecordExists_shouldPutProperly()
      throws ExecutionException {
    random.get().setSeed(seed);

    // Arrange
    putInitialDataWithRandomValues();

    Delete delete = prepareDelete().withCondition(ConditionBuilder.deleteIfExists());

    // Act
    storage.delete(delete);

    // Assert
    Optional<Result> result = storage.get(prepareGet());
    assertThat(result).isNotPresent();
  }

  @Test
  public void delete_withDeleteIfExistsWhenRecordDoesNotExist_shouldThrowNoMutationException()
      throws ExecutionException {
    random.get().setSeed(seed);

    // Arrange
    Delete delete = prepareDelete().withCondition(ConditionBuilder.deleteIfExists());

    // Act Assert
    assertThatThrownBy(() -> storage.delete(delete)).isInstanceOf(NoMutationException.class);

    Optional<Result> result = storage.get(prepareGet());
    assertThat(result).isNotPresent();
  }

  private Get prepareGet() {
    return prepareGet(Operator.EQ, DataType.INT);
  }

  private Get prepareGet(Operator operator, DataType dataType) {
    return prepareGet(operator, dataType, null, null);
  }

  private Get prepareGet(
      Operator firstOperator,
      DataType firstDataType,
      @Nullable Operator secondOperator,
      @Nullable DataType secondDataType) {
    return new Get(
            Key.ofText(
                PARTITION_KEY,
                getPartitionKeyValue(firstOperator, firstDataType, secondOperator, secondDataType)))
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(namespace)
        .forTable(TABLE);
  }

  private Put preparePutWithRandomValues() {
    return preparePutWithRandomValues(Operator.EQ, DataType.INT);
  }

  private Put preparePutWithRandomValues(Operator operator, DataType dataType) {
    return preparePutWithRandomValues(operator, dataType, null, null);
  }

  private Put preparePutWithRandomValues(
      Operator firstOperator,
      DataType firstDataType,
      @Nullable Operator secondOperator,
      @Nullable DataType secondDataType) {
    return new Put(
            Key.ofText(
                PARTITION_KEY,
                getPartitionKeyValue(firstOperator, firstDataType, secondOperator, secondDataType)))
        .withValue(getColumnWithRandomValue(random.get(), COL_NAME1, DataType.BOOLEAN))
        .withValue(getColumnWithRandomValue(random.get(), COL_NAME2, DataType.INT))
        .withValue(getColumnWithRandomValue(random.get(), COL_NAME3, DataType.BIGINT))
        .withValue(getColumnWithRandomValue(random.get(), COL_NAME4, DataType.FLOAT))
        .withValue(getColumnWithRandomValue(random.get(), COL_NAME5, DataType.DOUBLE))
        .withValue(getColumnWithRandomValue(random.get(), COL_NAME6, DataType.TEXT))
        .withValue(getColumnWithRandomValue(random.get(), COL_NAME7, DataType.BLOB))
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(namespace)
        .forTable(TABLE);
  }

  private Delete prepareDelete() {
    return prepareDelete(Operator.EQ, DataType.INT);
  }

  private Delete prepareDelete(Operator operator, DataType dataType) {
    return prepareDelete(operator, dataType, null, null);
  }

  private Delete prepareDelete(
      Operator firstOperator,
      DataType firstDataType,
      @Nullable Operator secondOperator,
      @Nullable DataType secondDataType) {
    return new Delete(
            Key.ofText(
                PARTITION_KEY,
                getPartitionKeyValue(firstOperator, firstDataType, secondOperator, secondDataType)))
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(namespace)
        .forTable(TABLE);
  }

  private Map<String, Column<?>> putInitialDataWithRandomValues() throws ExecutionException {
    return putInitialDataWithRandomValues(Operator.EQ, DataType.INT);
  }

  private Map<String, Column<?>> putInitialDataWithRandomValues(
      Operator operator, DataType dataType) throws ExecutionException {
    return putInitialDataWithRandomValues(operator, dataType, null, null);
  }

  private Map<String, Column<?>> putInitialDataWithRandomValues(
      Operator firstOperator,
      DataType firstDataType,
      @Nullable Operator secondOperator,
      @Nullable DataType secondDataType)
      throws ExecutionException {
    try {
      storage.delete(
          prepareDelete(firstOperator, firstDataType, secondOperator, secondDataType)
              .withCondition(ConditionBuilder.deleteIfExists()));
    } catch (NoMutationException ignored) {
      // ignored
    }

    Put initialPut =
        preparePutWithRandomValues(firstOperator, firstDataType, secondOperator, secondDataType)
            .withCondition(ConditionBuilder.putIfNotExists());
    storage.put(initialPut);
    return initialPut.getColumns();
  }

  private Map<String, Column<?>> putInitialDataWithNullValues(Operator operator, DataType dataType)
      throws ExecutionException {
    return putInitialDataWithNullValues(operator, dataType, null, null);
  }

  private Map<String, Column<?>> putInitialDataWithNullValues(
      Operator firstOperator,
      DataType firstDataType,
      @Nullable Operator secondOperator,
      @Nullable DataType secondDataType)
      throws ExecutionException {
    try {
      storage.delete(
          prepareDelete(firstOperator, firstDataType, secondOperator, secondDataType)
              .withCondition(ConditionBuilder.deleteIfExists()));
    } catch (NoMutationException ignored) {
      // ignored
    }

    Put initialPut =
        preparePutWithNullValues(firstOperator, firstDataType, secondOperator, secondDataType)
            .withCondition(ConditionBuilder.putIfNotExists());
    storage.put(initialPut);
    return initialPut.getColumns();
  }

  private Put preparePutWithNullValues(
      Operator firstOperator,
      DataType firstDataType,
      @Nullable Operator secondOperator,
      @Nullable DataType secondDataType) {
    return new Put(
            Key.ofText(
                PARTITION_KEY,
                getPartitionKeyValue(firstOperator, firstDataType, secondOperator, secondDataType)))
        .withBooleanValue(COL_NAME1, null)
        .withIntValue(COL_NAME2, null)
        .withBigIntValue(COL_NAME3, null)
        .withFloatValue(COL_NAME4, null)
        .withDoubleValue(COL_NAME5, null)
        .withTextValue(COL_NAME6, null)
        .withBlobValue(COL_NAME7, (ByteBuffer) null)
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(namespace)
        .forTable(TABLE);
  }

  private Map<String, Column<?>> putInitialDataWithoutValues(Operator operator, DataType dataType)
      throws ExecutionException {
    return putInitialDataWithoutValues(operator, dataType, null, null);
  }

  private Map<String, Column<?>> putInitialDataWithoutValues(
      Operator firstOperator,
      DataType firstDataType,
      @Nullable Operator secondOperator,
      @Nullable DataType secondDataType)
      throws ExecutionException {
    try {
      storage.delete(
          prepareDelete(firstOperator, firstDataType, secondOperator, secondDataType)
              .withCondition(ConditionBuilder.deleteIfExists()));
    } catch (NoMutationException ignored) {
      // ignored
    }

    Put initialPut =
        preparePutWithoutValues(firstOperator, firstDataType, secondOperator, secondDataType)
            .withCondition(ConditionBuilder.putIfNotExists());
    storage.put(initialPut);
    return ImmutableMap.<String, Column<?>>builder()
        .put(
            PARTITION_KEY,
            TextColumn.of(
                PARTITION_KEY,
                getPartitionKeyValue(firstOperator, firstDataType, secondOperator, secondDataType)))
        .put(COL_NAME1, BooleanColumn.ofNull(COL_NAME1))
        .put(COL_NAME2, IntColumn.ofNull(COL_NAME2))
        .put(COL_NAME3, BigIntColumn.ofNull(COL_NAME3))
        .put(COL_NAME4, FloatColumn.ofNull(COL_NAME4))
        .put(COL_NAME5, DoubleColumn.ofNull(COL_NAME5))
        .put(COL_NAME6, TextColumn.ofNull(COL_NAME6))
        .put(COL_NAME7, BlobColumn.ofNull(COL_NAME7))
        .build();
  }

  private Put preparePutWithoutValues(
      Operator firstOperator,
      DataType firstDataType,
      @Nullable Operator secondOperator,
      @Nullable DataType secondDataType) {
    return new Put(
            Key.ofText(
                PARTITION_KEY,
                getPartitionKeyValue(firstOperator, firstDataType, secondOperator, secondDataType)))
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(namespace)
        .forTable(TABLE);
  }

  private String getPartitionKeyValue(
      Operator firstOperator,
      DataType firstDataType,
      @Nullable Operator secondOperator,
      @Nullable DataType secondDataType) {
    return firstOperator.name()
        + "-"
        + firstDataType.name()
        + (secondOperator == null ? "" : "-" + secondOperator.name())
        + (secondDataType == null ? "" : "-" + secondDataType.name());
  }

  private String getColumnName(DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return COL_NAME1;
      case INT:
        return COL_NAME2;
      case BIGINT:
        return COL_NAME3;
      case FLOAT:
        return COL_NAME4;
      case DOUBLE:
        return COL_NAME5;
      case TEXT:
        return COL_NAME6;
      case BLOB:
        return COL_NAME7;
      default:
        throw new AssertionError();
    }
  }

  private ConditionalExpression buildConditionalExpression(
      Column<?> columnToCompare, Operator operator) {
    if (operator == Operator.IS_NULL || operator == Operator.IS_NOT_NULL) {
      switch (columnToCompare.getDataType()) {
        case BOOLEAN:
          return ConditionBuilder.buildConditionalExpression(
              BooleanColumn.ofNull(columnToCompare.getName()), operator);
        case INT:
          return ConditionBuilder.buildConditionalExpression(
              IntColumn.ofNull(columnToCompare.getName()), operator);
        case BIGINT:
          return ConditionBuilder.buildConditionalExpression(
              BigIntColumn.ofNull(columnToCompare.getName()), operator);
        case FLOAT:
          return ConditionBuilder.buildConditionalExpression(
              FloatColumn.ofNull(columnToCompare.getName()), operator);
        case DOUBLE:
          return ConditionBuilder.buildConditionalExpression(
              DoubleColumn.ofNull(columnToCompare.getName()), operator);
        case TEXT:
          return ConditionBuilder.buildConditionalExpression(
              TextColumn.ofNull(columnToCompare.getName()), operator);
        case BLOB:
          return ConditionBuilder.buildConditionalExpression(
              BlobColumn.ofNull(columnToCompare.getName()), operator);
        default:
          throw new AssertionError();
      }
    }
    return ConditionBuilder.buildConditionalExpression(columnToCompare, operator);
  }

  protected boolean shouldMutate(
      Column<?> initialColumn, Column<?> columnToCompare, Operator operator) {
    switch (operator) {
      case EQ:
        return !initialColumn.hasNullValue()
            && Ordering.natural().compare(initialColumn, columnToCompare) == 0;
      case NE:
        return !initialColumn.hasNullValue()
            && Ordering.natural().compare(initialColumn, columnToCompare) != 0;
      case GT:
        return !initialColumn.hasNullValue()
            && Ordering.natural().compare(initialColumn, columnToCompare) > 0;
      case GTE:
        return !initialColumn.hasNullValue()
            && Ordering.natural().compare(initialColumn, columnToCompare) >= 0;
      case LT:
        return !initialColumn.hasNullValue()
            && Ordering.natural().compare(initialColumn, columnToCompare) < 0;
      case LTE:
        return !initialColumn.hasNullValue()
            && Ordering.natural().compare(initialColumn, columnToCompare) <= 0;
      case IS_NULL:
        return initialColumn.hasNullValue();
      case IS_NOT_NULL:
        return !initialColumn.hasNullValue();
      default:
        throw new AssertionError();
    }
  }

  protected Column<?> getColumnWithRandomValue(
      Random random, String columnName, DataType dataType) {
    return TestUtils.getColumnWithRandomValue(random, columnName, dataType, true);
  }

  private String description(
      Map<String, Column<?>> initialData,
      Mutation mutation,
      Column<?> columnToCompare,
      Operator operator) {
    return String.format(
        "initialData: %s, mutation: %s, columnToCompare: %s, operator: %s",
        initialData, mutation, columnToCompare, operator);
  }

  private String description(
      Map<String, Column<?>> initialData,
      Mutation mutation,
      Column<?> firstColumnToCompare,
      Operator firstOperator,
      Column<?> secondColumnToCompare,
      Operator secondOperator) {
    return String.format(
        "initialData: %s, mutation: %s, firstColumnToCompare: %s, firstOperator: %s, "
            + "secondColumnToCompare: %s, secondOperator: %s",
        initialData,
        mutation,
        firstColumnToCompare,
        firstOperator,
        secondColumnToCompare,
        secondOperator);
  }

  private void executeInParallel(TestForSingleCondition test)
      throws java.util.concurrent.ExecutionException, InterruptedException {
    List<Callable<Void>> testCallables = new ArrayList<>();
    for (OperatorAndDataType operatorAndDataType : operatorAndDataTypeList) {
      testCallables.add(
          () -> {
            random.get().setSeed(seed);

            test.execute(operatorAndDataType.getOperator(), operatorAndDataType.getDataType());
            return null;
          });
    }
    executeInParallel(testCallables);
  }

  private void executeInParallel(TestForMultipleConditions test)
      throws java.util.concurrent.ExecutionException, InterruptedException {
    List<Callable<Void>> testCallables = new ArrayList<>();
    for (OperatorAndDataType firstOperatorAndDataType : operatorAndDataTypeList) {
      for (OperatorAndDataType secondOperatorAndDataType : operatorAndDataTypeList) {
        testCallables.add(
            () -> {
              random.get().setSeed(seed);

              test.execute(
                  firstOperatorAndDataType.getOperator(),
                  firstOperatorAndDataType.getDataType(),
                  secondOperatorAndDataType.getOperator(),
                  secondOperatorAndDataType.getDataType());
              return null;
            });
      }
    }
    executeInParallel(testCallables);
  }

  private void executeInParallel(List<Callable<Void>> testCallables)
      throws InterruptedException, java.util.concurrent.ExecutionException {
    List<Future<Void>> futures = executorService.invokeAll(testCallables);
    for (Future<Void> future : futures) {
      future.get();
    }
  }

  @FunctionalInterface
  private interface TestForSingleCondition {
    void execute(Operator operator, DataType dataType) throws Exception;
  }

  @FunctionalInterface
  private interface TestForMultipleConditions {
    void execute(
        Operator firstOperator,
        DataType firstDataType,
        Operator secondOperator,
        DataType secondDataType)
        throws Exception;
  }

  public static class OperatorAndDataType {
    private final Operator operator;
    private final DataType dataType;

    public OperatorAndDataType(Operator operator, DataType dataType) {
      this.operator = operator;
      this.dataType = dataType;
    }

    public Operator getOperator() {
      return operator;
    }

    public DataType getDataType() {
      return dataType;
    }
  }
}
