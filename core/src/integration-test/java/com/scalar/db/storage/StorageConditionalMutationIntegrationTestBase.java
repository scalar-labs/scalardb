package com.scalar.db.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

@SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
public abstract class StorageConditionalMutationIntegrationTestBase {

  private static final String TEST_NAME = "cond_mutation";
  private static final String NAMESPACE = "integration_testing_" + TEST_NAME;
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
          .addColumn(PARTITION_KEY, DataType.INT)
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
  private static final Random RANDOM = new Random();

  private static boolean initialized;
  private static DistributedStorageAdmin admin;
  private static DistributedStorage storage;
  private static String namespace;

  private static long seed;
  private static List<OperatorAndDataType> operatorAndDataTypeList;

  @Before
  public void setUp() throws Exception {
    if (!initialized) {
      StorageFactory factory =
          new StorageFactory(TestUtils.addSuffix(getDatabaseConfig(), TEST_NAME));
      admin = factory.getAdmin();
      namespace = getNamespace();
      createTable();
      storage = factory.getStorage();
      seed = System.currentTimeMillis();
      System.out.println("The seed used in the conditional mutation integration test is " + seed);
      operatorAndDataTypeList = getOperatorAndDataTypeListForTest();
      initialized = true;
    }
    admin.truncateTable(namespace, TABLE);
  }

  protected abstract DatabaseConfig getDatabaseConfig();

  protected String getNamespace() {
    return NAMESPACE;
  }

  protected Map<String, String> getCreateOptions() {
    return Collections.emptyMap();
  }

  private void createTable() throws ExecutionException {
    Map<String, String> options = getCreateOptions();
    admin.createNamespace(namespace, true, options);
    admin.createTable(namespace, TABLE, TABLE_METADATA, true, options);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    deleteTable();
    admin.close();
    storage.close();
  }

  private static void deleteTable() throws ExecutionException {
    admin.dropTable(namespace, TABLE);
    admin.dropNamespace(namespace);
  }

  protected List<OperatorAndDataType> getOperatorAndDataTypeListForTest() {
    List<OperatorAndDataType> ret = new ArrayList<>();
    for (Operator operator : Operator.values()) {
      for (DataType dataType : DataType.values()) {
        ret.add(new OperatorAndDataType(operator, dataType));
      }
    }
    return ret;
  }

  @Test
  public void put_withPutIfWithSingleCondition_shouldPutProperly() throws ExecutionException {
    RANDOM.setSeed(seed);

    for (OperatorAndDataType operatorAndDataType : operatorAndDataTypeList) {
      put_withPutIfWithSingleConditionWithSameValue_shouldPutProperly(
          operatorAndDataType.getOperator(), operatorAndDataType.getDataType());
      put_withPutIfWithSingleConditionWithRandomValue_shouldPutProperly(
          operatorAndDataType.getOperator(), operatorAndDataType.getDataType());
      put_withPutIfWithInitialDataWithNullValuesWithSingleCondition_shouldPutProperly(
          operatorAndDataType.getOperator(), operatorAndDataType.getDataType());
      put_withPutIfWithInitialDataWithoutValuesWithSingleCondition_shouldPutProperly(
          operatorAndDataType.getOperator(), operatorAndDataType.getDataType());
    }
  }

  private void put_withPutIfWithSingleConditionWithSameValue_shouldPutProperly(
      Operator operator, DataType dataType) throws ExecutionException {
    // Arrange
    Map<String, Column<?>> initialData = putInitialDataWithRandomValues();

    String columnName = getColumnName(dataType);
    Column<?> columnToCompare = initialData.get(columnName);

    MutationCondition condition =
        ConditionBuilder.putIf(buildConditionalExpression(columnToCompare, operator)).build();
    Put put = preparePutWithRandomValues().withCondition(condition);

    boolean shouldMutate = shouldMutate(initialData.get(columnName), columnToCompare, operator);

    // Act Assert
    put_withPutIf_shouldPutProperly(
        initialData, put, shouldMutate, description(initialData, put, columnToCompare, operator));
  }

  private void put_withPutIfWithSingleConditionWithRandomValue_shouldPutProperly(
      Operator operator, DataType dataType) throws ExecutionException {
    for (int i = 0; i < ATTEMPT_COUNT; i++) {
      // Arrange
      Map<String, Column<?>> initialData = putInitialDataWithRandomValues();

      String columnName = getColumnName(dataType);
      Column<?> columnToCompare = getColumnWithRandomValue(RANDOM, columnName, dataType);

      MutationCondition condition =
          ConditionBuilder.putIf(buildConditionalExpression(columnToCompare, operator)).build();
      Put put = preparePutWithRandomValues().withCondition(condition);

      boolean shouldMutate = shouldMutate(initialData.get(columnName), columnToCompare, operator);

      // Act Assert
      put_withPutIf_shouldPutProperly(
          initialData, put, shouldMutate, description(initialData, put, columnToCompare, operator));
    }
  }

  private void put_withPutIfWithInitialDataWithNullValuesWithSingleCondition_shouldPutProperly(
      Operator operator, DataType dataType) throws ExecutionException {
    // Arrange
    Map<String, Column<?>> initialData = putInitialDataWithNullValues();

    String columnName = getColumnName(dataType);
    Column<?> columnToCompare = getColumnWithRandomValue(RANDOM, columnName, dataType);

    MutationCondition condition =
        ConditionBuilder.putIf(buildConditionalExpression(columnToCompare, operator)).build();
    Put put = preparePutWithRandomValues().withCondition(condition);

    boolean shouldMutate = shouldMutate(initialData.get(columnName), columnToCompare, operator);

    // Act Assert
    put_withPutIf_shouldPutProperly(
        initialData, put, shouldMutate, description(initialData, put, columnToCompare, operator));
  }

  private void put_withPutIfWithInitialDataWithoutValuesWithSingleCondition_shouldPutProperly(
      Operator operator, DataType dataType) throws ExecutionException {
    // Arrange
    Map<String, Column<?>> initialData = putInitialDataWithoutValues();

    String columnName = getColumnName(dataType);
    Column<?> columnToCompare = getColumnWithRandomValue(RANDOM, columnName, dataType);

    MutationCondition condition =
        ConditionBuilder.putIf(buildConditionalExpression(columnToCompare, operator)).build();
    Put put = preparePutWithRandomValues().withCondition(condition);

    boolean shouldMutate = shouldMutate(initialData.get(columnName), columnToCompare, operator);

    // Act Assert
    put_withPutIf_shouldPutProperly(
        initialData, put, shouldMutate, description(initialData, put, columnToCompare, operator));
  }

  @Test
  public void put_withPutIfWithMultipleConditions_shouldPutProperly() throws ExecutionException {
    RANDOM.setSeed(seed);

    for (OperatorAndDataType firstOperatorAndDataType : operatorAndDataTypeList) {
      for (OperatorAndDataType secondOperatorAndDataType : operatorAndDataTypeList) {
        put_withPutIfWithMultipleConditionsWithSameValue_shouldPutProperly(
            firstOperatorAndDataType.getOperator(),
            firstOperatorAndDataType.getDataType(),
            secondOperatorAndDataType.getOperator(),
            secondOperatorAndDataType.getDataType());
        put_withPutIfWithMultipleConditionsWithRandomValue_shouldPutProperly(
            firstOperatorAndDataType.getOperator(),
            firstOperatorAndDataType.getDataType(),
            secondOperatorAndDataType.getOperator(),
            secondOperatorAndDataType.getDataType());
        put_withPutIfWithInitialDataWithNullValuesWithMultipleConditions_shouldPutProperly(
            firstOperatorAndDataType.getOperator(),
            firstOperatorAndDataType.getDataType(),
            secondOperatorAndDataType.getOperator(),
            secondOperatorAndDataType.getDataType());
        put_withPutIfWithInitialDataWithoutValuesWithMultipleConditions_shouldPutProperly(
            firstOperatorAndDataType.getOperator(),
            firstOperatorAndDataType.getDataType(),
            secondOperatorAndDataType.getOperator(),
            secondOperatorAndDataType.getDataType());
      }
    }
  }

  private void put_withPutIfWithMultipleConditionsWithSameValue_shouldPutProperly(
      Operator firstOperator,
      DataType firstDataType,
      Operator secondOperator,
      DataType secondDataType)
      throws ExecutionException {
    // Arrange
    Map<String, Column<?>> initialData = putInitialDataWithRandomValues();

    String firstColumnName = getColumnName(firstDataType);
    Column<?> firstColumnToCompare = initialData.get(firstColumnName);

    String secondColumnName = getColumnName(secondDataType);
    Column<?> secondColumnToCompare = initialData.get(secondColumnName);

    MutationCondition condition =
        ConditionBuilder.putIf(buildConditionalExpression(firstColumnToCompare, firstOperator))
            .and(buildConditionalExpression(secondColumnToCompare, secondOperator))
            .build();
    Put put = preparePutWithRandomValues().withCondition(condition);

    boolean shouldMutate =
        shouldMutate(initialData.get(firstColumnName), firstColumnToCompare, firstOperator)
            && shouldMutate(
                initialData.get(secondColumnName), secondColumnToCompare, secondOperator);

    // Act Assert
    put_withPutIf_shouldPutProperly(
        initialData,
        put,
        shouldMutate,
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
      Map<String, Column<?>> initialData = putInitialDataWithRandomValues();

      String firstColumnName = getColumnName(firstDataType);
      Column<?> firstColumnToCompare =
          getColumnWithRandomValue(RANDOM, firstColumnName, firstDataType);

      String secondColumnName = getColumnName(secondDataType);
      Column<?> secondColumnToCompare =
          getColumnWithRandomValue(RANDOM, secondColumnName, secondDataType);

      MutationCondition condition =
          ConditionBuilder.putIf(buildConditionalExpression(firstColumnToCompare, firstOperator))
              .and(buildConditionalExpression(secondColumnToCompare, secondOperator))
              .build();
      Put put = preparePutWithRandomValues().withCondition(condition);

      boolean shouldMutate =
          shouldMutate(initialData.get(firstColumnName), firstColumnToCompare, firstOperator)
              && shouldMutate(
                  initialData.get(secondColumnName), secondColumnToCompare, secondOperator);

      // Act Assert
      put_withPutIf_shouldPutProperly(
          initialData,
          put,
          shouldMutate,
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
    Map<String, Column<?>> initialData = putInitialDataWithNullValues();

    String firstColumnName = getColumnName(firstDataType);
    Column<?> firstColumnToCompare =
        getColumnWithRandomValue(RANDOM, firstColumnName, firstDataType);

    String secondColumnName = getColumnName(secondDataType);
    Column<?> secondColumnToCompare =
        getColumnWithRandomValue(RANDOM, secondColumnName, secondDataType);

    MutationCondition condition =
        ConditionBuilder.putIf(buildConditionalExpression(firstColumnToCompare, firstOperator))
            .and(buildConditionalExpression(secondColumnToCompare, secondOperator))
            .build();
    Put put = preparePutWithRandomValues().withCondition(condition);

    boolean shouldMutate =
        shouldMutate(initialData.get(firstColumnName), firstColumnToCompare, firstOperator)
            && shouldMutate(
                initialData.get(secondColumnName), secondColumnToCompare, secondOperator);

    // Act Assert
    put_withPutIf_shouldPutProperly(
        initialData,
        put,
        shouldMutate,
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
    Map<String, Column<?>> initialData = putInitialDataWithoutValues();

    String firstColumnName = getColumnName(firstDataType);
    Column<?> firstColumnToCompare =
        getColumnWithRandomValue(RANDOM, firstColumnName, firstDataType);

    String secondColumnName = getColumnName(secondDataType);
    Column<?> secondColumnToCompare =
        getColumnWithRandomValue(RANDOM, secondColumnName, secondDataType);

    MutationCondition condition =
        ConditionBuilder.putIf(buildConditionalExpression(firstColumnToCompare, firstOperator))
            .and(buildConditionalExpression(secondColumnToCompare, secondOperator))
            .build();
    Put put = preparePutWithRandomValues().withCondition(condition);

    boolean shouldMutate =
        shouldMutate(initialData.get(firstColumnName), firstColumnToCompare, firstOperator)
            && shouldMutate(
                initialData.get(secondColumnName), secondColumnToCompare, secondOperator);

    // Act Assert
    put_withPutIf_shouldPutProperly(
        initialData,
        put,
        shouldMutate,
        description(
            initialData,
            put,
            firstColumnToCompare,
            firstOperator,
            secondColumnToCompare,
            secondOperator));
  }

  private void put_withPutIf_shouldPutProperly(
      Map<String, Column<?>> initialData, Put put, boolean shouldMutate, String description)
      throws ExecutionException {
    Throwable thrown = catchThrowable(() -> storage.put(put));
    if (shouldMutate) {
      assertThat(thrown).describedAs(description).isNull();
    } else {
      assertThat(thrown).describedAs(description).isInstanceOf(NoMutationException.class);
    }

    Optional<Result> result = storage.get(prepareGet());
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
    RANDOM.setSeed(seed);

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
    RANDOM.setSeed(seed);

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
    RANDOM.setSeed(seed);

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
    RANDOM.setSeed(seed);

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
  public void delete_withDeleteIfWithSingleCondition_shouldPutProperly() throws ExecutionException {
    RANDOM.setSeed(seed);

    for (OperatorAndDataType operatorAndDataType : operatorAndDataTypeList) {
      delete_withDeleteIfWithSingleConditionWithSameValue_shouldPutProperly(
          operatorAndDataType.getOperator(), operatorAndDataType.getDataType());
      delete_withDeleteWithSingleConditionWithRandomValue_shouldPutProperly(
          operatorAndDataType.getOperator(), operatorAndDataType.getDataType());
      delete_withDeleteIfWithInitialDataWithNullValuesWithSingleCondition_shouldPutProperly(
          operatorAndDataType.getOperator(), operatorAndDataType.getDataType());
      delete_withDeleteIfWithInitialDataWithoutValuesWithSingleCondition_shouldPutProperly(
          operatorAndDataType.getOperator(), operatorAndDataType.getDataType());
    }
  }

  private void delete_withDeleteIfWithSingleConditionWithSameValue_shouldPutProperly(
      Operator operator, DataType dataType) throws ExecutionException {
    // Arrange
    Map<String, Column<?>> initialData = putInitialDataWithRandomValues();

    String columnName = getColumnName(dataType);
    Column<?> columnToCompare = initialData.get(columnName);

    MutationCondition condition =
        ConditionBuilder.deleteIf(buildConditionalExpression(columnToCompare, operator)).build();
    Delete delete = prepareDelete().withCondition(condition);

    boolean shouldMutate = shouldMutate(initialData.get(columnName), columnToCompare, operator);

    // Act Assert
    delete_withDeleteIf_shouldPutProperly(
        initialData,
        delete,
        shouldMutate,
        description(initialData, delete, columnToCompare, operator));
  }

  private void delete_withDeleteWithSingleConditionWithRandomValue_shouldPutProperly(
      Operator operator, DataType dataType) throws ExecutionException {
    for (int i = 0; i < ATTEMPT_COUNT; i++) {
      // Arrange
      Map<String, Column<?>> initialData = putInitialDataWithRandomValues();

      String columnName = getColumnName(dataType);
      Column<?> columnToCompare = getColumnWithRandomValue(RANDOM, columnName, dataType);

      MutationCondition condition =
          ConditionBuilder.deleteIf(buildConditionalExpression(columnToCompare, operator)).build();
      Delete delete = prepareDelete().withCondition(condition);

      boolean shouldMutate = shouldMutate(initialData.get(columnName), columnToCompare, operator);

      // Act Assert
      delete_withDeleteIf_shouldPutProperly(
          initialData,
          delete,
          shouldMutate,
          description(initialData, delete, columnToCompare, operator));
    }
  }

  private void
      delete_withDeleteIfWithInitialDataWithNullValuesWithSingleCondition_shouldPutProperly(
          Operator operator, DataType dataType) throws ExecutionException {
    // Arrange
    Map<String, Column<?>> initialData = putInitialDataWithNullValues();

    String columnName = getColumnName(dataType);
    Column<?> columnToCompare = getColumnWithRandomValue(RANDOM, columnName, dataType);

    MutationCondition condition =
        ConditionBuilder.deleteIf(buildConditionalExpression(columnToCompare, operator)).build();
    Delete delete = prepareDelete().withCondition(condition);

    boolean shouldMutate = shouldMutate(initialData.get(columnName), columnToCompare, operator);

    // Act Assert
    delete_withDeleteIf_shouldPutProperly(
        initialData,
        delete,
        shouldMutate,
        description(initialData, delete, columnToCompare, operator));
  }

  private void delete_withDeleteIfWithInitialDataWithoutValuesWithSingleCondition_shouldPutProperly(
      Operator operator, DataType dataType) throws ExecutionException {
    // Arrange
    Map<String, Column<?>> initialData = putInitialDataWithoutValues();

    String columnName = getColumnName(dataType);
    Column<?> columnToCompare = getColumnWithRandomValue(RANDOM, columnName, dataType);

    MutationCondition condition =
        ConditionBuilder.deleteIf(buildConditionalExpression(columnToCompare, operator)).build();
    Delete delete = prepareDelete().withCondition(condition);

    boolean shouldMutate = shouldMutate(initialData.get(columnName), columnToCompare, operator);

    // Act Assert
    delete_withDeleteIf_shouldPutProperly(
        initialData,
        delete,
        shouldMutate,
        description(initialData, delete, columnToCompare, operator));
  }

  @Test
  public void delete_withDeleteIfWithMultipleConditions_shouldPutProperly()
      throws ExecutionException {
    RANDOM.setSeed(seed);

    for (OperatorAndDataType firstOperatorAndDataType : operatorAndDataTypeList) {
      for (OperatorAndDataType secondOperatorAndDataType : operatorAndDataTypeList) {
        delete_withDeleteIfWithMultipleConditionsWithSameValue_shouldPutProperly(
            firstOperatorAndDataType.getOperator(),
            firstOperatorAndDataType.getDataType(),
            secondOperatorAndDataType.getOperator(),
            secondOperatorAndDataType.getDataType());
        delete_withDeleteIfWithMultipleConditionsWithRandomValue_shouldPutProperly(
            firstOperatorAndDataType.getOperator(),
            firstOperatorAndDataType.getDataType(),
            secondOperatorAndDataType.getOperator(),
            secondOperatorAndDataType.getDataType());
        delete_withDeleteIfWithInitialDataWithNullValuesWithMultipleConditions_shouldPutProperly(
            firstOperatorAndDataType.getOperator(),
            firstOperatorAndDataType.getDataType(),
            secondOperatorAndDataType.getOperator(),
            secondOperatorAndDataType.getDataType());
        delete_withDeleteIfWithInitialDataWithoutValuesWithMultipleConditions_shouldPutProperly(
            firstOperatorAndDataType.getOperator(),
            firstOperatorAndDataType.getDataType(),
            secondOperatorAndDataType.getOperator(),
            secondOperatorAndDataType.getDataType());
      }
    }
  }

  private void delete_withDeleteIfWithMultipleConditionsWithSameValue_shouldPutProperly(
      Operator firstOperator,
      DataType firstDataType,
      Operator secondOperator,
      DataType secondDataType)
      throws ExecutionException {
    // Arrange
    Map<String, Column<?>> initialData = putInitialDataWithRandomValues();

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

    Delete delete = prepareDelete().withCondition(condition);

    // Act Assert
    delete_withDeleteIf_shouldPutProperly(
        initialData,
        delete,
        shouldMutate,
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
      Map<String, Column<?>> initialData = putInitialDataWithRandomValues();

      String firstColumnName = getColumnName(firstDataType);
      Column<?> firstColumnToCompare =
          getColumnWithRandomValue(RANDOM, firstColumnName, firstDataType);

      String secondColumnName = getColumnName(secondDataType);
      Column<?> secondColumnToCompare =
          getColumnWithRandomValue(RANDOM, secondColumnName, secondDataType);

      MutationCondition condition =
          ConditionBuilder.deleteIf(buildConditionalExpression(firstColumnToCompare, firstOperator))
              .and(buildConditionalExpression(secondColumnToCompare, secondOperator))
              .build();

      boolean shouldMutate =
          shouldMutate(initialData.get(firstColumnName), firstColumnToCompare, firstOperator)
              && shouldMutate(
                  initialData.get(secondColumnName), secondColumnToCompare, secondOperator);

      Delete delete = prepareDelete().withCondition(condition);

      // Act Assert
      delete_withDeleteIf_shouldPutProperly(
          initialData,
          delete,
          shouldMutate,
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
    Map<String, Column<?>> initialData = putInitialDataWithNullValues();

    String firstColumnName = getColumnName(firstDataType);
    Column<?> firstColumnToCompare =
        getColumnWithRandomValue(RANDOM, firstColumnName, firstDataType);

    String secondColumnName = getColumnName(secondDataType);
    Column<?> secondColumnToCompare =
        getColumnWithRandomValue(RANDOM, secondColumnName, secondDataType);

    MutationCondition condition =
        ConditionBuilder.deleteIf(buildConditionalExpression(firstColumnToCompare, firstOperator))
            .and(buildConditionalExpression(secondColumnToCompare, secondOperator))
            .build();

    boolean shouldMutate =
        shouldMutate(initialData.get(firstColumnName), firstColumnToCompare, firstOperator)
            && shouldMutate(
                initialData.get(secondColumnName), secondColumnToCompare, secondOperator);

    Delete delete = prepareDelete().withCondition(condition);

    // Act Assert
    delete_withDeleteIf_shouldPutProperly(
        initialData,
        delete,
        shouldMutate,
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
    Map<String, Column<?>> initialData = putInitialDataWithoutValues();

    String firstColumnName = getColumnName(firstDataType);
    Column<?> firstColumnToCompare =
        getColumnWithRandomValue(RANDOM, firstColumnName, firstDataType);

    String secondColumnName = getColumnName(secondDataType);
    Column<?> secondColumnToCompare =
        getColumnWithRandomValue(RANDOM, secondColumnName, secondDataType);

    MutationCondition condition =
        ConditionBuilder.deleteIf(buildConditionalExpression(firstColumnToCompare, firstOperator))
            .and(buildConditionalExpression(secondColumnToCompare, secondOperator))
            .build();

    boolean shouldMutate =
        shouldMutate(initialData.get(firstColumnName), firstColumnToCompare, firstOperator)
            && shouldMutate(
                initialData.get(secondColumnName), secondColumnToCompare, secondOperator);

    Delete delete = prepareDelete().withCondition(condition);

    // Act Assert
    delete_withDeleteIf_shouldPutProperly(
        initialData,
        delete,
        shouldMutate,
        description(
            initialData,
            delete,
            firstColumnToCompare,
            firstOperator,
            secondColumnToCompare,
            secondOperator));
  }

  private void delete_withDeleteIf_shouldPutProperly(
      Map<String, Column<?>> initialData, Delete delete, boolean shouldMutate, String description)
      throws ExecutionException {
    Throwable thrown = catchThrowable(() -> storage.delete(delete));
    if (shouldMutate) {
      assertThat(thrown).describedAs(description).isNull();

      Optional<Result> result = storage.get(prepareGet());
      assertThat(result).describedAs(description).isNotPresent();
    } else {
      assertThat(thrown).describedAs(description).isInstanceOf(NoMutationException.class);

      Optional<Result> result = storage.get(prepareGet());
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
    RANDOM.setSeed(seed);

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
    RANDOM.setSeed(seed);

    // Arrange
    Delete delete = prepareDelete().withCondition(ConditionBuilder.deleteIfExists());

    // Act Assert
    assertThatThrownBy(() -> storage.delete(delete)).isInstanceOf(NoMutationException.class);

    Optional<Result> result = storage.get(prepareGet());
    assertThat(result).isNotPresent();
  }

  private Get prepareGet() {
    return new Get(Key.ofInt(PARTITION_KEY, 1))
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(namespace)
        .forTable(TABLE);
  }

  private Put preparePutWithRandomValues() {
    return new Put(Key.ofInt(PARTITION_KEY, 1))
        .withValue(getColumnWithRandomValue(RANDOM, COL_NAME1, DataType.BOOLEAN))
        .withValue(getColumnWithRandomValue(RANDOM, COL_NAME2, DataType.INT))
        .withValue(getColumnWithRandomValue(RANDOM, COL_NAME3, DataType.BIGINT))
        .withValue(getColumnWithRandomValue(RANDOM, COL_NAME4, DataType.FLOAT))
        .withValue(getColumnWithRandomValue(RANDOM, COL_NAME5, DataType.DOUBLE))
        .withValue(getColumnWithRandomValue(RANDOM, COL_NAME6, DataType.TEXT))
        .withValue(getColumnWithRandomValue(RANDOM, COL_NAME7, DataType.BLOB))
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(namespace)
        .forTable(TABLE);
  }

  private Put preparePutWithNullValues() {
    return new Put(Key.ofInt(PARTITION_KEY, 1))
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

  private Put preparePutWithoutValues() {
    return new Put(Key.ofInt(PARTITION_KEY, 1))
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(namespace)
        .forTable(TABLE);
  }

  private Delete prepareDelete() {
    return new Delete(Key.ofInt(PARTITION_KEY, 1))
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(namespace)
        .forTable(TABLE);
  }

  private Map<String, Column<?>> putInitialDataWithRandomValues() throws ExecutionException {
    try {
      storage.delete(prepareDelete().withCondition(ConditionBuilder.deleteIfExists()));
    } catch (NoMutationException ignored) {
      // ignored
    }

    Put initialPut = preparePutWithRandomValues().withCondition(ConditionBuilder.putIfNotExists());
    storage.put(initialPut);
    return initialPut.getColumns();
  }

  private Map<String, Column<?>> putInitialDataWithNullValues() throws ExecutionException {
    try {
      storage.delete(prepareDelete().withCondition(ConditionBuilder.deleteIfExists()));
    } catch (NoMutationException ignored) {
      // ignored
    }

    Put initialPut = preparePutWithNullValues().withCondition(ConditionBuilder.putIfNotExists());
    storage.put(initialPut);
    return initialPut.getColumns();
  }

  private Map<String, Column<?>> putInitialDataWithoutValues() throws ExecutionException {
    try {
      storage.delete(prepareDelete().withCondition(ConditionBuilder.deleteIfExists()));
    } catch (NoMutationException ignored) {
      // ignored
    }

    Put initialPut = preparePutWithoutValues().withCondition(ConditionBuilder.putIfNotExists());
    storage.put(initialPut);
    return ImmutableMap.<String, Column<?>>builder()
        .put(PARTITION_KEY, IntColumn.of(PARTITION_KEY, 1))
        .put(COL_NAME1, BooleanColumn.ofNull(COL_NAME1))
        .put(COL_NAME2, IntColumn.ofNull(COL_NAME2))
        .put(COL_NAME3, BigIntColumn.ofNull(COL_NAME3))
        .put(COL_NAME4, FloatColumn.ofNull(COL_NAME4))
        .put(COL_NAME5, DoubleColumn.ofNull(COL_NAME5))
        .put(COL_NAME6, TextColumn.ofNull(COL_NAME6))
        .put(COL_NAME7, BlobColumn.ofNull(COL_NAME7))
        .build();
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
