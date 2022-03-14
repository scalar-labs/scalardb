package com.scalar.db.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.service.StorageFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
    }
  }

  private void put_withPutIfWithSingleConditionWithSameValue_shouldPutProperly(
      Operator operator, DataType dataType) throws ExecutionException {
    // Arrange
    Put initialPut = putInitialData();

    String columnName = getColumnName(dataType);
    Value<?> value = getInitialValue(initialPut, dataType).copyWith("");
    MutationCondition condition =
        ConditionBuilder.putIf(getConditionalExpression(columnName, operator, value)).build();
    boolean shouldMutate = shouldMutate(initialPut, operator, dataType, value);

    Put put = preparePutWithRandomValues().withCondition(condition);

    // Act Assert
    put_withPutIf_shouldPutProperly(
        initialPut, put, shouldMutate, description(initialPut, put, columnName, operator, value));
  }

  private void put_withPutIfWithSingleConditionWithRandomValue_shouldPutProperly(
      Operator operator, DataType dataType) throws ExecutionException {
    for (int i = 0; i < ATTEMPT_COUNT; i++) {
      // Arrange
      Put initialPut = putInitialData();

      String columnName = getColumnName(dataType);
      Value<?> value = getRandomValue(RANDOM, "", dataType);
      MutationCondition condition =
          ConditionBuilder.putIf(getConditionalExpression(columnName, operator, value)).build();
      boolean shouldMutate = shouldMutate(initialPut, operator, dataType, value);

      Put put = preparePutWithRandomValues().withCondition(condition);

      // Act Assert
      put_withPutIf_shouldPutProperly(
          initialPut, put, shouldMutate, description(initialPut, put, columnName, operator, value));
    }
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
    Put initialPut = putInitialData();

    String firstColumnName = getColumnName(firstDataType);
    Value<?> firstValue = getInitialValue(initialPut, firstDataType).copyWith("");

    String secondColumnName = getColumnName(secondDataType);
    Value<?> secondValue = getInitialValue(initialPut, secondDataType).copyWith("");

    MutationCondition condition =
        ConditionBuilder.putIf(getConditionalExpression(firstColumnName, firstOperator, firstValue))
            .and(getConditionalExpression(secondColumnName, secondOperator, secondValue))
            .build();

    boolean shouldMutate =
        shouldMutate(initialPut, firstOperator, firstDataType, firstValue)
            && shouldMutate(initialPut, secondOperator, secondDataType, secondValue);

    Put put = preparePutWithRandomValues().withCondition(condition);

    // Act Assert
    put_withPutIf_shouldPutProperly(
        initialPut,
        put,
        shouldMutate,
        description(
            initialPut,
            put,
            firstColumnName,
            firstOperator,
            firstValue,
            secondColumnName,
            secondOperator,
            secondValue));
  }

  private void put_withPutIfWithMultipleConditionsWithRandomValue_shouldPutProperly(
      Operator firstOperator,
      DataType firstDataType,
      Operator secondOperator,
      DataType secondDataType)
      throws ExecutionException {
    for (int i = 0; i < ATTEMPT_COUNT; i++) {
      // Arrange
      Put initialPut = putInitialData();

      String firstColumnName = getColumnName(firstDataType);
      Value<?> firstValue = getRandomValue(RANDOM, "", firstDataType);

      String secondColumnName = getColumnName(secondDataType);
      Value<?> secondValue = getRandomValue(RANDOM, "", secondDataType);

      MutationCondition condition =
          ConditionBuilder.putIf(
                  getConditionalExpression(firstColumnName, firstOperator, firstValue))
              .and(getConditionalExpression(secondColumnName, secondOperator, secondValue))
              .build();

      boolean shouldMutate =
          shouldMutate(initialPut, firstOperator, firstDataType, firstValue)
              && shouldMutate(initialPut, secondOperator, secondDataType, secondValue);

      Put put = preparePutWithRandomValues().withCondition(condition);

      // Act Assert
      put_withPutIf_shouldPutProperly(
          initialPut,
          put,
          shouldMutate,
          description(
              initialPut,
              put,
              firstColumnName,
              firstOperator,
              firstValue,
              secondColumnName,
              secondOperator,
              secondValue));
    }
  }

  private void put_withPutIf_shouldPutProperly(
      Put initialPut, Put put, boolean shouldMutate, String description) throws ExecutionException {
    if (shouldMutate) {
      assertThatCode(() -> storage.put(put)).describedAs(description).doesNotThrowAnyException();
    } else {
      assertThatThrownBy(() -> storage.put(put))
          .describedAs(description)
          .isInstanceOf(NoMutationException.class);
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

    Put expectedPut = shouldMutate ? put : initialPut;
    assertThat(result.get().isNull(COL_NAME1)).describedAs(description).isFalse();
    assertThat(result.get().getBoolean(COL_NAME1))
        .describedAs(description)
        .isEqualTo(expectedPut.getBooleanValue(COL_NAME1));
    assertThat(result.get().isNull(COL_NAME2)).describedAs(description).isFalse();
    assertThat(result.get().getInt(COL_NAME2))
        .describedAs(description)
        .isEqualTo(expectedPut.getIntValue(COL_NAME2));
    assertThat(result.get().isNull(COL_NAME3)).describedAs(description).isFalse();
    assertThat(result.get().getBigInt(COL_NAME3))
        .describedAs(description)
        .isEqualTo(expectedPut.getBigIntValue(COL_NAME3));
    assertThat(result.get().isNull(COL_NAME4)).describedAs(description).isFalse();
    assertThat(result.get().getFloat(COL_NAME4))
        .describedAs(description)
        .isEqualTo(expectedPut.getFloatValue(COL_NAME4));
    assertThat(result.get().isNull(COL_NAME5)).describedAs(description).isFalse();
    assertThat(result.get().getDouble(COL_NAME5))
        .describedAs(description)
        .isEqualTo(expectedPut.getDoubleValue(COL_NAME5));
    assertThat(result.get().getText(COL_NAME6))
        .describedAs(description)
        .isEqualTo(expectedPut.getTextValue(COL_NAME6));
    assertThat(result.get().getBlob(COL_NAME7))
        .describedAs(description)
        .isEqualTo(expectedPut.getBlobValue(COL_NAME7));
  }

  @Test
  public void put_withPutIfExistsWhenRecordExists_shouldPutProperly() throws ExecutionException {
    RANDOM.setSeed(seed);

    // Arrange
    putInitialData();

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
    Put initialPut = putInitialData();

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
    assertThat(result.get().getBoolean(COL_NAME1)).isEqualTo(initialPut.getBooleanValue(COL_NAME1));
    assertThat(result.get().getInt(COL_NAME2)).isEqualTo(initialPut.getIntValue(COL_NAME2));

    assertThat(result.get().getBigInt(COL_NAME3)).isEqualTo(initialPut.getBigIntValue(COL_NAME3));

    assertThat(result.get().getFloat(COL_NAME4)).isEqualTo(initialPut.getFloatValue(COL_NAME4));

    assertThat(result.get().getDouble(COL_NAME5)).isEqualTo(initialPut.getDoubleValue(COL_NAME5));
    assertThat(result.get().getText(COL_NAME6)).isEqualTo(initialPut.getTextValue(COL_NAME6));
    assertThat(result.get().getBlob(COL_NAME7)).isEqualTo(initialPut.getBlobValue(COL_NAME7));
  }

  @Test
  public void delete_withDeleteIfWithSingleCondition_shouldPutProperly() throws ExecutionException {
    RANDOM.setSeed(seed);

    for (OperatorAndDataType operatorAndDataType : operatorAndDataTypeList) {
      delete_withDeleteIfWithSingleConditionWithSameValue_shouldPutProperly(
          operatorAndDataType.getOperator(), operatorAndDataType.getDataType());
      delete_withDeleteWithSingleConditionWithRandomValue_shouldPutProperly(
          operatorAndDataType.getOperator(), operatorAndDataType.getDataType());
    }
  }

  private void delete_withDeleteIfWithSingleConditionWithSameValue_shouldPutProperly(
      Operator operator, DataType dataType) throws ExecutionException {
    // Arrange
    Put initialPut = putInitialData();

    String columnName = getColumnName(dataType);
    Value<?> value = getInitialValue(initialPut, dataType).copyWith("");
    MutationCondition condition =
        ConditionBuilder.deleteIf(getConditionalExpression(columnName, operator, value)).build();
    boolean shouldMutate = shouldMutate(initialPut, operator, dataType, value);

    Delete delete = prepareDelete().withCondition(condition);

    // Act Assert
    delete_withDeleteIf_shouldPutProperly(
        initialPut,
        delete,
        shouldMutate,
        description(initialPut, delete, columnName, operator, value));
  }

  private void delete_withDeleteWithSingleConditionWithRandomValue_shouldPutProperly(
      Operator operator, DataType dataType) throws ExecutionException {
    for (int i = 0; i < ATTEMPT_COUNT; i++) {
      // Arrange
      Put initialPut = putInitialData();

      String columnName = getColumnName(dataType);
      Value<?> value = getRandomValue(RANDOM, "", dataType);
      MutationCondition condition =
          ConditionBuilder.deleteIf(getConditionalExpression(columnName, operator, value)).build();
      boolean shouldMutate = shouldMutate(initialPut, operator, dataType, value);

      Delete delete = prepareDelete().withCondition(condition);

      // Act Assert
      delete_withDeleteIf_shouldPutProperly(
          initialPut,
          delete,
          shouldMutate,
          description(initialPut, delete, columnName, operator, value));
    }
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
    Put initialPut = putInitialData();

    String firstColumnName = getColumnName(firstDataType);
    Value<?> firstValue = getInitialValue(initialPut, firstDataType).copyWith("");

    String secondColumnName = getColumnName(secondDataType);
    Value<?> secondValue = getInitialValue(initialPut, secondDataType).copyWith("");

    MutationCondition condition =
        ConditionBuilder.deleteIf(
                getConditionalExpression(firstColumnName, firstOperator, firstValue))
            .and(getConditionalExpression(secondColumnName, secondOperator, secondValue))
            .build();

    boolean shouldMutate =
        shouldMutate(initialPut, firstOperator, firstDataType, firstValue)
            && shouldMutate(initialPut, secondOperator, secondDataType, secondValue);

    Delete delete = prepareDelete().withCondition(condition);

    // Act Assert
    delete_withDeleteIf_shouldPutProperly(
        initialPut,
        delete,
        shouldMutate,
        description(
            initialPut,
            delete,
            firstColumnName,
            firstOperator,
            firstValue,
            secondColumnName,
            secondOperator,
            secondValue));
  }

  private void delete_withDeleteIfWithMultipleConditionsWithRandomValue_shouldPutProperly(
      Operator firstOperator,
      DataType firstDataType,
      Operator secondOperator,
      DataType secondDataType)
      throws ExecutionException {
    for (int i = 0; i < ATTEMPT_COUNT; i++) {
      // Arrange
      Put initialPut = putInitialData();

      String firstColumnName = getColumnName(firstDataType);
      Value<?> firstValue = getRandomValue(RANDOM, "", firstDataType);

      String secondColumnName = getColumnName(secondDataType);
      Value<?> secondValue = getRandomValue(RANDOM, "", secondDataType);

      MutationCondition condition =
          ConditionBuilder.deleteIf(
                  getConditionalExpression(firstColumnName, firstOperator, firstValue))
              .and(getConditionalExpression(secondColumnName, secondOperator, secondValue))
              .build();

      boolean shouldMutate =
          shouldMutate(initialPut, firstOperator, firstDataType, firstValue)
              && shouldMutate(initialPut, secondOperator, secondDataType, secondValue);

      Delete delete = prepareDelete().withCondition(condition);

      // Act Assert
      delete_withDeleteIf_shouldPutProperly(
          initialPut,
          delete,
          shouldMutate,
          description(
              initialPut,
              delete,
              firstColumnName,
              firstOperator,
              firstValue,
              secondColumnName,
              secondOperator,
              secondValue));
    }
  }

  private void delete_withDeleteIf_shouldPutProperly(
      Put initialPut, Delete delete, boolean shouldMutate, String description)
      throws ExecutionException {
    if (shouldMutate) {
      assertThatCode(() -> storage.delete(delete))
          .describedAs(description)
          .doesNotThrowAnyException();

      Optional<Result> result = storage.get(prepareGet());
      assertThat(result).describedAs(description).isNotPresent();
    } else {
      assertThatThrownBy(() -> storage.delete(delete))
          .describedAs(description)
          .isInstanceOf(NoMutationException.class);

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

      assertThat(result.get().getBoolean(COL_NAME1))
          .describedAs(description)
          .isEqualTo(initialPut.getBooleanValue(COL_NAME1));
      assertThat(result.get().getInt(COL_NAME2))
          .describedAs(description)
          .isEqualTo(initialPut.getIntValue(COL_NAME2));
      assertThat(result.get().getBigInt(COL_NAME3))
          .describedAs(description)
          .isEqualTo(initialPut.getBigIntValue(COL_NAME3));
      assertThat(result.get().getFloat(COL_NAME4))
          .describedAs(description)
          .isEqualTo(initialPut.getFloatValue(COL_NAME4));
      assertThat(result.get().getDouble(COL_NAME5))
          .describedAs(description)
          .isEqualTo(initialPut.getDoubleValue(COL_NAME5));
      assertThat(result.get().getText(COL_NAME6))
          .describedAs(description)
          .isEqualTo(initialPut.getTextValue(COL_NAME6));
      assertThat(result.get().getBlob(COL_NAME7))
          .describedAs(description)
          .isEqualTo(initialPut.getBlobValue(COL_NAME7));
    }
  }

  @Test
  public void delete_withDeleteIfExistsWhenRecordExists_shouldPutProperly()
      throws ExecutionException {
    RANDOM.setSeed(seed);

    // Arrange
    putInitialData();

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
        .withValue(getRandomValue(RANDOM, COL_NAME1, DataType.BOOLEAN))
        .withValue(getRandomValue(RANDOM, COL_NAME2, DataType.INT))
        .withValue(getRandomValue(RANDOM, COL_NAME3, DataType.BIGINT))
        .withValue(getRandomValue(RANDOM, COL_NAME4, DataType.FLOAT))
        .withValue(getRandomValue(RANDOM, COL_NAME5, DataType.DOUBLE))
        .withValue(getRandomValue(RANDOM, COL_NAME6, DataType.TEXT))
        .withValue(getRandomValue(RANDOM, COL_NAME7, DataType.BLOB))
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

  private Put putInitialData() throws ExecutionException {
    try {
      storage.delete(prepareDelete().withCondition(ConditionBuilder.deleteIfExists()));
    } catch (NoMutationException ignored) {
      // ignored
    }

    Put initialPut = preparePutWithRandomValues().withCondition(ConditionBuilder.putIfNotExists());
    storage.put(initialPut);
    return initialPut;
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

  private ConditionalExpression getConditionalExpression(
      String columnName, Operator operator, Value<?> value) {
    return new ConditionalExpression(columnName, value, operator);
  }

  private boolean shouldMutate(
      Put initialPut, Operator operator, DataType dataType, Value<?> value) {
    Value<?> initialValue = getInitialValue(initialPut, dataType).copyWith("");
    switch (operator) {
      case EQ:
        return Ordering.natural().compare(initialValue, value) == 0;
      case NE:
        return Ordering.natural().compare(initialValue, value) != 0;
      case GT:
        return Ordering.natural().compare(initialValue, value) > 0;
      case GTE:
        return Ordering.natural().compare(initialValue, value) >= 0;
      case LT:
        return Ordering.natural().compare(initialValue, value) < 0;
      case LTE:
        return Ordering.natural().compare(initialValue, value) <= 0;
      default:
        throw new AssertionError();
    }
  }

  private Value<?> getInitialValue(Put initialPut, DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return initialPut.getValues().get(COL_NAME1);
      case INT:
        return initialPut.getValues().get(COL_NAME2);
      case BIGINT:
        return initialPut.getValues().get(COL_NAME3);
      case FLOAT:
        return initialPut.getValues().get(COL_NAME4);
      case DOUBLE:
        return initialPut.getValues().get(COL_NAME5);
      case TEXT:
        return initialPut.getValues().get(COL_NAME6);
      case BLOB:
        return initialPut.getValues().get(COL_NAME7);
      default:
        throw new AssertionError();
    }
  }

  protected Value<?> getRandomValue(Random random, String columnName, DataType dataType) {
    return TestUtils.getRandomValue(random, columnName, dataType, true);
  }

  private String description(
      Put initialPut, Mutation mutation, String columnName, Operator operator, Value<?> value) {
    return String.format(
        "initialPut: %s, mutation: %s, columnName: %s, operator: %s, value: %s",
        initialPut, mutation, columnName, operator, value);
  }

  private String description(
      Put initialPut,
      Mutation mutation,
      String firstColumnName,
      Operator firstOperator,
      Value<?> firstValue,
      String secondColumnName,
      Operator secondOperator,
      Value<?> secondValue) {
    return String.format(
        "initialPut: %s, mutation: %s, firstColumnName: %s, firstOperator: %s, firstValue: %s, "
            + "secondColumnName: %s, secondOperator: %s, secondValue: %s",
        initialPut,
        mutation,
        firstColumnName,
        firstOperator,
        firstValue,
        secondColumnName,
        secondOperator,
        secondValue);
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
