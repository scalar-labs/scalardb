package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;

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
import com.scalar.db.util.TestUtils;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DistributedStorageColumnValueIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(DistributedStorageColumnValueIntegrationTestBase.class);

  private static final String TEST_NAME = "storage_col_val";
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

  private static final int ATTEMPT_COUNT = 50;
  private static final Random random = new Random();

  private DistributedStorageAdmin admin;
  private DistributedStorage storage;
  private String namespace;

  private long seed;

  @BeforeAll
  public void beforeAll() throws Exception {
    initialize(TEST_NAME);
    StorageFactory factory = StorageFactory.create(getProperties(TEST_NAME));
    admin = factory.getAdmin();
    namespace = getNamespace();
    createTable();
    storage = factory.getStorage();
    seed = System.currentTimeMillis();
    System.out.println("The seed used in the column value integration test is " + seed);
  }

  protected void initialize(String testName) throws Exception {}

  protected abstract Properties getProperties(String testName);

  protected String getNamespace() {
    return NAMESPACE;
  }

  private void createTable() throws ExecutionException {
    Map<String, String> options = getCreationOptions();
    admin.createNamespace(namespace, true, options);
    admin.createTable(
        namespace,
        TABLE,
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
            .build(),
        true,
        options);
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

  @Test
  public void put_WithRandomValues_ShouldPutCorrectly() throws ExecutionException {
    random.setSeed(seed);

    for (int i = 0; i < ATTEMPT_COUNT; i++) {
      // Arrange
      IntColumn partitionKeyValue =
          (IntColumn) getColumnWithRandomValue(random, PARTITION_KEY, DataType.INT);
      BooleanColumn col1Value =
          (BooleanColumn) getColumnWithRandomValue(random, COL_NAME1, DataType.BOOLEAN);
      IntColumn col2Value = (IntColumn) getColumnWithRandomValue(random, COL_NAME2, DataType.INT);
      BigIntColumn col3Value =
          (BigIntColumn) getColumnWithRandomValue(random, COL_NAME3, DataType.BIGINT);
      FloatColumn col4Value =
          (FloatColumn) getColumnWithRandomValue(random, COL_NAME4, DataType.FLOAT);
      DoubleColumn col5Value =
          (DoubleColumn) getColumnWithRandomValue(random, COL_NAME5, DataType.DOUBLE);
      TextColumn col6Value =
          (TextColumn) getColumnWithRandomValue(random, COL_NAME6, DataType.TEXT);
      BlobColumn col7Value =
          (BlobColumn) getColumnWithRandomValue(random, COL_NAME7, DataType.BLOB);
      Put put =
          Put.newBuilder()
              .namespace(namespace)
              .table(TABLE)
              .partitionKey(Key.newBuilder().add(partitionKeyValue).build())
              .value(col1Value)
              .value(col2Value)
              .value(col3Value)
              .value(col4Value)
              .value(col5Value)
              .value(col6Value)
              .value(col7Value)
              .build();
      // Act
      storage.put(put);

      // Assert
      assertResult(
          partitionKeyValue,
          col1Value,
          col2Value,
          col3Value,
          col4Value,
          col5Value,
          col6Value,
          col7Value);
    }
  }

  @Test
  public void put_WithMaxValues_ShouldPutCorrectly() throws ExecutionException {
    // Arrange
    IntColumn partitionKeyValue = (IntColumn) getColumnWithMaxValue(PARTITION_KEY, DataType.INT);
    BooleanColumn col1Value = (BooleanColumn) getColumnWithMaxValue(COL_NAME1, DataType.BOOLEAN);
    IntColumn col2Value = (IntColumn) getColumnWithMaxValue(COL_NAME2, DataType.INT);
    BigIntColumn col3Value = (BigIntColumn) getColumnWithMaxValue(COL_NAME3, DataType.BIGINT);
    FloatColumn col4Value = (FloatColumn) getColumnWithMaxValue(COL_NAME4, DataType.FLOAT);
    DoubleColumn col5Value = (DoubleColumn) getColumnWithMaxValue(COL_NAME5, DataType.DOUBLE);
    TextColumn col6Value = (TextColumn) getColumnWithMaxValue(COL_NAME6, DataType.TEXT);
    BlobColumn col7Value = (BlobColumn) getColumnWithMaxValue(COL_NAME7, DataType.BLOB);

    Put put =
        Put.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(Key.newBuilder().add(partitionKeyValue).build())
            .value(col1Value)
            .value(col2Value)
            .value(col3Value)
            .value(col4Value)
            .value(col5Value)
            .value(col6Value)
            .value(col7Value)
            .build();
    // Act
    storage.put(put);

    // Assert
    assertResult(
        partitionKeyValue,
        col1Value,
        col2Value,
        col3Value,
        col4Value,
        col5Value,
        col6Value,
        col7Value);
  }

  @Test
  public void put_WithMinValues_ShouldPutCorrectly() throws ExecutionException {
    // Arrange
    IntColumn partitionKeyValue = (IntColumn) getColumnWithMinValue(PARTITION_KEY, DataType.INT);
    BooleanColumn col1Value = (BooleanColumn) getColumnWithMinValue(COL_NAME1, DataType.BOOLEAN);
    IntColumn col2Value = (IntColumn) getColumnWithMinValue(COL_NAME2, DataType.INT);
    BigIntColumn col3Value = (BigIntColumn) getColumnWithMinValue(COL_NAME3, DataType.BIGINT);
    FloatColumn col4Value = (FloatColumn) getColumnWithMinValue(COL_NAME4, DataType.FLOAT);
    DoubleColumn col5Value = (DoubleColumn) getColumnWithMinValue(COL_NAME5, DataType.DOUBLE);
    TextColumn col6Value = (TextColumn) getColumnWithMinValue(COL_NAME6, DataType.TEXT);
    BlobColumn col7Value = (BlobColumn) getColumnWithMinValue(COL_NAME7, DataType.BLOB);

    Put put =
        Put.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(Key.newBuilder().add(partitionKeyValue).build())
            .value(col1Value)
            .value(col2Value)
            .value(col3Value)
            .value(col4Value)
            .value(col5Value)
            .value(col6Value)
            .value(col7Value)
            .build();
    // Act
    storage.put(put);

    // Assert
    assertResult(
        partitionKeyValue,
        col1Value,
        col2Value,
        col3Value,
        col4Value,
        col5Value,
        col6Value,
        col7Value);
  }

  @Test
  public void put_WithNullValues_ShouldPutCorrectly() throws ExecutionException {
    // Arrange
    IntColumn partitionKeyValue = IntColumn.of(PARTITION_KEY, 1);
    BooleanColumn col1Value = BooleanColumn.ofNull(COL_NAME1);
    IntColumn col2Value = IntColumn.ofNull(COL_NAME2);
    BigIntColumn col3Value = BigIntColumn.ofNull(COL_NAME3);
    FloatColumn col4Value = FloatColumn.ofNull(COL_NAME4);
    DoubleColumn col5Value = DoubleColumn.ofNull(COL_NAME5);
    TextColumn col6Value = TextColumn.ofNull(COL_NAME6);
    BlobColumn col7Value = BlobColumn.ofNull(COL_NAME7);

    Put put =
        Put.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(Key.newBuilder().add(partitionKeyValue).build())
            .value(col1Value)
            .value(col2Value)
            .value(col3Value)
            .value(col4Value)
            .value(col5Value)
            .value(col6Value)
            .value(col7Value)
            .build();
    // Act
    storage.put(put);

    // Assert
    assertResult(
        partitionKeyValue,
        col1Value,
        col2Value,
        col3Value,
        col4Value,
        col5Value,
        col6Value,
        col7Value);
  }

  @Test
  public void put_WithNullValues_AfterPuttingRandomValues_ShouldPutCorrectly()
      throws ExecutionException {
    // Arrange
    IntColumn partitionKeyValue = IntColumn.of(PARTITION_KEY, 1);
    BooleanColumn col1Value = BooleanColumn.ofNull(COL_NAME1);
    IntColumn col2Value = IntColumn.ofNull(COL_NAME2);
    BigIntColumn col3Value = BigIntColumn.ofNull(COL_NAME3);
    FloatColumn col4Value = FloatColumn.ofNull(COL_NAME4);
    DoubleColumn col5Value = DoubleColumn.ofNull(COL_NAME5);
    TextColumn col6Value = TextColumn.ofNull(COL_NAME6);
    BlobColumn col7Value = BlobColumn.ofNull(COL_NAME7);

    Put putForRandomValues =
        Put.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(Key.newBuilder().add(partitionKeyValue).build())
            .value(getColumnWithRandomValue(random, COL_NAME1, DataType.BOOLEAN))
            .value(getColumnWithRandomValue(random, COL_NAME2, DataType.INT))
            .value(getColumnWithRandomValue(random, COL_NAME3, DataType.BIGINT))
            .value(getColumnWithRandomValue(random, COL_NAME4, DataType.FLOAT))
            .value(getColumnWithRandomValue(random, COL_NAME5, DataType.DOUBLE))
            .value(getColumnWithRandomValue(random, COL_NAME6, DataType.TEXT))
            .value(getColumnWithRandomValue(random, COL_NAME7, DataType.BLOB))
            .build();
    Put putForNullValues =
        Put.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(Key.newBuilder().add(partitionKeyValue).build())
            .value(col1Value)
            .value(col2Value)
            .value(col3Value)
            .value(col4Value)
            .value(col5Value)
            .value(col6Value)
            .value(col7Value)
            .build();

    // Act
    storage.put(putForRandomValues);
    storage.put(putForNullValues);

    // Assert
    assertResult(
        partitionKeyValue,
        col1Value,
        col2Value,
        col3Value,
        col4Value,
        col5Value,
        col6Value,
        col7Value);
  }

  @Test
  public void put_WithoutValues_ShouldPutCorrectly() throws ExecutionException {
    // Arrange
    IntColumn partitionKeyValue = IntColumn.of(PARTITION_KEY, 1);
    BooleanColumn col1Value = BooleanColumn.ofNull(COL_NAME1);
    IntColumn col2Value = IntColumn.ofNull(COL_NAME2);
    BigIntColumn col3Value = BigIntColumn.ofNull(COL_NAME3);
    FloatColumn col4Value = FloatColumn.ofNull(COL_NAME4);
    DoubleColumn col5Value = DoubleColumn.ofNull(COL_NAME5);
    TextColumn col6Value = TextColumn.ofNull(COL_NAME6);
    BlobColumn col7Value = BlobColumn.ofNull(COL_NAME7);

    Put put =
        Put.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(Key.newBuilder().add(partitionKeyValue).build())
            .build();

    // Act
    storage.put(put);

    // Assert
    assertResult(
        partitionKeyValue,
        col1Value,
        col2Value,
        col3Value,
        col4Value,
        col5Value,
        col6Value,
        col7Value);
  }

  private void assertResult(
      IntColumn partitionKeyValue,
      BooleanColumn col1Value,
      IntColumn col2Value,
      BigIntColumn col3Value,
      FloatColumn col4Value,
      DoubleColumn col5Value,
      TextColumn col6Value,
      BlobColumn col7Value)
      throws ExecutionException {
    Optional<Result> actualOpt =
        storage.get(
            Get.newBuilder()
                .namespace(namespace)
                .table(TABLE)
                .partitionKey(
                    Key.ofInt(partitionKeyValue.getName(), partitionKeyValue.getIntValue()))
                .build());

    assertThat(actualOpt).isPresent();
    Result actual = actualOpt.get();

    assertThat(actual.contains(PARTITION_KEY)).isTrue();
    assertThat(actual.getColumns().get(PARTITION_KEY)).isEqualTo(partitionKeyValue);
    assertThat(actual.contains(COL_NAME1)).isTrue();
    assertThat(actual.getColumns().get(COL_NAME1)).isEqualTo(col1Value);
    assertThat(actual.contains(COL_NAME2)).isTrue();
    assertThat(actual.getColumns().get(COL_NAME2)).isEqualTo(col2Value);
    assertThat(actual.contains(COL_NAME3)).isTrue();
    assertThat(actual.getColumns().get(COL_NAME3)).isEqualTo(col3Value);
    assertThat(actual.contains(COL_NAME4)).isTrue();
    assertThat(actual.getColumns().get(COL_NAME4)).isEqualTo(col4Value);
    assertThat(actual.contains(COL_NAME5)).isTrue();
    assertThat(actual.getColumns().get(COL_NAME5)).isEqualTo(col5Value);
    assertThat(actual.contains(COL_NAME6)).isTrue();
    assertThat(actual.getColumns().get(COL_NAME6)).isEqualTo(col6Value);
    assertThat(actual.contains(COL_NAME7)).isTrue();
    assertThat(actual.getColumns().get(COL_NAME7)).isEqualTo(col7Value);

    assertThat(actual.getContainedColumnNames())
        .containsExactlyInAnyOrder(
            PARTITION_KEY,
            COL_NAME1,
            COL_NAME2,
            COL_NAME3,
            COL_NAME4,
            COL_NAME5,
            COL_NAME6,
            COL_NAME7);

    assertThat(actual.contains(PARTITION_KEY)).isTrue();
    assertThat(actual.isNull(PARTITION_KEY)).isFalse();
    assertThat(actual.getInt(PARTITION_KEY)).isEqualTo(partitionKeyValue.getIntValue());
    assertThat(actual.getAsObject(PARTITION_KEY)).isEqualTo(partitionKeyValue.getIntValue());

    assertThat(actual.contains(COL_NAME1)).isTrue();
    assertThat(actual.isNull(COL_NAME1)).isEqualTo(col1Value.hasNullValue());
    assertThat(actual.getBoolean(COL_NAME1)).isEqualTo(col1Value.getBooleanValue());
    assertThat(actual.getAsObject(COL_NAME1))
        .isEqualTo(col1Value.hasNullValue() ? null : col1Value.getBooleanValue());

    assertThat(actual.contains(COL_NAME2)).isTrue();
    assertThat(actual.isNull(COL_NAME2)).isEqualTo(col2Value.hasNullValue());
    assertThat(actual.getInt(COL_NAME2)).isEqualTo(col2Value.getIntValue());
    assertThat(actual.getAsObject(COL_NAME2))
        .isEqualTo(col2Value.hasNullValue() ? null : col2Value.getIntValue());

    assertThat(actual.contains(COL_NAME3)).isTrue();
    assertThat(actual.isNull(COL_NAME3)).isEqualTo(col3Value.hasNullValue());
    assertThat(actual.getBigInt(COL_NAME3)).isEqualTo(col3Value.getBigIntValue());
    assertThat(actual.getAsObject(COL_NAME3))
        .isEqualTo(col3Value.hasNullValue() ? null : col3Value.getBigIntValue());

    assertThat(actual.contains(COL_NAME4)).isTrue();
    assertThat(actual.isNull(COL_NAME4)).isEqualTo(col4Value.hasNullValue());
    assertThat(actual.getFloat(COL_NAME4)).isEqualTo(col4Value.getFloatValue());
    assertThat(actual.getAsObject(COL_NAME4))
        .isEqualTo(col4Value.hasNullValue() ? null : col4Value.getFloatValue());

    assertThat(actual.contains(COL_NAME5)).isTrue();
    assertThat(actual.isNull(COL_NAME5)).isEqualTo(col5Value.hasNullValue());
    assertThat(actual.getDouble(COL_NAME5)).isEqualTo(col5Value.getDoubleValue());
    assertThat(actual.getAsObject(COL_NAME5))
        .isEqualTo(col5Value.hasNullValue() ? null : col5Value.getDoubleValue());

    assertThat(actual.contains(COL_NAME6)).isTrue();
    assertThat(actual.isNull(COL_NAME6)).isEqualTo(col6Value.hasNullValue());
    assertThat(actual.getText(COL_NAME6)).isEqualTo(col6Value.getTextValue());
    assertThat(actual.getAsObject(COL_NAME6)).isEqualTo(col6Value.getTextValue());

    assertThat(actual.contains(COL_NAME7)).isTrue();
    assertThat(actual.isNull(COL_NAME7)).isEqualTo(col7Value.hasNullValue());
    assertThat(actual.getBlob(COL_NAME7)).isEqualTo(col7Value.getBlobValue());
    assertThat(actual.getBlobAsByteBuffer(COL_NAME7)).isEqualTo(col7Value.getBlobValue());
    assertThat(actual.getBlobAsBytes(COL_NAME7)).isEqualTo(col7Value.getBlobValueAsBytes());
    assertThat(actual.getAsObject(COL_NAME7)).isEqualTo(col7Value.getBlobValueAsByteBuffer());
  }

  protected Column<?> getColumnWithRandomValue(
      Random random, String columnName, DataType dataType) {
    return TestUtils.getColumnWithRandomValue(random, columnName, dataType, true);
  }

  protected Column<?> getColumnWithMinValue(String columnName, DataType dataType) {
    return TestUtils.getColumnWithMinValue(columnName, dataType, true);
  }

  protected Column<?> getColumnWithMaxValue(String columnName, DataType dataType) {
    return TestUtils.getColumnWithMaxValue(columnName, dataType);
  }
}
