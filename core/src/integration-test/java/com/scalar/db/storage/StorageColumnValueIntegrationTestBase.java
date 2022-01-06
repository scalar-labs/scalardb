package com.scalar.db.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import com.scalar.db.service.StorageFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

@SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
public abstract class StorageColumnValueIntegrationTestBase {

  private static final String TEST_NAME = "col_val";
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

  private static final int ATTEMPT_COUNT = 50;
  private static final Random RANDOM = new Random();

  private static boolean initialized;
  private static DistributedStorageAdmin admin;
  private static DistributedStorage storage;
  private static String namespace;

  private static long seed;

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
      System.out.println("The seed used in the column value integration test is " + seed);
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

  @Test
  public void put_WithRandomValues_ShouldPutCorrectly() throws ExecutionException {
    RANDOM.setSeed(seed);

    for (int i = 0; i < ATTEMPT_COUNT; i++) {
      // Arrange
      IntValue partitionKeyValue = (IntValue) getRandomValue(RANDOM, PARTITION_KEY, DataType.INT);
      BooleanValue col1Value = (BooleanValue) getRandomValue(RANDOM, COL_NAME1, DataType.BOOLEAN);
      IntValue col2Value = (IntValue) getRandomValue(RANDOM, COL_NAME2, DataType.INT);
      BigIntValue col3Value = (BigIntValue) getRandomValue(RANDOM, COL_NAME3, DataType.BIGINT);
      FloatValue col4Value = (FloatValue) getRandomValue(RANDOM, COL_NAME4, DataType.FLOAT);
      DoubleValue col5Value = (DoubleValue) getRandomValue(RANDOM, COL_NAME5, DataType.DOUBLE);
      TextValue col6Value = (TextValue) getRandomValue(RANDOM, COL_NAME6, DataType.TEXT);
      BlobValue col7Value = (BlobValue) getRandomValue(RANDOM, COL_NAME7, DataType.BLOB);

      Put put =
          new Put(new Key(partitionKeyValue))
              .withValue(col1Value)
              .withValue(col2Value)
              .withValue(col3Value)
              .withValue(col4Value)
              .withValue(col5Value)
              .withValue(col6Value)
              .withValue(col7Value)
              .forNamespace(namespace)
              .forTable(TABLE);

      // Act
      storage.put(put);

      // Assert
      Optional<Result> actual =
          storage.get(new Get(new Key(partitionKeyValue)).forNamespace(namespace).forTable(TABLE));
      assertThat(actual).isPresent();
      assertThat(actual.get().getValue(PARTITION_KEY).isPresent()).isTrue();
      assertThat(actual.get().getValue(PARTITION_KEY).get()).isEqualTo(partitionKeyValue);
      assertThat(actual.get().getValue(COL_NAME1).isPresent()).isTrue();
      assertThat(actual.get().getValue(COL_NAME1).get()).isEqualTo(col1Value);
      assertThat(actual.get().getValue(COL_NAME2).isPresent()).isTrue();
      assertThat(actual.get().getValue(COL_NAME2).get()).isEqualTo(col2Value);
      assertThat(actual.get().getValue(COL_NAME3).isPresent()).isTrue();
      assertThat(actual.get().getValue(COL_NAME3).get()).isEqualTo(col3Value);
      assertThat(actual.get().getValue(COL_NAME4).isPresent()).isTrue();
      assertThat(actual.get().getValue(COL_NAME4).get()).isEqualTo(col4Value);
      assertThat(actual.get().getValue(COL_NAME5).isPresent()).isTrue();
      assertThat(actual.get().getValue(COL_NAME5).get()).isEqualTo(col5Value);
      assertThat(actual.get().getValue(COL_NAME6).isPresent()).isTrue();
      assertThat(actual.get().getValue(COL_NAME6).get()).isEqualTo(col6Value);
      assertThat(actual.get().getValue(COL_NAME7).isPresent()).isTrue();
      assertThat(actual.get().getValue(COL_NAME7).get()).isEqualTo(col7Value);
    }
  }

  @Test
  public void put_WithMaxValues_ShouldPutCorrectly() throws ExecutionException {
    // Arrange
    IntValue partitionKeyValue = (IntValue) getMaxValue(PARTITION_KEY, DataType.INT);
    BooleanValue col1Value = (BooleanValue) getMaxValue(COL_NAME1, DataType.BOOLEAN);
    IntValue col2Value = (IntValue) getMaxValue(COL_NAME2, DataType.INT);
    BigIntValue col3Value = (BigIntValue) getMaxValue(COL_NAME3, DataType.BIGINT);
    FloatValue col4Value = (FloatValue) getMaxValue(COL_NAME4, DataType.FLOAT);
    DoubleValue col5Value = (DoubleValue) getMaxValue(COL_NAME5, DataType.DOUBLE);
    TextValue col6Value = (TextValue) getMaxValue(COL_NAME6, DataType.TEXT);
    BlobValue col7Value = (BlobValue) getMaxValue(COL_NAME7, DataType.BLOB);

    Put put =
        new Put(new Key(partitionKeyValue))
            .withValue(col1Value)
            .withValue(col2Value)
            .withValue(col3Value)
            .withValue(col4Value)
            .withValue(col5Value)
            .withValue(col6Value)
            .withValue(col7Value)
            .forNamespace(namespace)
            .forTable(TABLE);

    // Act
    storage.put(put);

    // Assert
    Optional<Result> actual =
        storage.get(new Get(new Key(partitionKeyValue)).forNamespace(namespace).forTable(TABLE));
    assertThat(actual).isPresent();
    assertThat(actual.get().getValue(PARTITION_KEY).isPresent()).isTrue();
    assertThat(actual.get().getValue(PARTITION_KEY).get()).isEqualTo(partitionKeyValue);
    assertThat(actual.get().getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME1).get()).isEqualTo(col1Value);
    assertThat(actual.get().getValue(COL_NAME2).isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME2).get()).isEqualTo(col2Value);
    assertThat(actual.get().getValue(COL_NAME3).isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME3).get()).isEqualTo(col3Value);
    assertThat(actual.get().getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME4).get()).isEqualTo(col4Value);
    assertThat(actual.get().getValue(COL_NAME5).isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME5).get()).isEqualTo(col5Value);
    assertThat(actual.get().getValue(COL_NAME6).isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME6).get()).isEqualTo(col6Value);
    assertThat(actual.get().getValue(COL_NAME7).isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME7).get()).isEqualTo(col7Value);
  }

  @Test
  public void put_WithMinValues_ShouldPutCorrectly() throws ExecutionException {
    // Arrange
    IntValue partitionKeyValue = (IntValue) getMinValue(PARTITION_KEY, DataType.INT);
    BooleanValue col1Value = (BooleanValue) getMinValue(COL_NAME1, DataType.BOOLEAN);
    IntValue col2Value = (IntValue) getMinValue(COL_NAME2, DataType.INT);
    BigIntValue col3Value = (BigIntValue) getMinValue(COL_NAME3, DataType.BIGINT);
    FloatValue col4Value = (FloatValue) getMinValue(COL_NAME4, DataType.FLOAT);
    DoubleValue col5Value = (DoubleValue) getMinValue(COL_NAME5, DataType.DOUBLE);
    TextValue col6Value = (TextValue) getMinValue(COL_NAME6, DataType.TEXT);
    BlobValue col7Value = (BlobValue) getMinValue(COL_NAME7, DataType.BLOB);

    Put put =
        new Put(new Key(partitionKeyValue))
            .withValue(col1Value)
            .withValue(col2Value)
            .withValue(col3Value)
            .withValue(col4Value)
            .withValue(col5Value)
            .withValue(col6Value)
            .withValue(col7Value)
            .forNamespace(namespace)
            .forTable(TABLE);

    // Act
    storage.put(put);

    // Assert
    Optional<Result> actual =
        storage.get(new Get(new Key(partitionKeyValue)).forNamespace(namespace).forTable(TABLE));
    assertThat(actual).isPresent();
    assertThat(actual.get().getValue(PARTITION_KEY).isPresent()).isTrue();
    assertThat(actual.get().getValue(PARTITION_KEY).get()).isEqualTo(partitionKeyValue);
    assertThat(actual.get().getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME1).get()).isEqualTo(col1Value);
    assertThat(actual.get().getValue(COL_NAME2).isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME2).get()).isEqualTo(col2Value);
    assertThat(actual.get().getValue(COL_NAME3).isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME3).get()).isEqualTo(col3Value);
    assertThat(actual.get().getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME4).get()).isEqualTo(col4Value);
    assertThat(actual.get().getValue(COL_NAME5).isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME5).get()).isEqualTo(col5Value);
    assertThat(actual.get().getValue(COL_NAME6).isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME6).get()).isEqualTo(col6Value);
    assertThat(actual.get().getValue(COL_NAME7).isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME7).get()).isEqualTo(col7Value);
  }

  @Test
  public void put_WithNullValues_ShouldPutCorrectly() throws ExecutionException {
    // Arrange
    IntValue partitionKeyValue = new IntValue(PARTITION_KEY, 1);
    BooleanValue col1Value = new BooleanValue(COL_NAME1, true);
    IntValue col2Value = new IntValue(COL_NAME2, 1);
    BigIntValue col3Value = new BigIntValue(COL_NAME3, 1L);
    FloatValue col4Value = new FloatValue(COL_NAME4, 1.0f);
    DoubleValue col5Value = new DoubleValue(COL_NAME5, 1.0d);
    TextValue col6Value = (TextValue) getNullValue(COL_NAME6, DataType.TEXT);
    BlobValue col7Value = (BlobValue) getNullValue(COL_NAME7, DataType.BLOB);

    Put put =
        new Put(new Key(partitionKeyValue))
            .withValue(col1Value)
            .withValue(col2Value)
            .withValue(col3Value)
            .withValue(col4Value)
            .withValue(col5Value)
            .withValue(col6Value)
            .withValue(col7Value)
            .forNamespace(namespace)
            .forTable(TABLE);

    // Act
    storage.put(put);

    // Assert
    Optional<Result> actual =
        storage.get(new Get(new Key(partitionKeyValue)).forNamespace(namespace).forTable(TABLE));
    assertThat(actual).isPresent();
    assertThat(actual.get().getValue(PARTITION_KEY).isPresent()).isTrue();
    assertThat(actual.get().getValue(PARTITION_KEY).get()).isEqualTo(partitionKeyValue);
    assertThat(actual.get().getValue(COL_NAME1).isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME1).get()).isEqualTo(col1Value);
    assertThat(actual.get().getValue(COL_NAME2).isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME2).get()).isEqualTo(col2Value);
    assertThat(actual.get().getValue(COL_NAME3).isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME3).get()).isEqualTo(col3Value);
    assertThat(actual.get().getValue(COL_NAME4).isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME4).get()).isEqualTo(col4Value);
    assertThat(actual.get().getValue(COL_NAME5).isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME5).get()).isEqualTo(col5Value);
    assertThat(actual.get().getValue(COL_NAME6).isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME6).get()).isEqualTo(col6Value);
    assertThat(actual.get().getValue(COL_NAME7).isPresent()).isTrue();
    assertThat(actual.get().getValue(COL_NAME7).get()).isEqualTo(col7Value);
  }

  protected Value<?> getRandomValue(Random random, String columnName, DataType dataType) {
    return TestUtils.getRandomValue(random, columnName, dataType, true);
  }

  protected Value<?> getMinValue(String columnName, DataType dataType) {
    return TestUtils.getMinValue(columnName, dataType, true);
  }

  protected Value<?> getMaxValue(String columnName, DataType dataType) {
    return TestUtils.getMaxValue(columnName, dataType);
  }

  protected Value<?> getNullValue(String columnName, DataType dataType) {
    return TestUtils.getNullValue(columnName, dataType);
  }
}
