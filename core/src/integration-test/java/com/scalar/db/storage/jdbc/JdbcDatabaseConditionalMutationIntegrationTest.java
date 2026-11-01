package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.DistributedStorageConditionalMutationIntegrationTestBase;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.util.TestUtils;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

/**
 * For the Spanner emulator test, see {@link
 * JdbcDatabaseConditionalMutationIntegrationTestWithSpanner}
 */
@DisabledIf("com.scalar.db.storage.jdbc.JdbcEnv#isSpannerEmulator")
public class JdbcDatabaseConditionalMutationIntegrationTest
    extends DistributedStorageConditionalMutationIntegrationTestBase {

  private static final String PARTITION_KEY = "pkey";
  private static final String INT_COLUMN = "c2";
  private static final String TEXT_COLUMN = "c6";
  private static final String TABLE_WITH_ONLY_PRIMARY_KEY_COLUMNS = "tbl_pk_only";
  private static final String CLUSTERING_KEY = "ckey";

  private RdbEngineStrategy rdbEngine;
  private JdbcAdminTestUtils jdbcAdminTestUtils;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
    if (JdbcEnv.isYugabyte() && jdbcAdminTestUtils == null) {
      jdbcAdminTestUtils = new JdbcAdminTestUtils(properties);
    }
    return properties;
  }

  @AfterAll
  void closeJdbcAdminTestUtils() throws Exception {
    if (jdbcAdminTestUtils != null) {
      jdbcAdminTestUtils.close();
    }
  }

  @Override
  protected void truncateTable() throws ExecutionException {
    // Use DML DELETE for YugabyteDB: TRUNCATE is DDL that conflicts with table locking.
    // This only affects @BeforeEach cleanup. The actual truncateTable() API is tested in admin ITs.
    if (JdbcEnv.isYugabyte()) {
      jdbcAdminTestUtils.deleteAllRowsWithSql(getNamespace(), TABLE);
      return;
    }
    super.truncateTable();
  }

  @Override
  protected int getThreadNum() {
    if (JdbcTestUtils.isMysql(rdbEngine)) {
      // Since Deadlock error sometimes happens in MySQL, change the concurrency to 1
      return 1;
    }
    return super.getThreadNum();
  }

  @Override
  protected Column<?> getColumnWithRandomValue(
      Random random, String columnName, DataType dataType) {
    if (JdbcTestUtils.isOracle(rdbEngine)) {
      if (dataType == DataType.DOUBLE) {
        return JdbcTestUtils.getRandomOracleDoubleColumn(random, columnName);
      }
      // don't allow empty value since Oracle treats empty value as NULL
      return TestUtils.getColumnWithRandomValue(random, columnName, dataType, false);
    }
    return super.getColumnWithRandomValue(random, columnName, dataType);
  }

  @Override
  protected boolean isConditionOnBlobColumnSupported() {
    return !JdbcTestUtils.isOracle(rdbEngine);
  }

  @Test
  public void put_withPutIfExistsWithoutValuesWhenRecordExists_shouldNotChangeValues()
      throws ExecutionException {
    // Arrange
    putRecord(1, "c6Value");
    Put put =
        Put.newBuilder()
            .namespace(getNamespace())
            .table(TABLE)
            .partitionKey(Key.ofText(PARTITION_KEY, "pkeyValue"))
            .condition(ConditionBuilder.putIfExists())
            .build();

    // Act
    storage.put(put);

    // Assert
    assertRecord(1, "c6Value");
  }

  @Test
  public void
      put_withPutIfExistsWithoutValuesWhenRecordDoesNotExist_shouldThrowNoMutationException()
          throws ExecutionException {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace(getNamespace())
            .table(TABLE)
            .partitionKey(Key.ofText(PARTITION_KEY, "pkeyValue"))
            .condition(ConditionBuilder.putIfExists())
            .build();

    // Act Assert
    assertThatThrownBy(() -> storage.put(put)).isInstanceOf(NoMutationException.class);
    assertThat(storage.get(prepareGetForRecord())).isNotPresent();
  }

  @Test
  public void put_withPutIfWithoutValuesWhenConditionIsSatisfied_shouldNotChangeValues()
      throws ExecutionException {
    // Arrange
    putRecord(1, "c6Value");
    Put put =
        Put.newBuilder()
            .namespace(getNamespace())
            .table(TABLE)
            .partitionKey(Key.ofText(PARTITION_KEY, "pkeyValue"))
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column(INT_COLUMN).isEqualToInt(1)).build())
            .build();

    // Act
    storage.put(put);

    // Assert
    assertRecord(1, "c6Value");
  }

  @Test
  public void put_withPutIfWithoutValuesWhenConditionIsNotSatisfied_shouldThrowNoMutationException()
      throws ExecutionException {
    // Arrange
    putRecord(1, "c6Value");
    Put put =
        Put.newBuilder()
            .namespace(getNamespace())
            .table(TABLE)
            .partitionKey(Key.ofText(PARTITION_KEY, "pkeyValue"))
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column(INT_COLUMN).isEqualToInt(2)).build())
            .build();

    // Act Assert
    assertThatThrownBy(() -> storage.put(put)).isInstanceOf(NoMutationException.class);
    assertRecord(1, "c6Value");
  }

  @Test
  @DisabledIf(
      value = "isSpanner",
      disabledReason =
          "Spanner rejects an UPDATE statement that assigns a primary key column, even to itself, so a"
              + " conditional put that specifies only primary key values fails for a table with only"
              + " primary key columns")
  public void
      put_withPutIfExistsWithoutValuesForTableWithOnlyPrimaryKeyColumns_shouldPutOnlyWhenRecordExists()
          throws ExecutionException {
    // Arrange
    admin.createTable(
        getNamespace(),
        TABLE_WITH_ONLY_PRIMARY_KEY_COLUMNS,
        TableMetadata.newBuilder()
            .addColumn(PARTITION_KEY, DataType.TEXT)
            .addColumn(CLUSTERING_KEY, DataType.INT)
            .addPartitionKey(PARTITION_KEY)
            .addClusteringKey(CLUSTERING_KEY)
            .build(),
        true,
        getCreationOptions());
    try {
      storage.put(
          Put.newBuilder()
              .namespace(getNamespace())
              .table(TABLE_WITH_ONLY_PRIMARY_KEY_COLUMNS)
              .partitionKey(Key.ofText(PARTITION_KEY, "pkeyValue"))
              .clusteringKey(Key.ofInt(CLUSTERING_KEY, 1))
              .condition(ConditionBuilder.putIfNotExists())
              .build());
      Put putForExistingRecord =
          Put.newBuilder()
              .namespace(getNamespace())
              .table(TABLE_WITH_ONLY_PRIMARY_KEY_COLUMNS)
              .partitionKey(Key.ofText(PARTITION_KEY, "pkeyValue"))
              .clusteringKey(Key.ofInt(CLUSTERING_KEY, 1))
              .condition(ConditionBuilder.putIfExists())
              .build();
      Put putForNonExistingRecord =
          Put.newBuilder()
              .namespace(getNamespace())
              .table(TABLE_WITH_ONLY_PRIMARY_KEY_COLUMNS)
              .partitionKey(Key.ofText(PARTITION_KEY, "pkeyValue"))
              .clusteringKey(Key.ofInt(CLUSTERING_KEY, 2))
              .condition(ConditionBuilder.putIfExists())
              .build();

      // Act Assert
      storage.put(putForExistingRecord);
      assertThatThrownBy(() -> storage.put(putForNonExistingRecord))
          .isInstanceOf(NoMutationException.class);
      assertThat(storage.get(prepareGetForTableWithOnlyPrimaryKeyColumns(1))).isPresent();
      assertThat(storage.get(prepareGetForTableWithOnlyPrimaryKeyColumns(2))).isNotPresent();
    } finally {
      admin.dropTable(getNamespace(), TABLE_WITH_ONLY_PRIMARY_KEY_COLUMNS, true);
    }
  }

  private void putRecord(int intValue, String textValue) throws ExecutionException {
    storage.put(
        Put.newBuilder()
            .namespace(getNamespace())
            .table(TABLE)
            .partitionKey(Key.ofText(PARTITION_KEY, "pkeyValue"))
            .intValue(INT_COLUMN, intValue)
            .textValue(TEXT_COLUMN, textValue)
            .condition(ConditionBuilder.putIfNotExists())
            .build());
  }

  private void assertRecord(int expectedIntValue, String expectedTextValue)
      throws ExecutionException {
    Optional<Result> result = storage.get(prepareGetForRecord());
    assertThat(result).isPresent();
    assertThat(result.get().getInt(INT_COLUMN)).isEqualTo(expectedIntValue);
    assertThat(result.get().getText(TEXT_COLUMN)).isEqualTo(expectedTextValue);
  }

  private Get prepareGetForRecord() {
    return Get.newBuilder()
        .namespace(getNamespace())
        .table(TABLE)
        .partitionKey(Key.ofText(PARTITION_KEY, "pkeyValue"))
        .build();
  }

  private Get prepareGetForTableWithOnlyPrimaryKeyColumns(int clusteringKeyValue) {
    return Get.newBuilder()
        .namespace(getNamespace())
        .table(TABLE_WITH_ONLY_PRIMARY_KEY_COLUMNS)
        .partitionKey(Key.ofText(PARTITION_KEY, "pkeyValue"))
        .clusteringKey(Key.ofInt(CLUSTERING_KEY, clusteringKeyValue))
        .build();
  }

  @SuppressWarnings("unused")
  private boolean isSpanner() {
    return JdbcEnv.isSpanner();
  }
}
