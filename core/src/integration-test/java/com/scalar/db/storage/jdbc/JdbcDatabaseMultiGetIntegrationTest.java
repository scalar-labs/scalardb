package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import com.scalar.db.service.StorageFactory;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Multi-get on JDBC databases: Gets that need more than one IN list, and keys whose SQL equality
 * can differ from the equality of their values (TEXT under a collation, FLOAT and DOUBLE signed
 * zeros, BLOB and date/time values).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JdbcDatabaseMultiGetIntegrationTest {
  private static final String NAMESPACE = "int_test_multi_get";
  private static final String INT_TABLE = "int_keys";
  private static final String TEXT_TABLE = "text_keys";
  private static final String CASE_INSENSITIVE_COLLATION = "scalardb_multi_get_ci";
  private static final String KEY_TABLE_PREFIX = "keys_";

  private RdbEngineStrategy rdbEngine;
  private DistributedStorageAdmin admin;
  private DistributedStorage storage;
  private JdbcAdminTestUtils testUtils;
  private boolean caseInsensitive;

  @BeforeAll
  void setUp() throws Exception {
    Properties properties = JdbcEnv.getProperties("multi_get");
    rdbEngine = RdbEngineFactory.create(new JdbcConfig(new DatabaseConfig(properties)));
    StorageFactory factory = StorageFactory.create(properties);
    admin = factory.getStorageAdmin();
    storage = factory.getStorage();
    testUtils = new JdbcAdminTestUtils(properties);

    admin.createNamespace(NAMESPACE, true);
    admin.createTable(
        NAMESPACE,
        INT_TABLE,
        TableMetadata.newBuilder()
            .addColumn("p", DataType.INT)
            .addColumn("c", DataType.INT)
            .addColumn("v", DataType.TEXT)
            .addPartitionKey("p")
            .addClusteringKey("c")
            .build(),
        true);
    admin.createTable(
        NAMESPACE,
        TEXT_TABLE,
        TableMetadata.newBuilder()
            .addColumn("p", DataType.TEXT)
            .addColumn("v", DataType.TEXT)
            .addPartitionKey("p")
            .build(),
        true);
    makeTextKeyCaseInsensitive();
    for (DataType type : DataType.values()) {
      if (isKeyTypeTested(type)) {
        admin.createTable(
            NAMESPACE,
            KEY_TABLE_PREFIX + type,
            TableMetadata.newBuilder()
                .addColumn("p", DataType.INT)
                .addColumn("c", type)
                .addColumn("v", DataType.TEXT)
                .addPartitionKey("p")
                .addClusteringKey("c")
                .build(),
            true);
      }
    }
  }

  /** The key types of the parity test: the inexact ones but TEXT, which has its own tests. */
  private boolean isKeyTypeTested(DataType type) {
    switch (type) {
      case FLOAT:
      case DOUBLE:
      case DATE:
      case TIME:
      case TIMESTAMP:
      case TIMESTAMPTZ:
        return true;
      case BLOB:
        return !JdbcTestUtils.isOracle(rdbEngine) && !JdbcTestUtils.isDb2(rdbEngine);
      default:
        return false;
    }
  }

  /** Gives the TEXT key a case-insensitive collation on the engines where that is easy to set. */
  private void makeTextKeyCaseInsensitive() throws Exception {
    String collation;
    if (JdbcTestUtils.isPostgresql(rdbEngine) && !JdbcEnv.isYugabyte()) {
      testUtils.executeSql(
          "CREATE COLLATION IF NOT EXISTS "
              + CASE_INSENSITIVE_COLLATION
              + " (provider = icu, locale = 'und-u-ks-level2', deterministic = false)");
      collation = "COLLATE " + CASE_INSENSITIVE_COLLATION;
    } else if (JdbcTestUtils.isMysql(rdbEngine)) {
      collation = "CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci";
    } else {
      return;
    }
    caseInsensitive = true;
    String type = rdbEngine.getDataTypeForKey(DataType.TEXT) + " " + collation;
    for (String sql : rdbEngine.alterColumnTypeSql(NAMESPACE, TEXT_TABLE, "p", type)) {
      testUtils.executeSql(sql);
    }
  }

  @AfterAll
  void tearDown() throws Exception {
    admin.dropTable(NAMESPACE, INT_TABLE, true);
    admin.dropTable(NAMESPACE, TEXT_TABLE, true);
    for (DataType type : DataType.values()) {
      admin.dropTable(NAMESPACE, KEY_TABLE_PREFIX + type, true);
    }
    admin.dropNamespace(NAMESPACE, true);
    if (JdbcTestUtils.isPostgresql(rdbEngine) && !JdbcEnv.isYugabyte()) {
      testUtils.executeSql("DROP COLLATION IF EXISTS " + CASE_INSENSITIVE_COLLATION);
    }
    storage.close();
    admin.close();
    testUtils.close();
  }

  @Test
  public void multiGet_MoreGetsThanOneInListHoldsGiven_ShouldRetrieveTheResultOfEachGet()
      throws ExecutionException {
    // Arrange
    List<Put> puts = new ArrayList<>();
    for (int c = 0; c < 600; c++) {
      puts.add(
          Put.newBuilder()
              .namespace(NAMESPACE)
              .table(INT_TABLE)
              .partitionKey(Key.ofInt("p", 1))
              .clusteringKey(Key.ofInt("c", c))
              .textValue("v", "v" + c)
              .build());
    }
    storage.put(puts);
    List<Get> gets = new ArrayList<>();
    for (int c = 699; c >= 0; c--) { // 600 found, 100 not found, in reverse order
      gets.add(
          Get.newBuilder()
              .namespace(NAMESPACE)
              .table(INT_TABLE)
              .partitionKey(Key.ofInt("p", 1))
              .clusteringKey(Key.ofInt("c", c))
              .build());
    }

    // Act
    List<Optional<Result>> results = storage.multiGet(gets);

    // Assert
    assertThat(results).hasSize(700);
    for (int i = 0; i < 700; i++) {
      int c = 699 - i;
      if (c < 600) {
        assertThat(results.get(i)).as("c=%d", c).isPresent();
        assertThat(results.get(i).get().getInt("c")).isEqualTo(c);
        assertThat(results.get(i).get().getText("v")).isEqualTo("v" + c);
      } else {
        assertThat(results.get(i)).as("c=%d", c).isEmpty();
      }
    }
  }

  @Test
  public void multiGet_TextKeysGiven_ShouldRetrieveWhatEachGetRetrievesByItself()
      throws ExecutionException {
    // Arrange
    for (String p : Arrays.asList("a", "B", "Ab")) {
      storage.put(
          Put.newBuilder()
              .namespace(NAMESPACE)
              .table(TEXT_TABLE)
              .partitionKey(Key.ofText("p", p))
              .textValue("v", p)
              .build());
    }
    List<Get> gets = new ArrayList<>();
    for (String p : Arrays.asList("a", "A", "b", "B", "Ab", "AB", "x", "a")) {
      gets.add(
          Get.newBuilder()
              .namespace(NAMESPACE)
              .table(TEXT_TABLE)
              .partitionKey(Key.ofText("p", p))
              .build());
    }

    // Act
    List<Optional<Result>> results = storage.multiGet(gets);

    // Assert
    for (int i = 0; i < gets.size(); i++) {
      Optional<Result> expected = storage.get(gets.get(i));
      assertThat(results.get(i).map(r -> r.getText("v")))
          .as("Get %s", gets.get(i).getPartitionKey())
          .isEqualTo(expected.map(r -> r.getText("v")));
    }
    assertThat(results.get(0).get().getText("v")).isEqualTo("a");
    assertThat(results.get(6)).isEmpty();
    if (caseInsensitive) {
      // "A" matches the record "a" only by the collation, so it is read again by itself
      assertThat(results.get(1).get().getText("v")).isEqualTo("a");
      assertThat(results.get(5).get().getText("v")).isEqualTo("Ab");
    }
  }

  @Test
  public void multiGet_TextKeysWithoutRecordsGiven_ShouldReturnEmptyForEachGet()
      throws ExecutionException {
    // Arrange
    List<Get> gets = new ArrayList<>();
    for (String p : Arrays.asList("none1", "NONE2", "none3")) {
      gets.add(
          Get.newBuilder()
              .namespace(NAMESPACE)
              .table(TEXT_TABLE)
              .partitionKey(Key.ofText("p", p))
              .build());
    }

    // Act
    List<Optional<Result>> results = storage.multiGet(gets);

    // Assert
    assertThat(results).hasSize(3).allMatch(r -> !r.isPresent());
  }

  @ParameterizedTest
  @EnumSource(
      value = DataType.class,
      names = {"FLOAT", "DOUBLE", "BLOB", "DATE", "TIME", "TIMESTAMP", "TIMESTAMPTZ"})
  public void multiGet_KeysOfTypeGiven_ShouldRetrieveWhatEachGetRetrievesByItself(DataType type)
      throws ExecutionException {
    if (!isKeyTypeTested(type)) {
      return;
    }

    // Arrange
    String table = KEY_TABLE_PREFIX + type;
    List<Column<?>> stored = storedKeys(type);
    for (int i = 0; i < stored.size(); i++) {
      storage.put(
          Put.newBuilder()
              .namespace(NAMESPACE)
              .table(table)
              .partitionKey(Key.ofInt("p", 1))
              .clusteringKey(Key.newBuilder().add(stored.get(i)).build())
              .textValue("v", "v" + i)
              .build());
    }
    List<Get> gets = new ArrayList<>();
    for (Column<?> key : getKeys(type)) {
      gets.add(
          Get.newBuilder()
              .namespace(NAMESPACE)
              .table(table)
              .partitionKey(Key.ofInt("p", 1))
              .clusteringKey(Key.newBuilder().add(key).build())
              .build());
    }

    // Act
    List<Optional<Result>> results = storage.multiGet(gets);

    // Assert
    assertThat(results).hasSize(gets.size());
    for (int i = 0; i < gets.size(); i++) {
      Optional<Result> expected = storage.get(gets.get(i));
      assertThat(results.get(i).map(r -> r.getText("v")))
          .as("Get %s", gets.get(i).getClusteringKey())
          .isEqualTo(expected.map(r -> r.getText("v")));
    }
    // The stored keys come first in getKeys(), and each of them finds its record
    for (int i = 0; i < stored.size(); i++) {
      assertThat(results.get(i).map(r -> r.getText("v"))).hasValue("v" + i);
    }
  }

  /** The keys of the records the parity test writes. */
  private static List<Column<?>> storedKeys(DataType type) {
    switch (type) {
      case FLOAT:
        return Arrays.asList(FloatColumn.of("c", 0.0f), FloatColumn.of("c", 1.5f));
      case DOUBLE:
        return Arrays.asList(DoubleColumn.of("c", 0.0), DoubleColumn.of("c", 1.5));
      case BLOB:
        return Arrays.asList(
            BlobColumn.of("c", new byte[] {1, 2}), BlobColumn.of("c", new byte[] {3}));
      case DATE:
        return Arrays.asList(
            DateColumn.of("c", LocalDate.of(2024, 2, 29)),
            DateColumn.of("c", LocalDate.of(2024, 3, 1)));
      case TIME:
        return Arrays.asList(
            TimeColumn.of("c", LocalTime.of(10, 0)),
            TimeColumn.of("c", LocalTime.of(12, 34, 56, 123_456_000)));
      case TIMESTAMP:
        return Arrays.asList(
            TimestampColumn.of("c", LocalDateTime.of(2024, 2, 29, 10, 0)),
            TimestampColumn.of("c", LocalDateTime.of(2024, 2, 29, 12, 34, 56, 789_000_000)));
      case TIMESTAMPTZ:
        return Arrays.asList(
            TimestampTZColumn.of("c", Instant.ofEpochMilli(1_700_000_000_000L)),
            TimestampTZColumn.of("c", Instant.ofEpochMilli(1_700_000_000_789L)));
      default:
        throw new AssertionError(type);
    }
  }

  /**
   * The keys of the Gets: the stored keys, then keys without records, and, for FLOAT and DOUBLE,
   * the negative zero, which SQL finds equal to the stored positive zero.
   */
  private static List<Column<?>> getKeys(DataType type) {
    List<Column<?>> keys = new ArrayList<>(storedKeys(type));
    switch (type) {
      case FLOAT:
        keys.add(FloatColumn.of("c", -0.0f));
        keys.add(FloatColumn.of("c", 2.5f));
        break;
      case DOUBLE:
        keys.add(DoubleColumn.of("c", -0.0));
        keys.add(DoubleColumn.of("c", 2.5));
        break;
      case BLOB:
        keys.add(BlobColumn.of("c", new byte[] {1}));
        keys.add(BlobColumn.of("c", new byte[] {1, 2, 0}));
        break;
      case DATE:
        keys.add(DateColumn.of("c", LocalDate.of(2024, 3, 2)));
        break;
      case TIME:
        keys.add(TimeColumn.of("c", LocalTime.of(12, 34, 56, 123_457_000)));
        break;
      case TIMESTAMP:
        keys.add(TimestampColumn.of("c", LocalDateTime.of(2024, 2, 29, 12, 34, 56, 788_000_000)));
        break;
      case TIMESTAMPTZ:
        keys.add(TimestampTZColumn.of("c", Instant.ofEpochMilli(1_700_000_000_788L)));
        break;
      default:
        throw new AssertionError(type);
    }
    return keys;
  }
}
