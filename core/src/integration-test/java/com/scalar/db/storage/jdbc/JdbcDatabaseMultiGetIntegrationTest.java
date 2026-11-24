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
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Multi-get on JDBC databases: Gets that need more than one IN list, and TEXT keys whose SQL
 * equality differs from the equality of their values.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JdbcDatabaseMultiGetIntegrationTest {
  private static final String NAMESPACE = "int_test_multi_get";
  private static final String INT_TABLE = "int_keys";
  private static final String TEXT_TABLE = "text_keys";
  private static final String CASE_INSENSITIVE_COLLATION = "scalardb_multi_get_ci";

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
}
