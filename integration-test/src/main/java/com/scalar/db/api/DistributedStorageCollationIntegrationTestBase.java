package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.Collation;
import com.scalar.db.io.CollationStrength;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exercises ICU text comparison semantics (scan ordering, conditional mutations, filtering) against
 * a backend table whose TEXT columns use a collation aligned with ICU at {@code PRIMARY} strength
 * (case- and accent-insensitive).
 *
 * <p>Data-set contract: key positions (partition key and clustering key) only ever hold values that
 * are pairwise distinct under ICU {@code PRIMARY} strength; spellings that are PRIMARY-equal (e.g.
 * {@code "apple"} vs {@code "Apple"}) appear only in non-key value positions.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DistributedStorageCollationIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(DistributedStorageCollationIntegrationTestBase.class);

  private static final String TEST_NAME = "storage_collation";
  private static final String NAMESPACE = "int_test_" + TEST_NAME;
  protected static final String TABLE = "tbl";
  private static final String COL_PK = "pk";
  private static final String COL_CK = "ck";
  private static final String COL_VAL = "val";
  private static final String COL_NUM = "num";

  private static final String ORDERING_PARTITION = "fruits";

  private DistributedStorage storage;
  private DistributedStorageAdmin admin;
  private String namespace;

  @BeforeAll
  public void beforeAll() throws Exception {
    StorageFactory factory = StorageFactory.create(getPropertiesWithIcuCollation(TEST_NAME));
    admin = factory.getStorageAdmin();
    namespace = getNamespace();
    createTable();
    applyCollation(namespace, TABLE);
    storage = factory.getStorage();
  }

  private Properties getPropertiesWithIcuCollation(String testName) {
    Properties properties = new Properties();
    properties.putAll(getProperties(testName));
    properties.setProperty(DatabaseConfig.COLLATION, Collation.ICU.name());
    properties.setProperty(DatabaseConfig.COLLATION_ICU_STRENGTH, CollationStrength.PRIMARY.name());
    return properties;
  }

  protected abstract Properties getProperties(String testName);

  /**
   * Applies the backend-specific collation to the test namespace so that tables created in it
   * afterwards inherit it. Called once after the namespace is created and before the table is
   * created. The default implementation is a no-op.
   */
  protected void applyNamespaceCollation(String namespace) throws Exception {}

  /**
   * Applies the backend-specific collation to the test table. Called once after the table is
   * created. The default implementation is a no-op.
   */
  protected void applyCollation(String namespace, String table) throws Exception {}

  /**
   * Returns whether accent-varied values (e.g. {@code "éclair"}) can be used in the data sets. For
   * example, SQL Server's {@code _CI_AI} coverage in these tests is constrained to basic-Latin case
   * variants, so accented values are excluded there.
   */
  protected boolean isAccentVariantSupported() {
    return true;
  }

  protected String getNamespace() {
    return NAMESPACE;
  }

  private void createTable() throws Exception {
    Map<String, String> options = getCreationOptions();
    admin.createNamespace(namespace, true, options);
    applyNamespaceCollation(namespace);
    admin.createTable(
        namespace,
        TABLE,
        TableMetadata.newBuilder()
            .addColumn(COL_PK, DataType.TEXT)
            .addColumn(COL_CK, DataType.TEXT)
            .addColumn(COL_VAL, DataType.TEXT)
            .addColumn(COL_NUM, DataType.INT)
            .addPartitionKey(COL_PK)
            .addClusteringKey(COL_CK, Scan.Ordering.Order.ASC)
            .build(),
        true,
        options);
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  @BeforeEach
  public void setUp() throws Exception {
    truncateTable();
  }

  protected void truncateTable() throws ExecutionException {
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
    try {
      closeCollationTestResources();
    } catch (Exception e) {
      logger.warn("Failed to close collation test resources", e);
    }
  }

  /**
   * Closes any resources the subclass holds for {@link #applyCollation(String, String)} and {@link
   * #cleanUpCollationArtifacts(String)}. Called at the end of teardown, after the namespace drop
   * that {@code cleanUpCollationArtifacts} participates in. The default implementation is a no-op.
   */
  protected void closeCollationTestResources() throws Exception {}

  private void dropTable() throws Exception {
    admin.dropTable(namespace, TABLE);
    cleanUpCollationArtifacts(namespace);
    admin.dropNamespace(namespace);
  }

  /**
   * Drops any backend collation object {@link #applyCollation(String, String)} created, after the
   * table is dropped and before the namespace is dropped (on PostgreSQL the created collation
   * depends on the namespace schema and would block a non-CASCADE schema drop). The default
   * implementation is a no-op.
   */
  protected void cleanUpCollationArtifacts(String namespace) throws Exception {}

  /**
   * Clustering-key values that are pairwise distinct under ICU PRIMARY strength but case- and
   * accent-varied across words. Byte order would yield {@code BANANA, Cherry, apple, éclair}; ICU
   * alphabetical order yields {@code apple, BANANA, Cherry, éclair}.
   */
  private List<String> getOrderingClusteringKeyValuesInIcuOrder() {
    if (isAccentVariantSupported()) {
      return Arrays.asList("apple", "BANANA", "Cherry", "éclair");
    }
    return Arrays.asList("apple", "BANANA", "Cherry");
  }

  private void populateOrderingPartition() throws ExecutionException {
    // Insert in neither byte order nor ICU order to avoid accidentally passing on
    // insertion-order-preserving backends
    List<String> clusteringKeyValues = new ArrayList<>(getOrderingClusteringKeyValuesInIcuOrder());
    Collections.rotate(clusteringKeyValues, 1);
    int i = 0;
    for (String clusteringKeyValue : clusteringKeyValues) {
      storage.put(
          Put.newBuilder()
              .namespace(namespace)
              .table(TABLE)
              .partitionKey(Key.ofText(COL_PK, ORDERING_PARTITION))
              .clusteringKey(Key.ofText(COL_CK, clusteringKeyValue))
              .textValue(COL_VAL, clusteringKeyValue)
              .intValue(COL_NUM, i++)
              .build());
    }
  }

  private List<String> scanClusteringKeyValues(Scan.Ordering ordering)
      throws ExecutionException, IOException {
    List<String> actual = new ArrayList<>();
    try (Scanner scanner =
        storage.scan(
            Scan.newBuilder()
                .namespace(namespace)
                .table(TABLE)
                .partitionKey(Key.ofText(COL_PK, ORDERING_PARTITION))
                .ordering(ordering)
                .build())) {
      for (Result result : scanner.all()) {
        actual.add(result.getText(COL_CK));
      }
    }
    return actual;
  }

  private void putInitialRow(String pk, String ck, String val, int num) throws ExecutionException {
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(Key.ofText(COL_PK, pk))
            .clusteringKey(Key.ofText(COL_CK, ck))
            .textValue(COL_VAL, val)
            .intValue(COL_NUM, num)
            .build());
  }

  private Put putNumWithCondition(String pk, String ck, int num, MutationCondition condition) {
    return Put.newBuilder()
        .namespace(namespace)
        .table(TABLE)
        .partitionKey(Key.ofText(COL_PK, pk))
        .clusteringKey(Key.ofText(COL_CK, ck))
        .intValue(COL_NUM, num)
        .condition(condition)
        .build();
  }

  private Result getRow(String pk, String ck) throws ExecutionException {
    Optional<Result> result =
        storage.get(
            Get.newBuilder()
                .namespace(namespace)
                .table(TABLE)
                .partitionKey(Key.ofText(COL_PK, pk))
                .clusteringKey(Key.ofText(COL_CK, ck))
                .build());
    assertThat(result).isPresent();
    return result.get();
  }

  @Test
  public void scan_WithClusteringKeyOrderingAsc_ShouldReturnIcuAlphabeticalOrder()
      throws ExecutionException, IOException {
    // Arrange
    populateOrderingPartition();
    List<String> expected = getOrderingClusteringKeyValuesInIcuOrder();

    // Act
    List<String> actual = scanClusteringKeyValues(Scan.Ordering.asc(COL_CK));

    // Assert
    assertThat(actual).containsExactlyElementsOf(expected);
  }

  @Test
  public void scan_WithClusteringKeyOrderingDesc_ShouldReturnReversedIcuAlphabeticalOrder()
      throws ExecutionException, IOException {
    // Arrange
    populateOrderingPartition();
    List<String> expected = new ArrayList<>(getOrderingClusteringKeyValuesInIcuOrder());
    Collections.reverse(expected);

    // Act
    List<String> actual = scanClusteringKeyValues(Scan.Ordering.desc(COL_CK));

    // Assert
    assertThat(actual).containsExactlyElementsOf(expected);
  }

  @Test
  public void put_WithPutIfOnPrimaryEqualSpellings_ShouldMatchOnEqualAndRejectOnNotEqual()
      throws ExecutionException {
    // Arrange
    putInitialRow("p1", "c1", "apple", 1);

    // Act Assert: EQ matches across PRIMARY-equal spellings ("Apple" vs stored "apple")
    storage.put(
        putNumWithCondition(
            "p1",
            "c1",
            2,
            ConditionBuilder.putIf(ConditionBuilder.column(COL_VAL).isEqualToText("Apple"))
                .build()));
    assertThat(getRow("p1", "c1").getInt(COL_NUM)).isEqualTo(2);

    // Act Assert: NE rejects a PRIMARY-equal spelling ("APPLE" vs stored "apple")
    assertThatThrownBy(
            () ->
                storage.put(
                    putNumWithCondition(
                        "p1",
                        "c1",
                        3,
                        ConditionBuilder.putIf(
                                ConditionBuilder.column(COL_VAL).isNotEqualToText("APPLE"))
                            .build())))
        .isInstanceOf(NoMutationException.class);
    assertThat(getRow("p1", "c1").getInt(COL_NUM)).isEqualTo(2);
  }

  @Test
  public void put_WithPutIfRangeConditionAcrossCaseBoundary_ShouldFollowIcuOrder()
      throws ExecutionException {
    // Arrange: byte order says 'apple' (0x61...) > 'BANANA' (0x42...); ICU says 'apple' < 'BANANA'
    putInitialRow("p1", "c1", "apple", 1);

    // Act Assert: GT fails under ICU order (would succeed under byte order)
    assertThatThrownBy(
            () ->
                storage.put(
                    putNumWithCondition(
                        "p1",
                        "c1",
                        2,
                        ConditionBuilder.putIf(
                                ConditionBuilder.column(COL_VAL).isGreaterThanText("BANANA"))
                            .build())))
        .isInstanceOf(NoMutationException.class);
    assertThat(getRow("p1", "c1").getInt(COL_NUM)).isEqualTo(1);

    // Act Assert: LTE succeeds under ICU order (would fail under byte order)
    storage.put(
        putNumWithCondition(
            "p1",
            "c1",
            2,
            ConditionBuilder.putIf(
                    ConditionBuilder.column(COL_VAL).isLessThanOrEqualToText("BANANA"))
                .build()));
    assertThat(getRow("p1", "c1").getInt(COL_NUM)).isEqualTo(2);
  }

  @Test
  public void scanAll_WithEqualConditionOnPrimaryEqualSpelling_ShouldReturnMatchingRow()
      throws ExecutionException, IOException {
    // Arrange: one row per partition; PRIMARY-equal spellings only appear in the val column
    putInitialRow("p1", "c1", "apple", 1);
    putInitialRow("p2", "c1", "banana", 2);
    putInitialRow("p3", "c1", "cherry", 3);

    // Act: filter with a PRIMARY-equal spelling of the stored value
    List<Result> results;
    try (Scanner scanner =
        storage.scan(
            Scan.newBuilder()
                .namespace(namespace)
                .table(TABLE)
                .all()
                .where(ConditionBuilder.column(COL_VAL).isEqualToText("Apple"))
                .build())) {
      results = scanner.all();
    }

    // Assert: binary equality would match nothing; ICU/CI equality matches exactly the 'apple' row
    assertThat(results).hasSize(1);
    assertThat(results.get(0).getText(COL_PK)).isEqualTo("p1");
    assertThat(results.get(0).getText(COL_VAL)).isEqualTo("apple");
    assertThat(results.get(0).getInt(COL_NUM)).isEqualTo(1);
  }
}
