package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.Collation;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;
import java.util.Collections;
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
 * Exercises collation-canonical key identity end-to-end through Consensus Commit transactions
 * against a backend table whose TEXT columns use a collation aligned with ICU at {@code PRIMARY}
 * strength (case- and accent-insensitive).
 *
 * <p>Each scenario spells the same logical partition key with different basic-Latin case variants
 * (e.g. {@code "apple"} vs {@code "Apple"}) across the reads and writes of a transaction, and
 * verifies that the transaction layer treats them as one logical key: no spurious conflicts, no
 * duplicate physical rows, and correct scan-after-delete detection. Only basic-Latin case variants
 * are used because the suite also runs on SQL Server with a {@code _CI_AI} collation.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ConsensusCommitCollationIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(ConsensusCommitCollationIntegrationTestBase.class);

  private static final String TEST_NAME = "cc_collation";
  private static final String NAMESPACE = "int_test_" + TEST_NAME;
  protected static final String TABLE = "tbl";
  private static final String COL_PK = "pk";
  private static final String COL_VAL = "val";

  private DistributedTransactionAdmin admin;
  private DistributedTransactionManager manager;
  private String namespace;

  @BeforeAll
  public void beforeAll() throws Exception {
    TransactionFactory factory =
        TransactionFactory.create(getPropertiesWithIcuCollation(TEST_NAME));
    admin = factory.getTransactionAdmin();
    namespace = getNamespace();
    createTables();
    applyCollation(namespace, TABLE);
    manager = factory.getTransactionManager();
  }

  private Properties getPropertiesWithIcuCollation(String testName) {
    Properties properties = new Properties();
    properties.putAll(getProperties(testName));
    properties.setProperty(DatabaseConfig.COLLATION, Collation.ICU.name());
    // [strength 1] is ICU's primary level: base letters only, so case and accents do not
    // distinguish values.
    properties.setProperty(DatabaseConfig.COLLATION_ICU_RULES, "[strength 1]");
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
   * Counts the physical rows of the test table directly on the backend, bypassing the transaction
   * layer, so that the tests can assert that PRIMARY-equal spellings converge to a single physical
   * row.
   */
  protected int countBackendRows() throws Exception {
    return countRowsInBackendTable(namespace, TABLE);
  }

  /** Counts the physical rows of the given backend table, bypassing the transaction layer. */
  protected abstract int countRowsInBackendTable(String namespace, String table) throws Exception;

  protected String getNamespace() {
    return NAMESPACE;
  }

  private void createTables() throws Exception {
    Map<String, String> options = getCreationOptions();
    admin.createCoordinatorTables(true, options);
    admin.createNamespace(namespace, true, options);
    applyNamespaceCollation(namespace);
    admin.createTable(
        namespace,
        TABLE,
        TableMetadata.newBuilder()
            .addColumn(COL_PK, DataType.TEXT)
            .addColumn(COL_VAL, DataType.INT)
            .addPartitionKey(COL_PK)
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
    admin.truncateCoordinatorTables();
  }

  @AfterAll
  public void afterAll() throws Exception {
    try {
      dropTables();
    } catch (Exception e) {
      logger.warn("Failed to drop tables", e);
    }

    try {
      if (admin != null) {
        admin.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close admin", e);
    }

    try {
      if (manager != null) {
        manager.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close manager", e);
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

  private void dropTables() throws Exception {
    admin.dropTable(namespace, TABLE);
    cleanUpCollationArtifacts(namespace);
    admin.dropNamespace(namespace);
    admin.dropCoordinatorTables();
  }
  /**
   * Drops any backend collation object {@link #applyCollation(String, String)} created, after the
   * table is dropped and before the namespace is dropped (on PostgreSQL the created collation
   * depends on the namespace schema and would block a non-CASCADE schema drop). The default
   * implementation is a no-op.
   */
  protected void cleanUpCollationArtifacts(String namespace) throws Exception {}

  private Insert prepareInsert(String pk, int val) {
    return Insert.newBuilder()
        .namespace(namespace)
        .table(TABLE)
        .partitionKey(Key.ofText(COL_PK, pk))
        .intValue(COL_VAL, val)
        .build();
  }

  private Put preparePutWithImplicitPreRead(String pk, int val) {
    return Put.newBuilder()
        .namespace(namespace)
        .table(TABLE)
        .partitionKey(Key.ofText(COL_PK, pk))
        .intValue(COL_VAL, val)
        .enableImplicitPreRead()
        .build();
  }

  private Get prepareGet(String pk) {
    return Get.newBuilder()
        .namespace(namespace)
        .table(TABLE)
        .partitionKey(Key.ofText(COL_PK, pk))
        .build();
  }

  private Delete prepareDelete(String pk) {
    return Delete.newBuilder()
        .namespace(namespace)
        .table(TABLE)
        .partitionKey(Key.ofText(COL_PK, pk))
        .build();
  }

  private Scan prepareScan(String pk) {
    return Scan.newBuilder()
        .namespace(namespace)
        .table(TABLE)
        .partitionKey(Key.ofText(COL_PK, pk))
        .build();
  }

  @Test
  public void putAndCommit_ReadModifyWriteAcrossPrimaryEqualSpellings_ShouldUpdateSingleLogicalRow()
      throws Exception {
    // Arrange: commit the row under one spelling
    DistributedTransaction transaction1 = manager.start();
    transaction1.insert(prepareInsert("Apple", 1));
    transaction1.commit();

    // Act: read-modify-write the same logical row under a PRIMARY-equal spelling
    DistributedTransaction transaction2 = manager.start();
    Optional<Result> result = transaction2.get(prepareGet("apple"));
    assertThat(result).isPresent();
    assertThat(result.get().getInt(COL_VAL)).isEqualTo(1);
    transaction2.put(preparePutWithImplicitPreRead("apple", 2));
    // The commit must not raise a spurious conflict for the key spelling difference
    transaction2.commit();

    // Assert: the update is visible under the original spelling
    DistributedTransaction transaction3 = manager.start();
    Optional<Result> updated = transaction3.get(prepareGet("Apple"));
    transaction3.commit();
    assertThat(updated).isPresent();
    assertThat(updated.get().getInt(COL_VAL)).isEqualTo(2);

    // Assert: exactly one physical row exists for the logical key
    assertThat(countBackendRows()).isEqualTo(1);
  }

  @Test
  public void get_InsertedRecordWithPrimaryEqualSpellingInSameTransaction_ShouldReturnRecord()
      throws TransactionException {
    // Arrange Act: insert under one spelling and read back under a PRIMARY-equal spelling within
    // the same transaction
    DistributedTransaction transaction = manager.start();
    transaction.insert(prepareInsert("banana", 10));
    Optional<Result> result = transaction.get(prepareGet("Banana"));

    // Assert: the transaction sees its own write across spellings
    assertThat(result).isPresent();
    assertThat(result.get().getInt(COL_VAL)).isEqualTo(10);

    transaction.commit();
  }

  @Test
  public void insertAndPut_WithPrimaryEqualSpellingsInSameTransaction_ShouldConvergeToSingleRow()
      throws Exception {
    // Arrange Act: write the same logical row twice with PRIMARY-equal spellings in one
    // transaction; the commit must not raise a conflict for the key spelling difference
    DistributedTransaction transaction1 = manager.start();
    transaction1.insert(prepareInsert("cherry", 1));
    transaction1.put(preparePutWithImplicitPreRead("CHERRY", 2));
    transaction1.commit();

    // Assert: the writes converged to a single logical row holding the last value
    DistributedTransaction transaction2 = manager.start();
    Optional<Result> result = transaction2.get(prepareGet("cherry"));
    transaction2.commit();
    assertThat(result).isPresent();
    assertThat(result.get().getInt(COL_VAL)).isEqualTo(2);

    // Assert: exactly one physical row exists for the logical key
    assertThat(countBackendRows()).isEqualTo(1);
  }

  @Test
  public void
      scan_PartitionDeletedWithPrimaryEqualSpellingInSameTransaction_ShouldThrowIllegalArgumentException()
          throws TransactionException {
    // Arrange: commit the row under one spelling
    DistributedTransaction transaction1 = manager.start();
    transaction1.insert(prepareInsert("date", 5));
    transaction1.commit();

    // Act Assert: delete under a PRIMARY-equal spelling, then scanning the partition under the
    // original spelling must be detected as scanning already-deleted data
    DistributedTransaction transaction2 = manager.start();
    Optional<Result> result = transaction2.get(prepareGet("DATE"));
    assertThat(result).isPresent();
    assertThat(result.get().getInt(COL_VAL)).isEqualTo(5);
    transaction2.delete(prepareDelete("DATE"));
    assertThatThrownBy(() -> transaction2.scan(prepareScan("date")))
        .isInstanceOf(IllegalArgumentException.class);

    transaction2.rollback();
  }
}
