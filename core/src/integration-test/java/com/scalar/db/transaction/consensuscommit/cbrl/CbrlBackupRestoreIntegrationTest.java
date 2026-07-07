package com.scalar.db.transaction.consensuscommit.cbrl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.service.TransactionFactory;
import com.scalar.db.storage.jdbc.JdbcEnv;
import com.scalar.db.transaction.consensuscommit.Attribute;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import com.scalar.db.transaction.consensuscommit.CoordinatorGroupCommitter.CoordinatorGroupCommitKeyManipulator;
import com.scalar.db.transaction.consensuscommit.proto.v1.Entry;
import com.scalar.db.transaction.consensuscommit.proto.v1.EntryGroup;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.jboss.byteman.agent.submit.ScriptText;
import org.jboss.byteman.agent.submit.Submit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

/**
 * CBRL backup/restore integration test (spike, §6.1 of the PoC plan): non-pausing backup with
 * <b>windowed repair of a non-snapshot-consistent copy</b>.
 *
 * <p>The user-table backup here is a <b>non-snapshot-consistent copy</b>: a live, raw {@code scan}
 * taken while the workload commits, so it is not an atomic point-in-time snapshot — different rows
 * reflect different instants, some are stale or missing, and some are caught mid-commit (PREPARED).
 *
 * <p><b>Logging is window-scoped</b>: a pre-window base is seeded with redo logging OFF (those
 * commits carry no redo), then the backup window opens (logging ON) and only in-window commits are
 * logged. The non-snapshot-consistent copy of the user tables is taken while the in-window workload
 * commits; the coordinator is backed up (self-contained, closed over the chain); at restore the
 * coordinator table is reloaded from that backup and the copy's in-flight records are recovered
 * against it (<b>C4: PREPARED-record recovery</b>: resolving records the copy caught mid-commit, in
 * the PREPARED state, to a clean committed-or-absent state — using the backed-up coordinator, never
 * the live one, which has diverged past the consistency point — before replay anchors on them); and
 * the committed redo is replayed <b>forward from each record's copied version</b> onto the copy via
 * the §5 core — not a full rebuild.
 *
 * <p>Two complementary workloads exercise the restore:
 *
 * <ul>
 *   <li>{@link #restore_AfterDisjointOwnerWorkload_ShouldYieldLatestValuePerColumn()} — each key is
 *       owned by one worker writing a monotonically increasing token, with column values derived
 *       from that token and a {@code token % 6} subset written per transaction. The coordinator is
 *       backed up <b>live</b> (non-pausing); the oracle is each key's recorded ops applied up to
 *       the backup's captured prefix, and every restored column must equal the value of its
 *       last-in-prefix writer. This proves window-consistency, that no older write overwrites a
 *       newer one, that partial after-images MERGE (untouched columns carried forward), and — via
 *       keys untouched in-window — that the copy is load-bearing.
 *   <li>{@link #restore_AfterConcurrentSameKeyTransfers_ShouldPreserveConservation()} — many
 *       workers run balance-preserving transfers on shared accounts, so the same record is written
 *       concurrently and conflicts. This is the same-key contention that leaves the copy with
 *       in-flight PREPARED records (recovered forward) and conflict-aborted records (recovered
 *       back); the conservation invariant (total balance and per-account cross-table equality) must
 *       survive restore. The backup is taken WHILE the workload runs, proving it does not pause.
 * </ul>
 *
 * <p>The two negative controls keep the checks honest: {@link
 * #consistencyCheck_ForInconsistentImage_ShouldReportViolations()} proves the consistency check has
 * teeth, and {@link #restore_WithoutTheUserTableBackup_ShouldLeavePreWindowKeysUnrecoverable()}
 * restores the same backup <b>without</b> the copy and shows the pre-window data is then
 * unrecoverable.
 *
 * <p>This is an abstract base; concrete subclasses select the config axis — {@code
 * CbrlBackupRestoreWithoutGroupCommitIntegrationTest} and {@code
 * CbrlBackupRestoreWithGroupCommitIntegrationTest} run the same scenarios with coordinator group
 * commit off and on. With group commit on, committed writes carry full child ids (parent + child)
 * that the redo explosion and recovery handle transparently — there is no group-commit-specific
 * test.
 *
 * <p>Requires PostgreSQL on localhost:5432 (override with {@code -Dscalardb.jdbc.url}).
 */
@TestInstance(Lifecycle.PER_CLASS)
public abstract class CbrlBackupRestoreIntegrationTest {

  /**
   * Whether coordinator group commit is enabled, supplied by the concrete subclass. The same
   * scenarios run with it false ({@code CbrlBackupRestoreWithoutGroupCommitIntegrationTest}) and
   * true ({@code CbrlBackupRestoreWithGroupCommitIntegrationTest}).
   */
  protected abstract boolean withCoordinatorGroupCommit();

  private static final String TEST_NAME = "cbrl";
  // One user namespace, restored in place: CbrlRestore routes each record by the namespace carried
  // in its redo, so the workload writes here, the non-snapshot-consistent copy is captured here,
  // and
  // restore rewrites it here. (The two-namespace src/restore split is gone — restore is in-place.)
  private static final String USER_NAMESPACE = "cbrl_data";
  private static final String COORDINATOR_NAMESPACE = "cbrl_coordinator";
  // The backup window's label: the workload opens the window with enableRedoLogging(BACKUP_LABEL)
  // and restore targets the same label. The coordinator is truncated between tests, so one label
  // is reused across them.
  private static final String BACKUP_LABEL = "cbrl-backup";

  private static final String TABLE_A = "table_a";
  private static final String A_PK = "pk";
  private static final String A_TOKEN = "col_token";
  private static final String A_INT = "col_int";
  private static final String A_TEXT = "col_text";
  private static final String A_BOOL = "col_bool";
  private static final String A_BLOB = "col_blob";
  private static final String[] A_USER_COLUMNS = {A_TOKEN, A_INT, A_TEXT, A_BOOL, A_BLOB};
  private static final TableMetadata TABLE_A_METADATA =
      TableMetadata.newBuilder()
          .addColumn(A_PK, DataType.INT)
          .addColumn(A_TOKEN, DataType.BIGINT)
          .addColumn(A_INT, DataType.INT)
          .addColumn(A_TEXT, DataType.TEXT)
          .addColumn(A_BOOL, DataType.BOOLEAN)
          .addColumn(A_BLOB, DataType.BLOB)
          .addPartitionKey(A_PK)
          .build();

  private static final String TABLE_B = "table_b";
  private static final String B_PK = "pk";
  private static final String B_CK = "ck";
  private static final String B_TOKEN = "col_token";
  private static final String B_TEXT = "col_text";
  private static final TableMetadata TABLE_B_METADATA =
      TableMetadata.newBuilder()
          .addColumn(B_PK, DataType.INT)
          .addColumn(B_CK, DataType.INT)
          .addColumn(B_TOKEN, DataType.BIGINT)
          .addColumn(B_TEXT, DataType.TEXT)
          .addPartitionKey(B_PK)
          .addClusteringKey(B_CK)
          .build();

  // table_c carries one column of EVERY ScalarDB data type, to prove restore round-trips them all
  // end-to-end (the other tables exercise only INT/BIGINT/TEXT/BOOLEAN/BLOB).
  private static final String TABLE_C = "table_c";
  private static final String C_PK = "pk";
  private static final String C_BOOLEAN = "c_boolean";
  private static final String C_INT = "c_int";
  private static final String C_BIGINT = "c_bigint";
  private static final String C_FLOAT = "c_float";
  private static final String C_DOUBLE = "c_double";
  private static final String C_TEXT = "c_text";
  private static final String C_BLOB = "c_blob";
  private static final String C_DATE = "c_date";
  private static final String C_TIME = "c_time";
  private static final String C_TIMESTAMP = "c_timestamp";
  private static final String C_TIMESTAMPTZ = "c_timestamptz";
  private static final TableMetadata TABLE_C_METADATA =
      TableMetadata.newBuilder()
          .addColumn(C_PK, DataType.INT)
          .addColumn(C_BOOLEAN, DataType.BOOLEAN)
          .addColumn(C_INT, DataType.INT)
          .addColumn(C_BIGINT, DataType.BIGINT)
          .addColumn(C_FLOAT, DataType.FLOAT)
          .addColumn(C_DOUBLE, DataType.DOUBLE)
          .addColumn(C_TEXT, DataType.TEXT)
          .addColumn(C_BLOB, DataType.BLOB)
          .addColumn(C_DATE, DataType.DATE)
          .addColumn(C_TIME, DataType.TIME)
          .addColumn(C_TIMESTAMP, DataType.TIMESTAMP)
          .addColumn(C_TIMESTAMPTZ, DataType.TIMESTAMPTZ)
          .addPartitionKey(C_PK)
          .build();

  // Every user table, for the copy/recover/snapshot loops that operate on all of them uniformly.
  private static final String[] USER_TABLES = {TABLE_A, TABLE_B, TABLE_C};

  private static final int RECORD_COUNT = 40;
  // Keys [0, PRE_WINDOW_ONLY_KEYS) are seeded pre-window and never touched in-window: zero redo,
  // restored from the copy alone (the load-bearing proof). The rest are split into one disjoint
  // range per worker in the disjoint-owner test.
  private static final int PRE_WINDOW_ONLY_KEYS = 4;
  // Half-and-half: keys [0, SEEDED_KEYS) are seeded pre-window (so they exist in the copy);
  // [SEEDED_KEYS, RECORD_COUNT) start ABSENT and are created in-window, so restore exercises both
  // the copied-base path AND replaying a redo INSERT onto a base the copy never had (a chain
  // root).
  private static final int SEEDED_KEYS = RECORD_COUNT / 2;

  // Per-column tracking for the disjoint-owner test: a column index per user column of table_a,
  // used
  // both as the keysState slot and as the bit in the token % 6 write-selection mask. table_b
  // carries
  // the shared columns (token, text).
  private static final int COL_TOKEN = 0;
  private static final int COL_INT = 1;
  private static final int COL_TEXT = 2;
  private static final int COL_BOOL = 3;
  private static final int COL_BLOB = 4;
  private static final int USER_COLUMN_COUNT = 5;
  private static final int ALL_COLUMNS = 0b11111;
  private static final int WORKLOAD_THREADS = 4;
  private static final int DELETE_PERCENTAGE = 30;
  private static final Duration WORKLOAD_WARMUP = Duration.ofMillis(400);
  private static final Duration WORKLOAD_AFTER_USER_BACKUP = Duration.ofMillis(400);
  // Both crash tests run the workload well past the user-table backup before backing up the
  // coordinator, so the restore has enough real work for its crash teeth to bite. The named-sites
  // test needs the backup to diverge from the consistency point on more keys than any crash site's
  // fireAt (max 3); the crashed-midway test needs the restore slow enough that a time-proportional
  // crash lands mid-flight rather than after a warm re-run has already finished. A short window
  // can,
  // under load, make the restore trivially fast (dominated by one-time cold-start cost) and flake
  // both teeth. The non-crash restore tests have no such teeth, so they keep the short
  // WORKLOAD_AFTER_USER_BACKUP window.
  private static final Duration CRASH_TEST_BACKUP_WINDOW = Duration.ofSeconds(2);
  // A deterministic floor on how many keys diverge between the user-table backup and the
  // consistency
  // point, committed on the MAIN thread (see divergeBackupFromConsistencyPoint) so the crash teeth
  // do
  // not depend on the background workload making progress in the window: under machine load those
  // threads can be starved for the whole window, leaving the backup ~equal to the consistency
  // point.
  // Must be > any crash site's fireAt (max 3) and <= SEEDED_KEYS.
  private static final int DIVERGENCE_FLOOR_KEYS = 12;
  // Backoff between workload-transaction retries: an UncommittedRecordException means a concurrent
  // (group-)committed record's coordinator state has not landed yet, so retrying immediately just
  // burns attempts before async recovery resolves it. A short pause lets it settle.
  private static final Duration WORKLOAD_RETRY_BACKOFF = Duration.ofMillis(20);

  private DistributedTransactionManager manager;
  private DistributedTransactionAdmin admin;
  private DistributedStorage storage;
  private DistributedStorageAdmin storageAdmin;
  private final CoordinatorGroupCommitKeyManipulator keyManipulator =
      new CoordinatorGroupCommitKeyManipulator();
  private ExecutorService workerExecutor;
  private CbrlRestore cbrlRestore;
  private final AtomicBoolean crashRestore = new AtomicBoolean(false);
  private final AtomicLong tokenCounter = new AtomicLong();
  private final AtomicLong committedCount = new AtomicLong();

  private interface TxBody {
    void run(DistributedTransaction tx) throws Exception;
  }

  private Properties properties() {
    Properties properties = JdbcEnv.getProperties(TEST_NAME);
    properties.setProperty(
        DatabaseConfig.TRANSACTION_MANAGER, ConsensusCommitConfig.TRANSACTION_MANAGER_NAME);
    properties.setProperty(ConsensusCommitConfig.COORDINATOR_NAMESPACE, COORDINATOR_NAMESPACE);
    // Redo logging starts OFF (its default now that it is dynamic-only); the test opens the backup
    // window by calling enableRedoLogging() explicitly, so the pre-window base is unlogged and the
    // copy is load-bearing.
    // Group commit is a config axis: the concrete subclass selects it. The same scenarios run with
    // it off and on (group-committed writes get full child ids — parent + child — which the redo
    // explosion and recovery must handle).
    if (withCoordinatorGroupCommit()) {
      properties.setProperty(ConsensusCommitConfig.COORDINATOR_GROUP_COMMIT_ENABLED, "true");
    }
    return properties;
  }

  @BeforeAll
  void beforeAll() throws Exception {
    Properties properties = properties();
    TransactionFactory transactionFactory = TransactionFactory.create(properties);
    manager = transactionFactory.getTransactionManager();
    admin = transactionFactory.getTransactionAdmin();
    StorageFactory storageFactory = StorageFactory.create(properties);
    storage = storageFactory.getStorage();
    storageAdmin = storageFactory.getStorageAdmin();
    workerExecutor = Executors.newFixedThreadPool(WORKLOAD_THREADS);

    admin.createCoordinatorTables(true);
    admin.createNamespace(USER_NAMESPACE, true);
    admin.createTable(USER_NAMESPACE, TABLE_A, TABLE_A_METADATA, true);
    admin.createTable(USER_NAMESPACE, TABLE_B, TABLE_B_METADATA, true);
    admin.createTable(USER_NAMESPACE, TABLE_C, TABLE_C_METADATA, true);

    cbrlRestore = new CbrlRestore(properties, BACKUP_LABEL, storage, manager, storageAdmin);
  }

  @AfterAll
  void afterAll() throws Exception {
    if (workerExecutor != null) {
      workerExecutor.shutdownNow();
    }
    if (admin != null) {
      admin.dropTable(USER_NAMESPACE, TABLE_A, true);
      admin.dropTable(USER_NAMESPACE, TABLE_B, true);
      admin.dropTable(USER_NAMESPACE, TABLE_C, true);
      admin.dropNamespace(USER_NAMESPACE, true);
      admin.dropCoordinatorTables(true);
      admin.dropNamespace(COORDINATOR_NAMESPACE, true);
      admin.close();
    }
    if (storageAdmin != null) {
      storageAdmin.close();
    }
    if (manager != null) {
      manager.close();
    }
    if (storage != null) {
      storage.close();
    }
  }

  @BeforeEach
  void beforeEach() throws Exception {
    manager.disableRedoLogging(); // Clean baseline: the window is closed until a test opens it.
    admin.truncateCoordinatorTables();
    admin.truncateTable(USER_NAMESPACE, TABLE_A);
    admin.truncateTable(USER_NAMESPACE, TABLE_B);
    admin.truncateTable(USER_NAMESPACE, TABLE_C);
  }

  /** Negative control: proves the consistency check detects an inconsistent image. */
  @Test
  void consistencyCheck_ForInconsistentImage_ShouldReportViolations() {
    // Arrange: key 0 with mismatched tokens across the two tables, key 1 in table_a only — two
    // distinct inconsistencies the check must catch.
    withRetry(
        tx -> {
          tx.put(putForTableA(USER_NAMESPACE, 0, 111L));
          tx.put(putForTableB(USER_NAMESPACE, 0, 222L));
        });
    withRetry(tx -> tx.put(putForTableA(USER_NAMESPACE, 1, 333L)));

    // Act
    List<String> violations = findConsistencyViolations();

    // Assert
    assertThat(violations).hasSize(2);
    assertThat(violations.toString()).contains("key 0").contains("key 1");
  }

  /**
   * Negative control for the load-bearing claim: restoring the same backup WITHOUT the copy leaves
   * the pre-window-only keys unrecoverable, since they have no redo and no other base.
   */
  @Test
  void restore_WithoutTheUserTableBackup_ShouldLeavePreWindowKeysUnrecoverable() throws Exception {
    // Arrange
    Map<Integer, Long> seed = seedPreWindowBase(PRE_WINDOW_ONLY_KEYS); // logging still OFF
    // Open the window and do a little in-window work on OTHER keys, so the backup has redo (the
    // restore path runs) but nothing touches the seeded keys.
    manager.enableRedoLogging(BACKUP_LABEL);
    for (int i = PRE_WINDOW_ONLY_KEYS; i < PRE_WINDOW_ONLY_KEYS + 2; i++) {
      long token = tokenCounter.incrementAndGet();
      int key = i;
      withRetry(
          tx -> {
            tx.put(putForTableA(USER_NAMESPACE, key, token));
            tx.put(putForTableB(USER_NAMESPACE, key, token));
          });
    }
    Map<String, Coordinator.State> backup = backupCoordinator();
    // Restore WITHOUT the copy: empty the user tables so the repair base is empty (the
    // coordinator is still restored, so the redo path runs — it just has no copied base to anchor).
    clearUserTables();
    arrangeRestoredCoordinator(backup);

    // Act
    cbrlRestore.restore();

    // Assert
    for (int i = 0; i < PRE_WINDOW_ONLY_KEYS; i++) {
      assertThat(getWithRetry(getForTableA(USER_NAMESPACE, i)))
          .as("without the copy, pre-window-only key %d is unrecoverable", i)
          .isEmpty();
    }
    assertThat(seed).hasSize(PRE_WINDOW_ONLY_KEYS); // sanity: the keys were in fact seeded
  }

  /**
   * Artificial roll-forward case (plan §4.5): the copy caught a record in the PREPARED state, but
   * its writer committed during the window. On restore the record must roll forward to COMMITTED
   * under the ORIGINAL writer's tx id, carry the writer's after-image, and — critically — keep its
   * ORIGINAL commit time (the coordinator's {@code tx_created_at}), not the recovery-time clock
   * that core's roll-forward stamps onto the record. A single record suffices; the existing suite
   * only exercises rolled-back in-doubt records, never a rolled-forward one.
   */
  @Test
  void restore_ForPreparedBackupRecordCommittedInWindow_ShouldRollForwardKeepingOriginalCommitTime()
      throws Exception {
    // Arrange
    int key = 0;
    long token = tokenCounter.incrementAndGet();
    manager.enableRedoLogging(BACKUP_LABEL); // Open the backup window.
    withRetry(tx -> tx.put(putForTableA(USER_NAMESPACE, key, token))); // T inserts + commits.
    Result committed =
        storage.get(getForTableA(USER_NAMESPACE, key)).orElseThrow(IllegalStateException::new);
    String txId = committed.getText(Attribute.ID);
    int version = committed.getInt(Attribute.VERSION);
    // Rewind the record to PREPARED, as if the scan caught T mid-commit, then capture it as the
    // copy
    // (insert-style: no before-image — roll-forward ignores it anyway, so it need not be rebuilt).
    storage.put(preparedRecordForTableA(USER_NAMESPACE, key, committed, txId, version));
    Map<String, List<Put>> userTableBackup = captureUserTableBackup();
    // T is the only in-window writer, so its coordinator row carries the original commit time.
    Map<String, Coordinator.State> backup = backupCoordinator();
    long originalCommittedAt = backup.values().iterator().next().getCreatedAt();
    restoreUserTableBackup(userTableBackup);
    arrangeRestoredCoordinator(backup);

    // Act
    cbrlRestore.restore();
    Result restored =
        storage.get(getForTableA(USER_NAMESPACE, key)).orElseThrow(IllegalStateException::new);

    // Assert
    assertThat(restored.getInt(Attribute.STATE)).isEqualTo(TransactionState.COMMITTED.get());
    assertThat(restored.getText(Attribute.ID)).as("original writer's tx id").isEqualTo(txId);
    assertThat(restored.getBigInt(A_TOKEN)).as("writer's after-image").isEqualTo(token);
    assertThat(restored.getInt(Attribute.VERSION)).isEqualTo(version);
    assertThat(restored.getBigInt(Attribute.COMMITTED_AT))
        .as("rolled-forward record keeps the original commit time, not the recovery-time clock")
        .isEqualTo(originalCommittedAt);
  }

  /**
   * Artificial roll-forward case (plan §4.5), delete variant: the copy caught a record in the
   * DELETED state whose writer committed during the window. On restore the record must roll forward
   * to physically absent. A single record suffices.
   */
  @Test
  void restore_ForDeletedBackupRecordCommittedInWindow_ShouldRemoveTheRecord() throws Exception {
    // Arrange
    int key = 0;
    long token = tokenCounter.incrementAndGet();
    // Pre-window committed base (logging OFF): a record for T to delete.
    withRetry(tx -> tx.put(putForTableA(USER_NAMESPACE, key, token)));
    manager.enableRedoLogging(BACKUP_LABEL); // Open the backup window.
    withRetry(tx -> tx.delete(deleteForTableA(USER_NAMESPACE, key))); // T deletes + commits.
    // The pre-window base also lands in the backup as a keys-only row (a logging-off commit still
    // writes a tx_write_set, just without redo), so pick the in-window DELETE writer by entry type.
    Map<String, Coordinator.State> backup = backupCoordinator();
    String txId = deleteWriterFullId(backup);
    // Re-create the record as the DELETED tombstone T left mid-commit, then capture it as the
    // copy
    // so the restore base carries the in-flight image.
    storage.put(deletedRecordForTableA(USER_NAMESPACE, key, txId));
    Map<String, List<Put>> userTableBackup = captureUserTableBackup();
    restoreUserTableBackup(userTableBackup);
    arrangeRestoredCoordinator(backup);

    // Act
    cbrlRestore.restore();
    Optional<Result> restored = storage.get(getForTableA(USER_NAMESPACE, key));

    // Assert
    assertThat(restored)
        .as("a DELETED copy record whose writer committed must restore as absent")
        .isEmpty();
  }

  /**
   * Every ScalarDB column type survives restore end-to-end. The other tables exercise only
   * INT/BIGINT/TEXT/BOOLEAN/BLOB; this writes a record with all 11 types to {@code table_c}, takes
   * the copy, and restores — driving the {@code cbrl} proto&lt;-&gt;io converters for every type
   * through the full pipeline (beyond the codec unit test).
   */
  @Test
  void restore_ForAllColumnTypes_ShouldRoundTripEveryType() throws Exception {
    // Arrange
    int key = 0;
    manager.enableRedoLogging(BACKUP_LABEL); // Open the backup window.
    withRetry(tx -> tx.put(allTypesPut(key)));
    Map<String, List<Put>> userTableBackup = captureUserTableBackup();
    restoreUserTableBackup(userTableBackup);
    arrangeRestoredCoordinator(backupCoordinator());

    // Act
    cbrlRestore.restore();
    Result restored = getWithRetry(getForTableC(key)).orElseThrow(IllegalStateException::new);

    // Assert
    assertThat(restored.getBoolean(C_BOOLEAN)).isTrue();
    assertThat(restored.getInt(C_INT)).isEqualTo(42);
    assertThat(restored.getBigInt(C_BIGINT)).isEqualTo(9_000_000_000L);
    assertThat(restored.getFloat(C_FLOAT)).isEqualTo(1.5f);
    assertThat(restored.getDouble(C_DOUBLE)).isEqualTo(2.25d);
    assertThat(restored.getText(C_TEXT)).isEqualTo("hello");
    assertThat(restored.getBlobAsBytes(C_BLOB)).isEqualTo(new byte[] {1, 2, 3});
    assertThat(restored.getDate(C_DATE)).isEqualTo(LocalDate.of(2026, 6, 24));
    assertThat(restored.getTime(C_TIME)).isEqualTo(LocalTime.of(1, 2, 3));
    assertThat(restored.getTimestamp(C_TIMESTAMP))
        .isEqualTo(LocalDateTime.of(2026, 6, 24, 1, 2, 3));
    assertThat(restored.getTimestampTZ(C_TIMESTAMPTZ))
        .isEqualTo(Instant.ofEpochSecond(1_700_000_000L));
  }

  /**
   * A re-insert that drops columns the non-snapshot copy still physically holds must CLEAR them,
   * not leave the stale copy value. The chain (full insert pre-window, then in-window delete +
   * re-insert of only the token column) nets to a present record whose user-column set is smaller
   * than the copied base; since write-back is a partial-column UPSERT, an unset column would
   * otherwise keep its old copy value. The restore writes the full committed image (NULL for
   * dropped columns), mirroring the replication log applier's fillUnsetColumnsWithNull.
   */
  @Test
  void restore_AfterReinsertDroppingColumns_ShouldClearTheDroppedColumns() throws Exception {
    // Arrange
    int key = 0;
    long baseToken = tokenCounter.incrementAndGet();
    // Pre-window committed base with EVERY table_a column set (logging off).
    withRetry(tx -> tx.put(putForTableA(USER_NAMESPACE, key, baseToken)));
    manager.enableRedoLogging(BACKUP_LABEL); // Open the backup window.
    // Capture the copy at the full-column base, before the in-window delete + re-insert.
    Map<String, List<Put>> userTableBackup = captureUserTableBackup();
    // In-window: delete, then re-insert with ONLY the token column, so the chain nets to a present
    // record that drops col_int/col_text/col_bool/col_blob — columns the copy base still holds.
    withRetry(tx -> tx.delete(deleteForTableA(USER_NAMESPACE, key)));
    long reinsertToken = tokenCounter.incrementAndGet();
    withRetry(
        tx -> {
          tx.get(getForTableA(USER_NAMESPACE, key));
          tx.put(deterministicPutForTableA(key, reinsertToken, 1 << COL_TOKEN));
        });
    restoreUserTableBackup(userTableBackup);
    arrangeRestoredCoordinator(backupCoordinator());

    // Act
    cbrlRestore.restore();
    Result restored =
        getWithRetry(getForTableA(USER_NAMESPACE, key)).orElseThrow(IllegalStateException::new);

    // Assert
    assertThat(restored.getBigInt(A_TOKEN)).isEqualTo(reinsertToken);
    assertThat(restored.isNull(A_INT))
        .as("dropped column is cleared, not the stale copy value")
        .isTrue();
    assertThat(restored.isNull(A_TEXT)).isTrue();
    assertThat(restored.isNull(A_BOOL)).isTrue();
    assertThat(restored.isNull(A_BLOB)).isTrue();
  }

  /** One committed in-window write on an owned key, recorded for the prefix oracle. */
  private static final class OwnedOp {
    private final String txId;
    private final boolean delete;
    private final int mask; // columns written (unused for a delete)
    private final long token;

    private OwnedOp(String txId, boolean delete, int mask, long token) {
      this.txId = txId;
      this.delete = delete;
      this.mask = mask;
      this.token = token;
    }
  }

  /**
   * Disjoint-owner test: ordering, partial-column merge, and the load-bearing copy in one, with a
   * <b>non-pausing</b> backup (Flow steps 3→4). Each key is written by exactly one worker thread;
   * every column value is a deterministic function of the writing token, and each transaction
   * writes the subset of columns selected by {@code token % 6} (all, or a single column), so a
   * key's columns are written at different times and replay must MERGE partial after-images onto
   * the copied version rather than replace it. The coordinator is backed up <b>while the workload
   * runs</b>; that live backup captures a committed prefix of each key's op history. The oracle is
   * independent of the replay: the test records each key's ops with their {@code txId}, and the
   * expected state is the seed base with that key's ops applied up to the last one whose {@code
   * txId} is in the backup's captured set (a clean prefix, since one owner commits a key
   * sequentially). Every restored column must equal the value derived from its last-in-prefix
   * writer — proving window-consistency, that replay never lets an older write overwrite a newer
   * one (a regression yields a smaller token), that an update's untouched columns are carried
   * forward (a replace-not-merge bug nulls them), that a re-insert which drops columns CLEARS them
   * (a stale-column bug leaves the copy's value under the partial-column write-back), and that
   * post-backup writes do not leak in (their ids are not in the captured set). Keys in {@code [0,
   * PRE_WINDOW_ONLY_KEYS)} are seeded but never touched in-window, so they also prove the copy is
   * load-bearing (restored with no redo).
   */
  @Test
  void restore_AfterDisjointOwnerWorkload_ShouldYieldLatestValuePerColumn() throws Exception {
    // Arrange
    long[] seedToken = new long[RECORD_COUNT];
    boolean[] present = new boolean[RECORD_COUNT];
    Map<Integer, List<OwnedOp>> history = new HashMap<>();
    for (int i = 0; i < RECORD_COUNT; i++) {
      history.put(i, new ArrayList<>());
    }
    // Seed only the first SEEDED_KEYS pre-window; the rest start absent and are created in-window
    // by
    // their owner (mutateOwnedKey inserts an absent key), so their first redo op is a chain-root
    // INSERT replayed onto a base the copy never had.
    for (int i = 0; i < SEEDED_KEYS; i++) {
      long token = tokenCounter.incrementAndGet();
      int key = i;
      seedToken[i] = token;
      present[i] = true;
      withRetry(
          tx -> {
            tx.put(deterministicPutForTableA(key, token, ALL_COLUMNS));
            tx.put(
                deterministicPutForTableB(key, token, ALL_COLUMNS)
                    .orElseThrow(IllegalStateException::new));
          });
    }

    manager.enableRedoLogging(BACKUP_LABEL); // Open the backup window.

    AtomicBoolean stop = new AtomicBoolean(false);
    List<Future<?>> workload = startDisjointWorkload(stop, present, history);
    Uninterruptibles.sleepUninterruptibly(WORKLOAD_WARMUP);
    // Live, non-snapshot-consistent copy; catches in-flight records to recover at restore.
    Map<String, List<Put>> userTableBackup = captureUserTableBackup();
    Uninterruptibles.sleepUninterruptibly(WORKLOAD_AFTER_USER_BACKUP);

    // Back up the coordinator WHILE the workload runs (Flow: back up, THEN stop) — non-pausing.
    long commitsBeforeBackup = committedCount.get();
    Map<String, Coordinator.State> coordinatorBackup = backupCoordinator();
    long deadlineMillis = System.currentTimeMillis() + 5_000;
    while (committedCount.get() <= commitsBeforeBackup
        && System.currentTimeMillis() < deadlineMillis) {
      Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(5));
    }
    assertThat(committedCount.get())
        .as("non-pausing: workload kept committing across the backup")
        .isGreaterThan(commitsBeforeBackup);

    // Stop the workload; transactions that committed after the backup scan are the divergence —
    // excluded from the expected state because their ids are not in the captured set.
    stop.set(true);
    for (Future<?> future : workload) {
      future.get();
    }
    Set<String> committedInBackup = committedFullIdsIn(coordinatorBackup);

    restoreUserTableBackup(userTableBackup);
    arrangeRestoredCoordinator(coordinatorBackup);

    // Act
    cbrlRestore.restore();

    // Assert
    assertThat(findConsistencyViolations()).as("consistency").isEmpty();
    assertThat(findDisjointViolations(seedToken, history, committedInBackup))
        .as(
            "every restored column == its last in-backup writer (merged, no regression, point-in-time)")
        .isEmpty();
    // A second restore over the already-restored image changes nothing (idempotent).
    Map<Integer, Long> image = readRestoredTokens();
    cbrlRestore.restore();
    assertThat(readRestoredTokens()).as("idempotent restore").isEqualTo(image);
  }

  private List<Future<?>> startDisjointWorkload(
      AtomicBoolean stop, boolean[] present, Map<Integer, List<OwnedOp>> history) {
    List<Future<?>> futures = new ArrayList<>(WORKLOAD_THREADS);
    int workedKeys = RECORD_COUNT - PRE_WINDOW_ONLY_KEYS;
    int chunk = (workedKeys + WORKLOAD_THREADS - 1) / WORKLOAD_THREADS;
    for (int t = 0; t < WORKLOAD_THREADS; t++) {
      int from = PRE_WINDOW_ONLY_KEYS + t * chunk;
      int to = Math.min(from + chunk, RECORD_COUNT);
      if (from >= to) {
        break;
      }
      int ownedFrom = from;
      int ownedTo = to;
      futures.add(
          workerExecutor.submit(
              () -> {
                while (!stop.get()) {
                  // Single owner per key: the only writer of these keys, their present flag, and
                  // their op history, so a key's commit order is its token order, with no race.
                  int key = ownedFrom + ThreadLocalRandom.current().nextInt(ownedTo - ownedFrom);
                  mutateOwnedKey(key, present, history);
                }
              }));
    }
    return futures;
  }

  /**
   * One transaction on an owned key: a re-insert if absent, else a delete or a partial update of
   * the columns selected by {@code token % 6}. A re-insert often DROPS columns the copy still holds
   * (see {@link #reinsertMask}), so restore must clear them rather than leave the stale copy value.
   * Appends the committed op (with its {@code txId}) to the key's history for the prefix oracle.
   */
  private void mutateOwnedKey(int key, boolean[] present, Map<Integer, List<OwnedOp>> history) {
    long token = tokenCounter.incrementAndGet();
    boolean delete = present[key] && rollDelete();
    int mask = present[key] ? columnMask(token) : reinsertMask(token); // absent -> re-insert
    String txId = commitOwnedOp(key, token, delete, mask);
    history.get(key).add(new OwnedOp(txId, delete, mask, token));
    present[key] = !delete;
    committedCount.incrementAndGet();
  }

  /**
   * Commits one owned-key op (read-then-write so partial writes chain); returns its transaction id.
   */
  private String commitOwnedOp(int key, long token, boolean delete, int mask) {
    RuntimeException last = null;
    for (int retry = 0; retry < 100; retry++) {
      DistributedTransaction tx = null;
      try {
        tx = manager.begin();
        tx.get(getForTableA(USER_NAMESPACE, key)); // Read both so partial writes chain (merge).
        tx.get(getForTableB(USER_NAMESPACE, key));
        if (delete) {
          tx.delete(deleteForTableA(USER_NAMESPACE, key));
          tx.delete(deleteForTableB(USER_NAMESPACE, key));
        } else {
          tx.put(deterministicPutForTableA(key, token, mask));
          Optional<Put> putB = deterministicPutForTableB(key, token, mask);
          if (putB.isPresent()) {
            tx.put(putB.get());
          }
        }
        tx.commit();
        return tx.getId();
      } catch (Exception e) {
        last = e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
        abortQuietly(tx);
        Uninterruptibles.sleepUninterruptibly(WORKLOAD_RETRY_BACKOFF);
      }
    }
    throw last;
  }

  /** Full transaction ids whose writes the backup carries (the consistency-point committed set). */
  private Set<String> committedFullIdsIn(Map<String, Coordinator.State> backup) {
    Set<String> ids = new HashSet<>();
    for (Coordinator.State state : backup.values()) {
      for (EntryGroup group : state.getWriteSet().get().getEntryGroupsList()) {
        ids.add(
            group.getChildId().isEmpty()
                ? state.getId()
                : keyManipulator.fullKey(state.getId(), group.getChildId()));
      }
    }
    return ids;
  }

  /** The full tx id of the (single) backup writer whose redo contains a DELETE entry. */
  private String deleteWriterFullId(Map<String, Coordinator.State> backup) {
    for (Coordinator.State state : backup.values()) {
      for (EntryGroup group : state.getWriteSet().get().getEntryGroupsList()) {
        for (Entry entry : group.getEntriesList()) {
          if (entry.getEntryType() == Entry.EntryType.ENTRY_TYPE_DELETE) {
            return group.getChildId().isEmpty()
                ? state.getId()
                : keyManipulator.fullKey(state.getId(), group.getChildId());
          }
        }
      }
    }
    throw new IllegalStateException("No DELETE redo entry in the coordinator backup");
  }

  /**
   * Every restored column compared against the prefix oracle: the seed base with each key's
   * recorded ops applied up to the last one whose transaction is in the backup's captured set.
   */
  private List<String> findDisjointViolations(
      long[] seedToken, Map<Integer, List<OwnedOp>> history, Set<String> committedInBackup) {
    List<String> violations = new ArrayList<>();
    for (int i = 0; i < RECORD_COUNT; i++) {
      // Un-seeded keys (i >= SEEDED_KEYS) start absent; seeded keys start present with all columns.
      boolean present = i < SEEDED_KEYS;
      // Per column, the token that last wrote it, or null if the column is currently absent. A
      // re-insert REPLACES (columns it omits become null); an update MERGES (only its columns
      // change). The distinction is read off the running present state: a non-delete op on an
      // absent
      // key is a re-insert, on a present key an update.
      Long[] cols = new Long[USER_COLUMN_COUNT];
      if (present) {
        Arrays.fill(cols, seedToken[i]); // The seed wrote every column.
      }
      for (OwnedOp op : history.get(i)) {
        if (!committedInBackup.contains(op.txId)) {
          continue; // Committed after the cut (divergence) — not in the restored image.
        }
        if (op.delete) {
          present = false;
        } else if (!present) {
          for (int c = 0; c < USER_COLUMN_COUNT; c++) {
            cols[c] = (op.mask & (1 << c)) != 0 ? op.token : null; // re-insert: replace
          }
          present = true;
        } else {
          for (int c = 0; c < USER_COLUMN_COUNT; c++) {
            if ((op.mask & (1 << c)) != 0) {
              cols[c] = op.token; // update: merge
            }
          }
        }
      }
      Optional<Result> a = getWithRetry(getForTableA(USER_NAMESPACE, i));
      Optional<Result> b = getWithRetry(getForTableB(USER_NAMESPACE, i));
      if (a.isPresent() != present || b.isPresent() != present) {
        violations.add(
            String.format(
                "key %d presence: a=%s b=%s expected=%s",
                i, a.isPresent(), b.isPresent(), present));
        continue;
      }
      if (!present) {
        continue;
      }
      Result ra = a.get();
      Result rb = b.get();
      checkColumn(violations, i, "a.token", bigIntMatches(ra, A_TOKEN, cols[COL_TOKEN]));
      checkColumn(violations, i, "a.int", intMatches(ra, A_INT, cols[COL_INT]));
      checkColumn(violations, i, "a.text", textMatches(ra, A_TEXT, cols[COL_TEXT]));
      checkColumn(violations, i, "a.bool", boolMatches(ra, A_BOOL, cols[COL_BOOL]));
      checkColumn(violations, i, "a.blob", blobMatches(ra, A_BLOB, cols[COL_BLOB]));
      checkColumn(violations, i, "b.token", bigIntMatches(rb, B_TOKEN, cols[COL_TOKEN]));
      checkColumn(violations, i, "b.text", textMatches(rb, B_TEXT, cols[COL_TEXT]));
    }
    return violations;
  }

  private static void checkColumn(List<String> violations, int key, String what, boolean ok) {
    if (!ok) {
      violations.add(String.format("key %d %s mismatch", key, what));
    }
  }

  // Each column matches the oracle iff: it is absent when the expected token is null (the column
  // was
  // dropped by a re-insert), or holds the value derived from the expected token otherwise.
  private static boolean bigIntMatches(Result r, String column, @Nullable Long token) {
    return token == null ? r.isNull(column) : !r.isNull(column) && r.getBigInt(column) == token;
  }

  private static boolean intMatches(Result r, String column, @Nullable Long token) {
    return token == null
        ? r.isNull(column)
        : !r.isNull(column) && r.getInt(column) == deriveInt(token);
  }

  private static boolean textMatches(Result r, String column, @Nullable Long token) {
    return token == null
        ? r.isNull(column)
        : !r.isNull(column) && deriveText(token).equals(r.getText(column));
  }

  private static boolean boolMatches(Result r, String column, @Nullable Long token) {
    return token == null
        ? r.isNull(column)
        : !r.isNull(column) && r.getBoolean(column) == deriveBool(token);
  }

  private static boolean blobMatches(Result r, String column, @Nullable Long token) {
    return token == null
        ? r.isNull(column)
        : !r.isNull(column) && Arrays.equals(r.getBlobAsBytes(column), deriveBlob(token));
  }

  // Column values are deterministic functions of the writing token, so the oracle can reconstruct
  // any column's expected value from the token that last wrote it.
  private static int deriveInt(long token) {
    return (int) token;
  }

  private static String deriveText(long token) {
    return Long.toString(token);
  }

  private static boolean deriveBool(long token) {
    return (token & 1L) == 0L;
  }

  private static byte[] deriveBlob(long token) {
    return ByteBuffer.allocate(Long.BYTES).putLong(token).array();
  }

  /**
   * A re-insert's column mask: always the shared token (so both tables are re-created and stay
   * token-consistent), plus a token-varied subset of the rest. So a re-insert usually OMITS some
   * columns the copy still physically holds — exercising whether restore CLEARS them (a replace) or
   * leaves the stale copy value. token % 16 == 0 drops every optional column; == 15 keeps them all.
   */
  private static int reinsertMask(long token) {
    int mask = 1 << COL_TOKEN;
    if ((token & 1L) != 0) {
      mask |= 1 << COL_INT;
    }
    if ((token & 2L) != 0) {
      mask |= 1 << COL_TEXT;
    }
    if ((token & 4L) != 0) {
      mask |= 1 << COL_BOOL;
    }
    if ((token & 8L) != 0) {
      mask |= 1 << COL_BLOB;
    }
    return mask;
  }

  /** Columns of table_a to write, as a bitmask over the COL_* indices, selected by token % 6. */
  private static int columnMask(long token) {
    switch ((int) (token % 6)) {
      case 0:
        return ALL_COLUMNS;
      case 1:
        return 1 << COL_INT;
      case 2:
        return 1 << COL_TEXT;
      case 3:
        return 1 << COL_BOOL;
      case 4:
        return 1 << COL_BLOB;
      default:
        return 1 << COL_TOKEN;
    }
  }

  /**
   * A table_a Put with exactly the columns in {@code mask}, each set to its token-derived value.
   */
  private Put deterministicPutForTableA(int i, long token, int mask) {
    PutBuilder.Buildable builder =
        Put.newBuilder().namespace(USER_NAMESPACE).table(TABLE_A).partitionKey(Key.ofInt(A_PK, i));
    if ((mask & (1 << COL_TOKEN)) != 0) {
      builder.bigIntValue(A_TOKEN, token);
    }
    if ((mask & (1 << COL_INT)) != 0) {
      builder.intValue(A_INT, deriveInt(token));
    }
    if ((mask & (1 << COL_TEXT)) != 0) {
      builder.textValue(A_TEXT, deriveText(token));
    }
    if ((mask & (1 << COL_BOOL)) != 0) {
      builder.booleanValue(A_BOOL, deriveBool(token));
    }
    if ((mask & (1 << COL_BLOB)) != 0) {
      builder.blobValue(A_BLOB, deriveBlob(token));
    }
    return builder.build();
  }

  /** A table_b Put with its columns (token, text) that are in {@code mask}; empty if none are. */
  private Optional<Put> deterministicPutForTableB(int i, long token, int mask) {
    PutBuilder.Buildable builder =
        Put.newBuilder()
            .namespace(USER_NAMESPACE)
            .table(TABLE_B)
            .partitionKey(Key.ofInt(B_PK, i))
            .clusteringKey(Key.ofInt(B_CK, i));
    boolean any = false;
    if ((mask & (1 << COL_TOKEN)) != 0) {
      builder.bigIntValue(B_TOKEN, token);
      any = true;
    }
    if ((mask & (1 << COL_TEXT)) != 0) {
      builder.textValue(B_TEXT, deriveText(token));
      any = true;
    }
    return any ? Optional.of(builder.build()) : Optional.empty();
  }

  /**
   * Concurrent same-key consistency check, in the conservation-invariant (bank-transfer) style.
   * Many workers run balance-preserving transfers between random accounts, so the same accounts are
   * written concurrently and conflict — the contention disjoint-owner avoids, and the contention
   * that leaves the copy with in-flight PREPARED records (recovered forward) and conflict-aborted
   * records (recovered back). Each account's balance lives in both tables and the total is
   * invariant, so a transactionally-consistent restored image must conserve it: a torn transfer
   * (debit without credit, or one table updated and not the other) breaks the sum or the
   * cross-table equality. The backup is taken WHILE the workload runs (non-pausing); conservation
   * holds for whatever consistent cut it captures.
   */
  @Test
  void restore_AfterConcurrentSameKeyTransfers_ShouldPreserveConservation() throws Exception {
    // Arrange
    long workloadStartMillis = System.currentTimeMillis();
    long initialBalance = 1_000L;
    seedBalances(SEEDED_KEYS, initialBalance); // Half seeded; the rest are created in-window.
    long total = (long) SEEDED_KEYS * initialBalance;

    manager.enableRedoLogging(BACKUP_LABEL); // Open the backup window.

    AtomicBoolean stop = new AtomicBoolean(false);
    List<Future<?>> workload = startTransferWorkload(stop);
    Uninterruptibles.sleepUninterruptibly(WORKLOAD_WARMUP);
    Map<String, List<Put>> userTableBackup = captureUserTableBackup();
    Uninterruptibles.sleepUninterruptibly(WORKLOAD_AFTER_USER_BACKUP);

    // Back up the coordinator WHILE the workload runs — conservation tolerates a fuzzy cut.
    long commitsBeforeBackup = committedCount.get();
    Map<String, Coordinator.State> coordinatorBackup = backupCoordinator();
    long deadlineMillis = System.currentTimeMillis() + 5_000;
    while (committedCount.get() <= commitsBeforeBackup
        && System.currentTimeMillis() < deadlineMillis) {
      Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(5));
    }
    assertThat(committedCount.get())
        .as("non-pausing: workload kept committing across the backup")
        .isGreaterThan(commitsBeforeBackup);

    stop.set(true);
    for (Future<?> future : workload) {
      future.get();
    }

    restoreUserTableBackup(userTableBackup);
    arrangeRestoredCoordinator(coordinatorBackup);
    long beforeRestoreMillis = System.currentTimeMillis();

    // Act
    cbrlRestore.restore();

    // Assert
    assertThat(findConsistencyViolations())
        .as("cross-table consistency: both tables hold the same balance per account")
        .isEmpty();
    assertThat(presentSeededKeyCount())
        .as("no deletes: every seeded account survives")
        .isEqualTo(SEEDED_KEYS);
    assertThat(sumRestoredBalances(TABLE_A, A_TOKEN))
        .as("table_a total balance conserved")
        .isEqualTo(total);
    assertThat(sumRestoredBalances(TABLE_B, B_TOKEN))
        .as("table_b total balance conserved")
        .isEqualTo(total);
    // Every restored record must carry an ORIGINAL commit time — set while the workload ran, before
    // the restore — not the restore-time clock. The window [workloadStart, beforeRestore) rejects
    // both a restore-time "now" stamp (>= beforeRestore) and any bogus/zeroed value (<
    // workloadStart);
    // isNotEmpty rejects a vacuous pass. (committed_at can come from either the writer's
    // coordinator
    // tx_created_at or the copy record's own tx_committed_at, so we bound it rather than match
    // exact.)
    assertThat(restoredCommitTimes())
        .as("restored records keep their original commit time, not the restore-time clock")
        .isNotEmpty()
        .allSatisfy(
            committedAt ->
                assertThat(committedAt)
                    .isGreaterThanOrEqualTo(workloadStartMillis)
                    .isLessThan(beforeRestoreMillis));
  }

  /** Raw {@code tx_committed_at} of every present record in the restore tables. */
  private List<Long> restoredCommitTimes() throws Exception {
    List<Long> times = new ArrayList<>();
    for (String table : USER_TABLES) {
      Scan scan = Scan.newBuilder().namespace(USER_NAMESPACE).table(table).all().build();
      try (Scanner scanner = storage.scan(scan)) {
        for (Result result : scanner.all()) {
          if (!result.isNull(Attribute.COMMITTED_AT)) {
            times.add(result.getBigInt(Attribute.COMMITTED_AT));
          }
        }
      }
    }
    return times;
  }

  /**
   * Crash-in-the-middle: a restore killed partway and then re-run from the full backup must still
   * converge to a consistent, conserved image — re-running never corrupts the result, no matter
   * where the first attempt died. Uses the conservation workload (no per-key oracle needed). A
   * baseline restore is timed first; the crash is then injected at a sweep of points across that
   * measured duration, and after each kill the restore is re-run to completion and re-checked.
   *
   * <p>This is the design's crash-safety claim under test. Write-back is NOT atomic: it writes each
   * record individually via the Storage API (one storage.put/delete per record, no enclosing
   * transaction), so a crash can leave a partial set. Crash-safety instead comes from idempotent
   * re-derivation — a re-run recomputes the same final states from the same backup and copy and
   * re-stamps them, overwriting any partially-written records — plus idempotent recovery/replay, so
   * the converged image is correct regardless of where the first attempt died. The crash is
   * injected by wrapping the storage/manager the restore uses in proxies that throw once a flag is
   * set, so the kill lands mid-operation (mid-recovery or mid-write-back) rather than relying on
   * cooperative thread interruption, which JDBC ignores.
   */
  @Test
  void restore_CrashedMidway_ShouldConvergeOnReRun() throws Exception {
    // Arrange
    long initialBalance = 1_000L;
    seedBalances(SEEDED_KEYS, initialBalance); // Half seeded; the rest are created in-window.
    long total = (long) SEEDED_KEYS * initialBalance;

    manager.enableRedoLogging(BACKUP_LABEL); // Open the backup window.
    AtomicBoolean stop = new AtomicBoolean(false);
    List<Future<?>> workload = startTransferWorkload(stop);
    Uninterruptibles.sleepUninterruptibly(CRASH_TEST_BACKUP_WINDOW);
    // The non-snapshot-consistent copy, captured so each crash iteration restarts from the same
    // base. The wide window gives the restore enough work that a time-proportional crash reliably
    // lands mid-flight (see CRASH_TEST_BACKUP_WINDOW).
    Map<String, List<Put>> userTableBackup = captureUserTableBackup();
    // Guarantee a floor of restore work on the main thread (see divergeBackupFromConsistencyPoint),
    // then let the workload add more over the window, so the restore is never trivially small even
    // if
    // the workload threads are starved.
    divergeBackupFromConsistencyPoint();
    Uninterruptibles.sleepUninterruptibly(CRASH_TEST_BACKUP_WINDOW);
    Map<String, Coordinator.State> backup = backupCoordinator();
    stop.set(true);
    for (Future<?> future : workload) {
      future.get();
    }

    // The coordinator table is restored from its backup before any restore (here: test arrange).
    // CbrlRestore never touches the coordinator, so this holds across every crash iteration below.
    restoreUserTableBackup(userTableBackup);
    arrangeRestoredCoordinator(backup);

    // Act & Assert (baseline): one full restore validates the happy path. Its FIRST run pays
    // one-time cold-start cost (JIT, connection-pool and schema warmup) that the crashing re-runs
    // below do not, so time a SECOND, warm restore for the crash timeline. Otherwise the crash
    // sleeps a fraction of the cold duration, which exceeds the warm restore, so every attempt
    // finishes before the crash lands and the teeth (killed > 0) never bite.
    cbrlRestore.restore();
    assertThat(findConsistencyViolations()).as("baseline restore consistent").isEmpty();
    assertThat(sumRestoredBalances(TABLE_A, A_TOKEN)).as("baseline conserved").isEqualTo(total);
    restoreUserTableBackup(userTableBackup); // Reset the base, then time a warm restore.
    long startNanos = System.nanoTime();
    cbrlRestore.restore();
    long restoreNanos = System.nanoTime() - startNanos;

    // Act & Assert (crash): kill a restore at a sweep of points, then re-run to convergence each
    // time, asserting the re-derived image stays consistent and conserved no matter where it died.
    CbrlRestore crashingRestore = newCrashingRestore();
    int steps = 4;
    int killed = 0;
    for (int step = 1; step <= steps; step++) {
      restoreUserTableBackup(userTableBackup); // Fresh non-consistent base for this attempt.

      crashRestore.set(false);
      Throwable[] thrown = {null};
      Thread attempt =
          new Thread(
              () -> {
                try {
                  crashingRestore.restore();
                } catch (Throwable t) {
                  thrown[0] = t; // The injected crash; null if the restore finished first.
                }
              });
      attempt.start();
      Uninterruptibles.sleepUninterruptibly(Duration.ofNanos((restoreNanos / steps) * step));
      crashRestore.set(true); // Inject the crash at this fraction of the restore timeline.
      attempt.join();
      crashRestore.set(false);
      if (thrown[0] != null) {
        killed++; // The attempt was genuinely killed mid-restore, not allowed to finish.
      }

      // Re-run from the full backup on whatever partial state the crash left behind.
      cbrlRestore.restore();

      assertThat(findConsistencyViolations())
          .as("re-run after crash at %d/%d: cross-table consistency", step, steps)
          .isEmpty();
      assertThat(presentSeededKeyCount())
          .as("re-run after crash at %d/%d: every seeded account survives", step, steps)
          .isEqualTo(SEEDED_KEYS);
      assertThat(sumRestoredBalances(TABLE_A, A_TOKEN))
          .as("re-run after crash at %d/%d: table_a conserved", step, steps)
          .isEqualTo(total);
      assertThat(sumRestoredBalances(TABLE_B, B_TOKEN))
          .as("re-run after crash at %d/%d: table_b conserved", step, steps)
          .isEqualTo(total);
    }

    assertThat(killed)
        .as("the crash injection actually killed a restore mid-flight at least once (has teeth)")
        .isGreaterThan(0);
  }

  /**
   * A {@link CbrlRestore} whose storage/manager/coordinator throw once {@link #crashRestore} is
   * set.
   */
  private CbrlRestore newCrashingRestore() throws Exception {
    DistributedStorage crashingStorage = crashing(DistributedStorage.class, storage, false);
    DistributedTransactionManager crashingManager =
        crashing(DistributedTransactionManager.class, manager, true);
    // The storage admin is the real one: schema is read lazily through it (not the crashing
    // handles), so a crash lands on recovery (manager) or read/write-back (storage), never
    // metadata.
    return new CbrlRestore(
        properties(), BACKUP_LABEL, crashingStorage, crashingManager, storageAdmin);
  }

  /**
   * Wraps {@code real} so every call throws once {@link #crashRestore} is set (close/abort
   * excepted, so cleanup still runs). When {@code wrapTransactions}, transactions it returns are
   * wrapped too, so a kill can land mid-transaction.
   */
  @SuppressWarnings("unchecked")
  private <T> T crashing(Class<T> iface, T real, boolean wrapTransactions) {
    return (T)
        Proxy.newProxyInstance(
            iface.getClassLoader(),
            new Class<?>[] {iface},
            (proxy, method, args) -> {
              String name = method.getName();
              if (crashRestore.get() && !name.equals("close") && !name.equals("abort")) {
                throw new IllegalStateException("injected mid-restore crash");
              }
              Object result;
              try {
                result = args == null ? method.invoke(real) : method.invoke(real, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
              if (wrapTransactions && result instanceof DistributedTransaction) {
                return crashing(
                    DistributedTransaction.class, (DistributedTransaction) result, false);
              }
              return result;
            });
  }

  // Byteman rules that turn five named methods of the restore pipeline into crash points. Each is a
  // no-op until CrashInjector is armed for its site, then throws on exactly the k-th call. Verified
  // against the code: RedoOperation.<init> (redo load), CbrlRestore.recoverKey / applyToRestore,
  // RecordApplier.applyBucket, and JdbcDatabase.put (at exit, after the write has committed).
  private static final String CRASH_RULES =
      String.join(
          "\n",
          crashRule("cbrl-redo-load", "RedoOperation", "<init>", "AT ENTRY", "redoLoad"),
          crashRule("cbrl-recover-key", "CbrlRestore", "recoverKey", "AT ENTRY", "recoverKey"),
          crashRule("cbrl-apply-bucket", "RecordApplier", "applyBucket", "AT ENTRY", "applyBucket"),
          crashRule(
              "cbrl-write-back", "CbrlRestore", "applyToRestore", "AT ENTRY", "writeBackEntry"),
          jdbcPutExitRule());

  private static final String CBRL_PACKAGE = "com.scalar.db.transaction.consensuscommit.cbrl";

  private static String crashRule(
      String name, String simpleClass, String method, String location, String site) {
    return String.join(
        "\n",
        "RULE " + name,
        "CLASS " + CBRL_PACKAGE + "." + simpleClass,
        "METHOD " + method,
        location,
        "IF TRUE",
        "DO " + CBRL_PACKAGE + ".CrashInjector.hit(\"" + site + "\")",
        "ENDRULE");
  }

  /**
   * The JDBC put-after-commit rule targets a framework class (a proxy could not reach its exit).
   */
  private static String jdbcPutExitRule() {
    return String.join(
        "\n",
        "RULE cbrl-jdbc-put-exit",
        "CLASS com.scalar.db.storage.jdbc.JdbcDatabase",
        "METHOD put",
        "AT EXIT",
        "IF TRUE",
        "DO " + CBRL_PACKAGE + ".CrashInjector.hit(\"jdbcPutExit\")",
        "ENDRULE");
  }

  /**
   * The same crash-safety claim as {@link #restore_CrashedMidway_ShouldConvergeOnReRun()}, but the
   * crash is injected at <b>named methods of the restore pipeline</b> via Byteman rather than by a
   * timing sweep over storage/manager proxies. Byteman reaches call sites a proxy cannot — package-
   * private methods ({@code RedoOperation} construction during the redo load, {@code recoverKey},
   * {@code applyBucket}, {@code applyToRestore}) and {@code JdbcDatabase.put} <b>at exit</b> (after
   * the write has committed) — so each stage of recover&rarr;replay&rarr;write-back is killed
   * precisely.
   *
   * <p>Determinism: the restore runs with a single replay worker, so the Byteman counter has no
   * thread races and the crash fires on exactly the k-th call of the armed site. A disarmed
   * baseline restore first establishes the converged oracle image; then each site is armed in turn,
   * the restore is killed mid-flight, and a re-run from the same backup must re-derive the
   * identical image and stay consistent + conserved — no matter where the first attempt died.
   *
   * <p>Teeth: every site asserts the injection fired exactly once ({@link
   * CrashInjector#firedCount()} {@code == 1}). If a rule failed to bind (renamed method / wrong
   * signature) the crash would never fire, the fired count would be 0, and that assertion would
   * fail loudly rather than passing a test that silently injects nothing.
   */
  @Test
  void restore_CrashInjectedAtNamedSites_ShouldConvergeOnReRun() throws Exception {
    // Arrange: install the crash rules on the running JVM's Byteman agent, run the conservation
    // workload, capture a non-snapshot-consistent copy + restored coordinator, and build a
    // single-worker restore so the crash counter is race-free (crash fires on exactly the k-th
    // call).
    Submit submit = new Submit();
    submit.addScripts(Arrays.asList(new ScriptText("cbrl-crash-rules.btm", CRASH_RULES)));
    try {
      long initialBalance = 1_000L;
      seedBalances(SEEDED_KEYS, initialBalance); // Half seeded; the rest are created in-window.
      long total = (long) SEEDED_KEYS * initialBalance;
      manager.enableRedoLogging(BACKUP_LABEL); // Open the backup window.
      AtomicBoolean stop = new AtomicBoolean(false);
      List<Future<?>> workload = startTransferWorkload(stop);
      Uninterruptibles.sleepUninterruptibly(CRASH_TEST_BACKUP_WINDOW);
      Map<String, List<Put>> userTableBackup = captureUserTableBackup();
      // Guarantee a floor of divergence on the main thread, then let the workload add more over the
      // window, so the backup diverges from the consistency point on more keys than any crash
      // site's
      // fireAt regardless of machine load (see divergeBackupFromConsistencyPoint).
      divergeBackupFromConsistencyPoint();
      Uninterruptibles.sleepUninterruptibly(CRASH_TEST_BACKUP_WINDOW);
      Map<String, Coordinator.State> backup = backupCoordinator();
      stop.set(true);
      for (Future<?> future : workload) {
        future.get();
      }
      restoreUserTableBackup(userTableBackup);
      arrangeRestoredCoordinator(backup);
      CbrlRestore restore = newSingleWorkerRestore();

      // Act (baseline, disarmed): a clean restore establishes the converged oracle image.
      CrashInjector.disarm();
      restore.restore();
      Map<Integer, Long> oracle = readRestoredTokens();

      // Assert (baseline): the oracle is itself consistent and conserved before any crash.
      assertThat(findConsistencyViolations()).as("baseline restore consistent").isEmpty();
      assertThat(sumRestoredBalances(TABLE_A, A_TOKEN)).as("baseline conserved").isEqualTo(total);
      assertThat(oracle).as("baseline restored the seeded accounts").isNotEmpty();

      // Act & Assert (each named site): kill the restore there, then re-run to convergence.
      crashAtSiteThenConverge("redoLoad", 3, restore, userTableBackup, oracle, total);
      crashAtSiteThenConverge("recoverKey", 3, restore, userTableBackup, oracle, total);
      crashAtSiteThenConverge("applyBucket", 2, restore, userTableBackup, oracle, total);
      crashAtSiteThenConverge("writeBackEntry", 3, restore, userTableBackup, oracle, total);
      crashAtSiteThenConverge("jdbcPutExit", 3, restore, userTableBackup, oracle, total);
    } finally {
      CrashInjector.disarm();
      submit.deleteScripts(Arrays.asList(new ScriptText("cbrl-crash-rules.btm", CRASH_RULES)));
    }
  }

  /**
   * Reinstalls the non-snapshot base, arms the crash at {@code site} on its {@code fireAt}-th call,
   * runs the restore (which must die there), then re-runs it to completion on the partial state the
   * crash left and asserts the re-derived image equals the no-crash {@code oracle} and is still
   * consistent and conserved.
   */
  private void crashAtSiteThenConverge(
      String site,
      int fireAt,
      CbrlRestore restore,
      Map<String, List<Put>> userTableBackup,
      Map<Integer, Long> oracle,
      long total)
      throws Exception {
    // Arrange: fresh non-snapshot base for this attempt, and arm the crash at the named site.
    restoreUserTableBackup(userTableBackup);
    CrashInjector.arm(site, fireAt);

    // Act: the armed restore must be killed at the injection point; then re-run on the partial
    // state (no re-install) so convergence is proven from wherever the crash left off.
    Throwable crash = catchThrowable(restore::restore);
    int fired = CrashInjector.firedCount();
    // The physical state the crash left, read with a raw storage get (no transaction, so Consensus
    // Commit lazy recovery does not mask it) before the re-run. It must differ from the converged
    // oracle: the crash fires at the k-th of many keys, so most keys still hold their non-snapshot
    // copy values, proving the crash interrupted the restore before completion — the re-run
    // genuinely
    // recovers rather than finding the work already done. (Reading it transactionally would let
    // lazy
    // recovery resolve in-flight copy records toward the committed image and could spuriously
    // equal
    // the oracle.)
    Map<Integer, Long> afterCrash = readRestoredTokensRaw();
    CrashInjector.disarm();
    restore.restore();
    Map<Integer, Long> reRun = readRestoredTokens();

    // Assert: the injection fired exactly once (teeth), the restore was actually killed, and the
    // re-derived image is identical to the no-crash oracle and still consistent + conserved.
    assertThat(fired)
        .as("crash at %s fired exactly once — its Byteman rule bound and tripped", site)
        .isEqualTo(1);
    assertThat(crash).as("the armed restore at %s was killed mid-flight", site).isNotNull();
    assertThat(causedByCrashInjection(crash))
        .as("the restore at %s died from the injected crash, not an unrelated error", site)
        .isTrue();
    assertThat(afterCrash)
        .as("crash at %s left the restore incomplete, so the re-run genuinely recovers", site)
        .isNotEqualTo(oracle);
    assertThat(reRun)
        .as("re-run after crash at %s re-derives the identical final image", site)
        .isEqualTo(oracle);
    assertThat(findConsistencyViolations())
        .as("re-run after crash at %s: cross-table consistency", site)
        .isEmpty();
    assertThat(sumRestoredBalances(TABLE_A, A_TOKEN))
        .as("re-run after crash at %s: table_a conserved", site)
        .isEqualTo(total);
    assertThat(sumRestoredBalances(TABLE_B, B_TOKEN))
        .as("re-run after crash at %s: table_b conserved", site)
        .isEqualTo(total);
  }

  /** Whether the injected crash appears anywhere in the throwable's cause chain. */
  private static boolean causedByCrashInjection(Throwable thrown) {
    for (Throwable cause = thrown; cause != null; cause = cause.getCause()) {
      if (cause instanceof CrashInjector.CrashInjectionError) {
        return true;
      }
    }
    return false;
  }

  /**
   * A {@link CbrlRestore} on the real (non-proxy) handles that Byteman intercepts, pinned to a
   * single replay worker so the injected crash fires on exactly the k-th call of a site with no
   * thread races on the counter.
   */
  private CbrlRestore newSingleWorkerRestore() throws Exception {
    Properties properties = properties();
    properties.setProperty(CbrlConfig.REPLAY_WORKERS, "1");
    return new CbrlRestore(properties, BACKUP_LABEL, storage, manager, storageAdmin);
  }

  private List<Future<?>> startTransferWorkload(AtomicBoolean stop) {
    List<Future<?>> futures = new ArrayList<>(WORKLOAD_THREADS);
    for (int t = 0; t < WORKLOAD_THREADS; t++) {
      futures.add(
          workerExecutor.submit(
              () -> {
                while (!stop.get()) {
                  transferOnce();
                }
              }));
    }
    return futures;
  }

  /** One balance-preserving transfer between two random accounts, atomic across both tables. */
  private void transferOnce() {
    int from = ThreadLocalRandom.current().nextInt(RECORD_COUNT);
    int to = ThreadLocalRandom.current().nextInt(RECORD_COUNT);
    if (from == to) {
      return;
    }
    long amount = 1 + ThreadLocalRandom.current().nextLong(100);
    withRetry(
        tx -> {
          long fromBalance = readBalance(tx, from);
          long toBalance = readBalance(tx, to);
          writeBalance(tx, from, fromBalance - amount);
          writeBalance(tx, to, toBalance + amount);
        });
    committedCount.incrementAndGet();
  }

  /**
   * Deterministically diverges the user-table backup from the consistency point by {@link
   * #DIVERGENCE_FLOOR_KEYS} keys, committed on the MAIN thread between the two backups. This is a
   * floor the background workload cannot be relied on to provide: under machine load the workload
   * threads can be starved for the whole capture-to-coordinator window, leaving the backup ~equal
   * to the consistency point, which starves the crash teeth (a late crash then finds the restore
   * already converged, or leaves too little work to catch it mid-flight). Balance-conserving
   * transfers among seeded keys, so the total invariant and the clean-restore oracle are unchanged.
   */
  private void divergeBackupFromConsistencyPoint() {
    for (int i = 0; i + 1 < DIVERGENCE_FLOOR_KEYS; i += 2) {
      int from = i;
      int to = i + 1;
      withRetry(
          tx -> {
            long fromBalance = readBalance(tx, from);
            long toBalance = readBalance(tx, to);
            writeBalance(tx, from, fromBalance - 1);
            writeBalance(tx, to, toBalance + 1);
          });
    }
  }

  /** Reads an account's balance, pulling both tables into the read set so its writes chain. */
  private long readBalance(DistributedTransaction tx, int i) throws Exception {
    Optional<Result> a = tx.get(getForTableA(USER_NAMESPACE, i)); // Absent (un-seeded) reads as 0.
    tx.get(
        getForTableB(USER_NAMESPACE, i)); // Read so the table_b write links to its prior version.
    return a.map(r -> r.getBigInt(A_TOKEN)).orElse(0L);
  }

  private void writeBalance(DistributedTransaction tx, int i, long balance) throws Exception {
    tx.put(putForTableA(USER_NAMESPACE, i, balance));
    tx.put(putForTableB(USER_NAMESPACE, i, balance));
  }

  private long sumRestoredBalances(String table, String balanceColumn) {
    long sum = 0;
    for (int i = 0; i < RECORD_COUNT; i++) {
      Optional<Result> result =
          table.equals(TABLE_A)
              ? getWithRetry(getForTableA(USER_NAMESPACE, i))
              : getWithRetry(getForTableB(USER_NAMESPACE, i));
      if (result.isPresent()) {
        sum += result.get().getBigInt(balanceColumn);
      }
    }
    return sum;
  }

  private Map<Integer, Long> seedPreWindowBase(int keyCount) {
    Map<Integer, Long> seed = new HashMap<>();
    for (int i = 0; i < keyCount; i++) {
      long token = tokenCounter.incrementAndGet();
      int key = i;
      seed.put(i, token);
      withRetry(
          tx -> {
            tx.put(putForTableA(USER_NAMESPACE, key, token));
            tx.put(putForTableB(USER_NAMESPACE, key, token));
          });
    }
    return seed;
  }

  private boolean rollDelete() {
    return ThreadLocalRandom.current().nextInt(100) < DELETE_PERCENTAGE;
  }

  /**
   * Captures a raw, storage-level copy of the user tables (all columns, with transaction metadata)
   * — the non-snapshot-consistent copy of the user-table data. Taken WHILE the workload runs, so it
   * catches in-flight (PREPARED/DELETED) records and a different instant per row. Held in memory
   * and re-installed by {@link #restoreUserTableBackup} before restore, modelling the user-table
   * physical backup that is restored ahead of CBRL.
   */
  private Map<String, List<Put>> captureUserTableBackup() throws Exception {
    Map<String, List<Put>> userTableBackup = new LinkedHashMap<>();
    for (String table : USER_TABLES) {
      List<Put> puts = new ArrayList<>();
      Scan scan = Scan.newBuilder().namespace(USER_NAMESPACE).table(table).all().build();
      try (Scanner scanner = storage.scan(scan)) {
        for (Result result : scanner.all()) {
          PutBuilder.Buildable builder =
              Put.newBuilder()
                  .namespace(USER_NAMESPACE)
                  .table(table)
                  .partitionKey(result.getPartitionKey().orElseThrow(IllegalStateException::new));
          result.getClusteringKey().ifPresent(builder::clusteringKey);
          for (Column<?> column : result.getColumns().values()) {
            if (!isKeyColumn(column.getName(), table)) {
              builder.value(column); // capture user + transaction-metadata columns verbatim
            }
          }
          puts.add(builder.build());
        }
      }
      userTableBackup.put(table, puts);
    }
    return userTableBackup;
  }

  /**
   * Installs the captured copy as the restore base, in place: truncate each user table, then
   * rewrite the copy verbatim. This models the physical restore of the user-table backup
   * (discarding the live post-copy state) before CBRL runs, and resets the base between crash
   * iterations.
   */
  private void restoreUserTableBackup(Map<String, List<Put>> userTableBackup) throws Exception {
    for (String table : USER_TABLES) {
      admin.truncateTable(USER_NAMESPACE, table);
      for (Put put : userTableBackup.get(table)) {
        storage.put(put);
      }
    }
  }

  /** Empties the user tables: the restore base when the user-table backup is NOT restored. */
  private void clearUserTables() throws Exception {
    for (String table : USER_TABLES) {
      admin.truncateTable(USER_NAMESPACE, table);
    }
  }

  private static boolean isKeyColumn(String name, String table) {
    if (table.equals(TABLE_A)) {
      return name.equals(A_PK);
    }
    if (table.equals(TABLE_B)) {
      return name.equals(B_PK) || name.equals(B_CK);
    }
    return name.equals(C_PK);
  }

  /**
   * The restore path must add no coordinator rows of its own: it applies records with the Storage
   * API and resolves in-flight ones via record-level recovery against the already-restored
   * coordinator, never a transaction. The coordinator after restore must equal what the restore
   * found (the restored backup).
   */
  @Test
  void restore_ShouldWriteNoCoordinatorRows() throws Exception {
    // Arrange
    long initialBalance = 1_000L;
    seedBalances(RECORD_COUNT, initialBalance);
    manager.enableRedoLogging(BACKUP_LABEL);
    AtomicBoolean stop = new AtomicBoolean(false);
    List<Future<?>> workload = startTransferWorkload(stop);
    Uninterruptibles.sleepUninterruptibly(WORKLOAD_WARMUP);
    Map<String, List<Put>> userTableBackup = captureUserTableBackup();
    Uninterruptibles.sleepUninterruptibly(WORKLOAD_AFTER_USER_BACKUP);
    Map<String, Coordinator.State> backup = backupCoordinator();
    stop.set(true);
    for (Future<?> future : workload) {
      future.get();
    }
    restoreUserTableBackup(userTableBackup);
    arrangeRestoredCoordinator(backup);

    // Act
    Set<String> coordinatorBefore = coordinatorTxIds();
    cbrlRestore.restore();
    Set<String> coordinatorAfter = coordinatorTxIds();

    // Assert
    assertThat(coordinatorAfter)
        .as("restore must not write any coordinator rows of its own")
        .isEqualTo(coordinatorBefore);
  }

  /**
   * Test arrange: simulate the physical restore of the coordinator table into the target — every
   * backed-up transaction COMMITTED, and every in-flight copy record whose writer is absent from
   * the backup ABORTED. In production this is the backup tool restoring the coordinator table like
   * any other; {@link CbrlRestore} itself never writes or truncates the coordinator.
   */
  private void arrangeRestoredCoordinator(Map<String, Coordinator.State> backup) throws Exception {
    admin.truncateCoordinatorTables();
    // Faithful physical restore: rewrite each backed-up coordinator row verbatim (state,
    // created_at, write_set, child_ids) with a raw storage.put — NOT coordinator.putState, which
    // would re-stamp created_at to now and (for group commit) drop the write_set. CbrlRestore now
    // reads the redo and the original commit times from the restored coordinator table itself, so
    // those columns must survive the restore exactly as a real coordinator-table backup would.
    for (Coordinator.State state : backup.values()) {
      storage.put(coordinatorRowPut(state));
    }
    // Compare copy records' tx_id against the FULL committed ids (a group-commit child record
    // carries its parent+child id, not the parent row key), so committed children are not
    // misclassified as in-doubt and given a spurious ABORTED state.
    for (String txId : inDoubtBackupTxIds(committedFullIdsIn(backup))) {
      storage.put(abortedCoordinatorRowPut(txId));
    }
  }

  /** A faithful raw restore of a backed-up coordinator row — every column preserved. */
  private Put coordinatorRowPut(Coordinator.State state) {
    PutBuilder.Buildable put =
        Put.newBuilder()
            .namespace(COORDINATOR_NAMESPACE)
            .table(Coordinator.TABLE)
            .partitionKey(Key.ofText(Attribute.ID, state.getId()))
            .intValue(Attribute.STATE, state.getState().get())
            .bigIntValue(Attribute.CREATED_AT, state.getCreatedAt());
    state.getWriteSet().ifPresent(ws -> put.blobValue(Attribute.WRITE_SET, ws.toByteArray()));
    if (!state.getChildIds().isEmpty()) {
      put.textValue(Attribute.CHILD_IDS, String.join(",", state.getChildIds()));
    }
    return put.build();
  }

  /**
   * An ABORTED coordinator row for an in-doubt copy record's writer (created_at must be non-zero).
   */
  private Put abortedCoordinatorRowPut(String txId) {
    return Put.newBuilder()
        .namespace(COORDINATOR_NAMESPACE)
        .table(Coordinator.TABLE)
        .partitionKey(Key.ofText(Attribute.ID, txId))
        .intValue(Attribute.STATE, TransactionState.ABORTED.get())
        .bigIntValue(Attribute.CREATED_AT, System.currentTimeMillis())
        .build();
  }

  /**
   * Transaction ids of copy records still in an in-flight state (PREPARED/DELETED) whose
   * transaction is not in the backup — in flight at copy time, never committed by the consistency
   * point.
   */
  private Set<String> inDoubtBackupTxIds(Set<String> committedInBackup) throws Exception {
    Set<String> inDoubt = new HashSet<>();
    for (String table : USER_TABLES) {
      Scan scan = Scan.newBuilder().namespace(USER_NAMESPACE).table(table).all().build();
      try (Scanner scanner = storage.scan(scan)) {
        for (Result result : scanner.all()) {
          if (result.isNull(Attribute.ID)
              || result.isNull(Attribute.STATE)
              || result.getInt(Attribute.STATE) == TransactionState.COMMITTED.get()) {
            continue;
          }
          String txId = result.getText(Attribute.ID);
          if (!committedInBackup.contains(txId)) {
            inDoubt.add(txId);
          }
        }
      }
    }
    return inDoubt;
  }

  /** The transaction ids currently present in the coordinator table. */
  private Set<String> coordinatorTxIds() throws Exception {
    Set<String> ids = new HashSet<>();
    Scan scan =
        Scan.newBuilder().namespace(COORDINATOR_NAMESPACE).table(Coordinator.TABLE).all().build();
    try (Scanner scanner = storage.scan(scan)) {
      for (Result result : scanner.all()) {
        ids.add(result.getText(Attribute.ID));
      }
    }
    return ids;
  }

  /** Backs up the coordinator WHILE live and closes it over the chain (self-contained). */
  private Map<String, Coordinator.State> backupCoordinator() throws Exception {
    Map<String, Coordinator.State> committedById = new HashMap<>();
    Scan scan =
        Scan.newBuilder().namespace(COORDINATOR_NAMESPACE).table(Coordinator.TABLE).all().build();
    try (Scanner scanner = storage.scan(scan)) {
      for (Result result : scanner.all()) {
        if (committedWithWriteSet(result)) {
          Coordinator.State state = new Coordinator.State(result);
          committedById.put(state.getId(), state);
        }
      }
    }
    closeOverChain(committedById);
    return committedById;
  }

  /** Whether a coordinator row is a COMMITTED row carrying a write set (the redo-bearing rows). */
  private static boolean committedWithWriteSet(Result result) {
    return !result.isNull(Attribute.STATE)
        && result.getInt(Attribute.STATE) == TransactionState.COMMITTED.get()
        && !result.isNull(Attribute.WRITE_SET);
  }

  private void closeOverChain(Map<String, Coordinator.State> committedById) throws Exception {
    Deque<String> pending = new ArrayDeque<>();
    for (Coordinator.State state : committedById.values()) {
      collectPrevTxIds(state, committedById, pending);
    }
    while (!pending.isEmpty()) {
      String prevTxId = pending.poll();
      // A prev_tx_id may be a group-commit child's full id, but its coordinator row is keyed by the
      // parent id; resolve to the row key before looking it up or fetching it.
      String rowKey =
          keyManipulator.isFullKey(prevTxId)
              ? keyManipulator.keysFromFullKey(prevTxId).parentKey
              : prevTxId;
      if (committedById.containsKey(rowKey)) {
        continue;
      }
      Coordinator.State state = fetchCommittedCoordinatorRow(rowKey);
      if (state == null) {
        continue;
      }
      committedById.put(rowKey, state);
      collectPrevTxIds(state, committedById, pending);
    }
  }

  private static void collectPrevTxIds(
      Coordinator.State state, Map<String, Coordinator.State> known, Deque<String> pending) {
    for (EntryGroup group : state.getWriteSet().get().getEntryGroupsList()) {
      for (Entry entry : group.getEntriesList()) {
        if (entry.hasPrevTxId() && !known.containsKey(entry.getPrevTxId())) {
          pending.add(entry.getPrevTxId());
        }
      }
    }
  }

  /** The COMMITTED-with-write-set coordinator row for {@code txId}, or null if it is neither. */
  @Nullable
  private Coordinator.State fetchCommittedCoordinatorRow(String txId) throws Exception {
    Get get =
        Get.newBuilder()
            .namespace(COORDINATOR_NAMESPACE)
            .table(Coordinator.TABLE)
            .partitionKey(Key.ofText(Attribute.ID, txId))
            .build();
    Optional<Result> result = storage.get(get);
    return result.isPresent() && committedWithWriteSet(result.get())
        ? new Coordinator.State(result.get())
        : null;
  }

  private List<String> findConsistencyViolations() {
    List<String> violations = new ArrayList<>();
    for (int i = 0; i < RECORD_COUNT; i++) {
      Optional<Result> a = getWithRetry(getForTableA(USER_NAMESPACE, i));
      Optional<Result> b = getWithRetry(getForTableB(USER_NAMESPACE, i));
      if (a.isPresent() != b.isPresent()) {
        violations.add(
            String.format("key %d presence: a=%s b=%s", i, a.isPresent(), b.isPresent()));
      } else if (a.isPresent()) {
        long tokenA = a.get().getBigInt(A_TOKEN);
        long tokenB = b.get().getBigInt(B_TOKEN);
        if (tokenA != tokenB) {
          violations.add(String.format("key %d token: a=%d b=%d", i, tokenA, tokenB));
        }
      }
    }
    return violations;
  }

  /** How many seeded keys [0, SEEDED_KEYS) survive — transfers never delete them, so all must. */
  private int presentSeededKeyCount() {
    int count = 0;
    for (int i = 0; i < SEEDED_KEYS; i++) {
      if (getWithRetry(getForTableA(USER_NAMESPACE, i)).isPresent()) {
        count++;
      }
    }
    return count;
  }

  /** Seeds keys [0, count) with {@code initialBalance} in both tables, pre-window (logging off). */
  private void seedBalances(int count, long initialBalance) {
    for (int i = 0; i < count; i++) {
      int key = i;
      withRetry(
          tx -> {
            tx.put(putForTableA(USER_NAMESPACE, key, initialBalance));
            tx.put(putForTableB(USER_NAMESPACE, key, initialBalance));
          });
    }
  }

  private Map<Integer, Long> readRestoredTokens() {
    Map<Integer, Long> tokens = new HashMap<>();
    for (int i = 0; i < RECORD_COUNT; i++) {
      Optional<Result> a = getWithRetry(getForTableA(USER_NAMESPACE, i));
      if (a.isPresent()) {
        tokens.put(i, a.get().getBigInt(A_TOKEN));
      }
    }
    return tokens;
  }

  // Like readRestoredTokens, but a raw storage get (no transaction) so it reflects the physical
  // state without triggering Consensus Commit lazy recovery — used to observe the incomplete state
  // a
  // crash left, which must not be masked into the converged image.
  private Map<Integer, Long> readRestoredTokensRaw() throws ExecutionException {
    Map<Integer, Long> tokens = new HashMap<>();
    for (int i = 0; i < RECORD_COUNT; i++) {
      Optional<Result> a = storage.get(getForTableA(USER_NAMESPACE, i));
      if (a.isPresent() && !a.get().isNull(A_TOKEN)) {
        tokens.put(i, a.get().getBigInt(A_TOKEN));
      }
    }
    return tokens;
  }

  private void withRetry(TxBody body) {
    RuntimeException last = null;
    for (int retry = 0; retry < 100; retry++) {
      DistributedTransaction tx = null;
      try {
        tx = manager.begin();
        body.run(tx);
        tx.commit();
        return;
      } catch (Exception e) {
        last = e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
        abortQuietly(tx);
        Uninterruptibles.sleepUninterruptibly(WORKLOAD_RETRY_BACKOFF);
      }
    }
    throw last;
  }

  private Optional<Result> getWithRetry(Get get) {
    RuntimeException last = null;
    for (int retry = 0; retry < 100; retry++) {
      DistributedTransaction tx = null;
      try {
        tx = manager.begin();
        Optional<Result> result = tx.get(get);
        tx.commit();
        return result;
      } catch (Exception e) {
        last = e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
        abortQuietly(tx);
        Uninterruptibles.sleepUninterruptibly(WORKLOAD_RETRY_BACKOFF);
      }
    }
    throw last;
  }

  private void abortQuietly(@Nullable DistributedTransaction tx) {
    if (tx != null) {
      try {
        tx.abort();
      } catch (Exception ignored) {
        // Best effort.
      }
    }
  }

  private Put putForTableA(String namespace, int i, long token) {
    byte[] blob = new byte[8];
    ThreadLocalRandom.current().nextBytes(blob);
    return Put.newBuilder()
        .namespace(namespace)
        .table(TABLE_A)
        .partitionKey(Key.ofInt(A_PK, i))
        .bigIntValue(A_TOKEN, token)
        .intValue(A_INT, ThreadLocalRandom.current().nextInt())
        .textValue(A_TEXT, Long.toHexString(ThreadLocalRandom.current().nextLong()))
        .booleanValue(A_BOOL, ThreadLocalRandom.current().nextBoolean())
        .blobValue(A_BLOB, blob)
        .build();
  }

  private Put putForTableB(String namespace, int i, long token) {
    return Put.newBuilder()
        .namespace(namespace)
        .table(TABLE_B)
        .partitionKey(Key.ofInt(B_PK, i))
        .clusteringKey(Key.ofInt(B_CK, i))
        .bigIntValue(B_TOKEN, token)
        .textValue(B_TEXT, Long.toHexString(ThreadLocalRandom.current().nextLong()))
        .build();
  }

  private Delete deleteForTableA(String namespace, int i) {
    return Delete.newBuilder()
        .namespace(namespace)
        .table(TABLE_A)
        .partitionKey(Key.ofInt(A_PK, i))
        .build();
  }

  private Delete deleteForTableB(String namespace, int i) {
    return Delete.newBuilder()
        .namespace(namespace)
        .table(TABLE_B)
        .partitionKey(Key.ofInt(B_PK, i))
        .clusteringKey(Key.ofInt(B_CK, i))
        .build();
  }

  /** A record with one value of every ScalarDB column type, for the all-types round-trip test. */
  private Put allTypesPut(int key) {
    return Put.newBuilder()
        .namespace(USER_NAMESPACE)
        .table(TABLE_C)
        .partitionKey(Key.ofInt(C_PK, key))
        .booleanValue(C_BOOLEAN, true)
        .intValue(C_INT, 42)
        .bigIntValue(C_BIGINT, 9_000_000_000L)
        .floatValue(C_FLOAT, 1.5f)
        .doubleValue(C_DOUBLE, 2.25d)
        .textValue(C_TEXT, "hello")
        .blobValue(C_BLOB, new byte[] {1, 2, 3})
        .dateValue(C_DATE, LocalDate.of(2026, 6, 24))
        .timeValue(C_TIME, LocalTime.of(1, 2, 3))
        .timestampValue(C_TIMESTAMP, LocalDateTime.of(2026, 6, 24, 1, 2, 3))
        .timestampTZValue(C_TIMESTAMPTZ, Instant.ofEpochSecond(1_700_000_000L))
        .build();
  }

  private Get getForTableC(int key) {
    return Get.newBuilder()
        .namespace(USER_NAMESPACE)
        .table(TABLE_C)
        .partitionKey(Key.ofInt(C_PK, key))
        .build();
  }

  /**
   * A raw PREPARED copy record carrying the writer's after-image (insert-style: no before-image),
   * fabricated to simulate the raw scan catching a record mid-commit. Mirrors what {@code
   * PrepareMutationComposer} writes for an initial record: user columns + {@code tx_id}, {@code
   * tx_state = PREPARED}, {@code tx_prepared_at}, {@code tx_version}.
   */
  private Put preparedRecordForTableA(
      String namespace, int key, Result afterImage, String txId, int version) {
    PutBuilder.Buildable put =
        Put.newBuilder().namespace(namespace).table(TABLE_A).partitionKey(Key.ofInt(A_PK, key));
    for (String column : A_USER_COLUMNS) {
      put.value(afterImage.getColumns().get(column));
    }
    return put.textValue(Attribute.ID, txId)
        .intValue(Attribute.STATE, TransactionState.PREPARED.get())
        .bigIntValue(Attribute.PREPARED_AT, System.currentTimeMillis())
        .intValue(Attribute.VERSION, version)
        .build();
  }

  /**
   * A raw DELETED copy record (a tombstone: tx metadata only, no user columns), fabricated like
   * {@link #preparedRecordForTableA}. The version is arbitrary — delete roll-forward keys off
   * {@code tx_state = DELETED} and {@code tx_id} only.
   */
  private Put deletedRecordForTableA(String namespace, int key, String txId) {
    return Put.newBuilder()
        .namespace(namespace)
        .table(TABLE_A)
        .partitionKey(Key.ofInt(A_PK, key))
        .textValue(Attribute.ID, txId)
        .intValue(Attribute.STATE, TransactionState.DELETED.get())
        .bigIntValue(Attribute.PREPARED_AT, System.currentTimeMillis())
        .intValue(Attribute.VERSION, 2)
        .build();
  }

  private Get getForTableA(String namespace, int i) {
    return Get.newBuilder()
        .namespace(namespace)
        .table(TABLE_A)
        .partitionKey(Key.ofInt(A_PK, i))
        .build();
  }

  private Get getForTableB(String namespace, int i) {
    return Get.newBuilder()
        .namespace(namespace)
        .table(TABLE_B)
        .partitionKey(Key.ofInt(B_PK, i))
        .clusteringKey(Key.ofInt(B_CK, i))
        .build();
  }
}
