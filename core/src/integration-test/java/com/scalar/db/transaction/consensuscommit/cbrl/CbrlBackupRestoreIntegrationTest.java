package com.scalar.db.transaction.consensuscommit.cbrl;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
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
import com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
 *   <li>{@link #disjointOwnerWorkload_restoresLatestValuePerColumn()} — each key is owned by one
 *       worker writing a monotonically increasing token, with column values derived from that token
 *       and a {@code token % 6} subset written per transaction. The coordinator is backed up
 *       <b>live</b> (non-pausing); the oracle is each key's recorded ops applied up to the backup's
 *       captured prefix, and every restored column must equal the value of its last-in-prefix
 *       writer. This proves window-consistency, that no older write overwrites a newer one, that
 *       partial after-images MERGE (untouched columns carried forward), and — via keys untouched
 *       in-window — that the copy is load-bearing.
 *   <li>{@link #concurrentSameKeyTransfers_restorePreservesConservation()} — many workers run
 *       balance-preserving transfers on shared accounts, so the same record is written concurrently
 *       and conflicts. This is the same-key contention that leaves the copy with in-flight PREPARED
 *       records (recovered forward) and conflict-aborted records (recovered back); the conservation
 *       invariant (total balance and per-account cross-table equality) must survive restore. The
 *       backup is taken WHILE the workload runs, proving it does not pause.
 * </ul>
 *
 * <p>The two negative controls keep the checks honest: {@link
 * #consistencyCheckDetectsInconsistentImage_negativeControl()} proves the consistency check has
 * teeth, and {@link #copyIsLoadBearing_negativeControl()} restores the same backup <b>without</b>
 * the copy and shows the pre-window data is then unrecoverable.
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
  private static final String SRC_NAMESPACE = "cbrl_src";
  private static final String RESTORE_NAMESPACE = "cbrl_restore";
  private static final String COORDINATOR_NAMESPACE = "cbrl_coordinator";

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
  private static final String[] B_USER_COLUMNS = {B_TOKEN, B_TEXT};
  private static final TableMetadata TABLE_B_METADATA =
      TableMetadata.newBuilder()
          .addColumn(B_PK, DataType.INT)
          .addColumn(B_CK, DataType.INT)
          .addColumn(B_TOKEN, DataType.BIGINT)
          .addColumn(B_TEXT, DataType.TEXT)
          .addPartitionKey(B_PK)
          .addClusteringKey(B_CK)
          .build();

  private static final int RECORD_COUNT = 40;
  // Keys [0, PRE_WINDOW_ONLY_KEYS) are seeded pre-window and never touched in-window: zero redo,
  // restored from the copy alone (the load-bearing proof). The rest are split into one disjoint
  // range per worker in the disjoint-owner test.
  private static final int PRE_WINDOW_ONLY_KEYS = 4;

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
  private static final Duration WORKLOAD_AFTER_COPY = Duration.ofMillis(400);
  private static final int REPLAY_BUCKETS = 8;
  private static final int REPLAY_WORKERS = 4;

  private DistributedTransactionManager manager;
  private DistributedTransactionAdmin admin;
  private DistributedStorage storage;
  private Coordinator coordinator;
  private final CoordinatorGroupCommitKeyManipulator keyManipulator =
      new CoordinatorGroupCommitKeyManipulator();
  private ExecutorService workerExecutor;
  private CbrlRestore cbrlRestore;
  private ConsensusCommitConfig consensusCommitConfig;
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
    // Start with redo logging OFF; the test opens the backup window (enables it) explicitly so the
    // pre-window base is unlogged and the copy is load-bearing.
    properties.setProperty(ConsensusCommitConfig.REDO_LOGGING_ENABLED, "false");
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
    storage = StorageFactory.create(properties).getStorage();
    consensusCommitConfig = new ConsensusCommitConfig(new DatabaseConfig(properties));
    coordinator = new Coordinator(storage, consensusCommitConfig);
    workerExecutor = Executors.newFixedThreadPool(WORKLOAD_THREADS);

    admin.createCoordinatorTables(true);
    for (String namespace : new String[] {SRC_NAMESPACE, RESTORE_NAMESPACE}) {
      admin.createNamespace(namespace, true);
      admin.createTable(namespace, TABLE_A, TABLE_A_METADATA, true);
      admin.createTable(namespace, TABLE_B, TABLE_B_METADATA, true);
    }

    cbrlRestore =
        new CbrlRestore(
            storage,
            manager,
            coordinator,
            admin,
            keyManipulator,
            RESTORE_NAMESPACE,
            userColumnsByTable(),
            REPLAY_BUCKETS,
            REPLAY_WORKERS);
  }

  @AfterAll
  void afterAll() throws Exception {
    if (workerExecutor != null) {
      workerExecutor.shutdownNow();
    }
    if (admin != null) {
      for (String namespace : new String[] {SRC_NAMESPACE, RESTORE_NAMESPACE}) {
        admin.dropTable(namespace, TABLE_A, true);
        admin.dropTable(namespace, TABLE_B, true);
        admin.dropNamespace(namespace, true);
      }
      admin.dropCoordinatorTables(true);
      admin.dropNamespace(COORDINATOR_NAMESPACE, true);
      admin.close();
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
    for (String namespace : new String[] {SRC_NAMESPACE, RESTORE_NAMESPACE}) {
      admin.truncateTable(namespace, TABLE_A);
      admin.truncateTable(namespace, TABLE_B);
    }
  }

  /** Negative control: proves the consistency check detects an inconsistent image. */
  @Test
  void consistencyCheckDetectsInconsistentImage_negativeControl() {
    withRetry(
        tx -> {
          tx.put(putForTableA(RESTORE_NAMESPACE, 0, 111L));
          tx.put(putForTableB(RESTORE_NAMESPACE, 0, 222L));
        });
    withRetry(tx -> tx.put(putForTableA(RESTORE_NAMESPACE, 1, 333L)));

    List<String> violations = findConsistencyViolations();

    assertThat(violations).hasSize(2);
    assertThat(violations.toString()).contains("key 0").contains("key 1");
  }

  /**
   * Negative control for the load-bearing claim: restoring the same backup WITHOUT the copy leaves
   * the pre-window-only keys unrecoverable, since they have no redo and no other base.
   */
  @Test
  void copyIsLoadBearing_negativeControl() throws Exception {
    Map<Integer, Long> seed = seedPreWindowBase(PRE_WINDOW_ONLY_KEYS); // logging still OFF

    // Open the window and do a little in-window work on OTHER keys, so the backup has redo (the
    // restore path runs) but nothing touches the seeded keys.
    manager.enableRedoLogging();
    for (int i = PRE_WINDOW_ONLY_KEYS; i < PRE_WINDOW_ONLY_KEYS + 2; i++) {
      long token = tokenCounter.incrementAndGet();
      int key = i;
      withRetry(
          tx -> {
            tx.put(putForTableA(SRC_NAMESPACE, key, token));
            tx.put(putForTableB(SRC_NAMESPACE, key, token));
          });
    }

    Map<String, CoordinatorBackupRow> backup = backUpCoordinator();

    // Restore WITHOUT taking a copy: the restore tables stay empty as the repair base.
    cbrlRestore.restore(backup);

    for (int i = 0; i < PRE_WINDOW_ONLY_KEYS; i++) {
      assertThat(getWithRetry(getForTableA(RESTORE_NAMESPACE, i)))
          .as("without the copy, pre-window-only key %d is unrecoverable", i)
          .isEmpty();
    }
    assertThat(seed).hasSize(PRE_WINDOW_ONLY_KEYS); // sanity: the keys were in fact seeded
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
   * one (a regression yields a smaller token), that untouched columns are carried forward (a
   * replace-not-merge bug nulls them), and that post-backup writes do not leak in (their ids are
   * not in the captured set). Keys in {@code [0, PRE_WINDOW_ONLY_KEYS)} are seeded but never
   * touched in-window, so they also prove the copy is load-bearing (restored with no redo).
   */
  @Test
  void disjointOwnerWorkload_restoresLatestValuePerColumn() throws Exception {
    long[] seedToken = new long[RECORD_COUNT];
    boolean[] present = new boolean[RECORD_COUNT];
    Map<Integer, List<OwnedOp>> history = new HashMap<>();
    for (int i = 0; i < RECORD_COUNT; i++) {
      long token = tokenCounter.incrementAndGet();
      int key = i;
      seedToken[i] = token;
      present[i] = true;
      history.put(i, new ArrayList<>());
      withRetry(
          tx -> {
            tx.put(deterministicPutForTableA(key, token, ALL_COLUMNS));
            tx.put(
                deterministicPutForTableB(key, token, ALL_COLUMNS)
                    .orElseThrow(IllegalStateException::new));
          });
    }

    manager.enableRedoLogging(); // Open the backup window.

    AtomicBoolean stop = new AtomicBoolean(false);
    List<Future<?>> workload = startDisjointWorkload(stop, present, history);
    Uninterruptibles.sleepUninterruptibly(WORKLOAD_WARMUP);
    copyUserTables(); // Live, non-snapshot-consistent base; catches in-flight records to recover.
    Uninterruptibles.sleepUninterruptibly(WORKLOAD_AFTER_COPY);

    // Back up the coordinator WHILE the workload runs (Flow: back up, THEN stop) — non-pausing.
    long commitsBeforeBackup = committedCount.get();
    Map<String, CoordinatorBackupRow> coordinatorBackup = backUpCoordinator();
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

    cbrlRestore.restore(coordinatorBackup);

    assertThat(findConsistencyViolations()).as("consistency").isEmpty();
    assertThat(findDisjointViolations(seedToken, history, committedInBackup))
        .as(
            "every restored column == its last in-backup writer (merged, no regression, point-in-time)")
        .isEmpty();

    Map<Integer, Long> image = readRestoredTokens();
    cbrlRestore.restore(coordinatorBackup);
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
   * One transaction on an owned key: a full insert if absent, else a delete or a partial update of
   * the columns selected by {@code token % 6}. Appends the committed op (with its {@code txId}) to
   * the key's history for the prefix oracle.
   */
  private void mutateOwnedKey(int key, boolean[] present, Map<Integer, List<OwnedOp>> history) {
    long token = tokenCounter.incrementAndGet();
    boolean delete = present[key] && rollDelete();
    int mask =
        present[key] ? columnMask(token) : ALL_COLUMNS; // absent -> full deterministic insert
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
        tx.get(getForTableA(SRC_NAMESPACE, key)); // Read both so partial writes chain (merge).
        tx.get(getForTableB(SRC_NAMESPACE, key));
        if (delete) {
          tx.delete(deleteForTableA(SRC_NAMESPACE, key));
          tx.delete(deleteForTableB(SRC_NAMESPACE, key));
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
      }
    }
    throw last;
  }

  /** Full transaction ids whose writes the backup carries (the consistency-point committed set). */
  private Set<String> committedFullIdsIn(Map<String, CoordinatorBackupRow> backup) {
    Set<String> ids = new HashSet<>();
    for (CoordinatorBackupRow row : backup.values()) {
      for (EntryGroup group : row.writeSet.getEntryGroupsList()) {
        ids.add(
            group.getChildId().isEmpty()
                ? row.txId
                : keyManipulator.fullKey(row.txId, group.getChildId()));
      }
    }
    return ids;
  }

  /**
   * Every restored column compared against the prefix oracle: the seed base with each key's
   * recorded ops applied up to the last one whose transaction is in the backup's captured set.
   */
  private List<String> findDisjointViolations(
      long[] seedToken, Map<Integer, List<OwnedOp>> history, Set<String> committedInBackup) {
    List<String> violations = new ArrayList<>();
    for (int i = 0; i < RECORD_COUNT; i++) {
      boolean present = true;
      long[] cols = new long[USER_COLUMN_COUNT];
      Arrays.fill(cols, seedToken[i]);
      for (OwnedOp op : history.get(i)) {
        if (!committedInBackup.contains(op.txId)) {
          continue; // Committed after the cut (divergence) — not in the restored image.
        }
        if (op.delete) {
          present = false;
        } else {
          present = true;
          for (int c = 0; c < USER_COLUMN_COUNT; c++) {
            if ((op.mask & (1 << c)) != 0) {
              cols[c] = op.token;
            }
          }
        }
      }
      Optional<Result> a = getWithRetry(getForTableA(RESTORE_NAMESPACE, i));
      Optional<Result> b = getWithRetry(getForTableB(RESTORE_NAMESPACE, i));
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
      checkColumn(violations, i, "a.token", ra.getBigInt(A_TOKEN) == cols[COL_TOKEN]);
      checkColumn(violations, i, "a.int", ra.getInt(A_INT) == deriveInt(cols[COL_INT]));
      checkColumn(violations, i, "a.text", ra.getText(A_TEXT).equals(deriveText(cols[COL_TEXT])));
      checkColumn(violations, i, "a.bool", ra.getBoolean(A_BOOL) == deriveBool(cols[COL_BOOL]));
      checkColumn(
          violations,
          i,
          "a.blob",
          Arrays.equals(ra.getBlobAsBytes(A_BLOB), deriveBlob(cols[COL_BLOB])));
      checkColumn(violations, i, "b.token", rb.getBigInt(B_TOKEN) == cols[COL_TOKEN]);
      checkColumn(violations, i, "b.text", rb.getText(B_TEXT).equals(deriveText(cols[COL_TEXT])));
    }
    return violations;
  }

  private static void checkColumn(List<String> violations, int key, String what, boolean ok) {
    if (!ok) {
      violations.add(String.format("key %d %s mismatch", key, what));
    }
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
        Put.newBuilder().namespace(SRC_NAMESPACE).table(TABLE_A).partitionKey(Key.ofInt(A_PK, i));
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
            .namespace(SRC_NAMESPACE)
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
  void concurrentSameKeyTransfers_restorePreservesConservation() throws Exception {
    long initialBalance = 1_000L;
    for (int i = 0; i < RECORD_COUNT; i++) {
      int key = i;
      withRetry(
          tx -> {
            tx.put(putForTableA(SRC_NAMESPACE, key, initialBalance));
            tx.put(putForTableB(SRC_NAMESPACE, key, initialBalance));
          });
    }
    long total = (long) RECORD_COUNT * initialBalance;

    manager.enableRedoLogging(); // Open the backup window.

    AtomicBoolean stop = new AtomicBoolean(false);
    List<Future<?>> workload = startTransferWorkload(stop);
    Uninterruptibles.sleepUninterruptibly(WORKLOAD_WARMUP);
    copyUserTables();
    Uninterruptibles.sleepUninterruptibly(WORKLOAD_AFTER_COPY);

    // Back up the coordinator WHILE the workload runs — conservation tolerates a fuzzy cut.
    long commitsBeforeBackup = committedCount.get();
    Map<String, CoordinatorBackupRow> coordinatorBackup = backUpCoordinator();
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

    cbrlRestore.restore(coordinatorBackup);

    assertThat(findConsistencyViolations())
        .as("cross-table consistency: both tables hold the same balance per account")
        .isEmpty();
    assertThat(presentKeyCount())
        .as("no deletes: every account is present")
        .isEqualTo(RECORD_COUNT);
    assertThat(sumRestoredBalances(TABLE_A, A_TOKEN))
        .as("table_a total balance conserved")
        .isEqualTo(total);
    assertThat(sumRestoredBalances(TABLE_B, B_TOKEN))
        .as("table_b total balance conserved")
        .isEqualTo(total);
  }

  /**
   * Crash-in-the-middle: a restore killed partway and then re-run from the full backup must still
   * converge to a consistent, conserved image — re-running never corrupts the result, no matter
   * where the first attempt died. Uses the conservation workload (no per-key oracle needed). A
   * baseline restore is timed first; the crash is then injected at a sweep of points across that
   * measured duration, and after each kill the restore is re-run to completion and re-checked.
   *
   * <p>This is the design's crash-safety claim under test: write-back is one atomic transaction
   * that persists only the final replayed state (with a fresh tx id), and recovery/replay are
   * idempotent, so a re-run never sees a half-applied chain. The crash is injected by wrapping the
   * storage/manager/coordinator the restore uses in proxies that throw once a flag is set, so the
   * kill lands mid-operation (mid-recovery or mid-write-back) rather than relying on cooperative
   * thread interruption, which JDBC ignores.
   */
  @Test
  void crashMidRestore_reRunRestoresConsistently() throws Exception {
    long initialBalance = 1_000L;
    for (int i = 0; i < RECORD_COUNT; i++) {
      int key = i;
      withRetry(
          tx -> {
            tx.put(putForTableA(SRC_NAMESPACE, key, initialBalance));
            tx.put(putForTableB(SRC_NAMESPACE, key, initialBalance));
          });
    }
    long total = (long) RECORD_COUNT * initialBalance;

    manager.enableRedoLogging(); // Open the backup window.
    AtomicBoolean stop = new AtomicBoolean(false);
    List<Future<?>> workload = startTransferWorkload(stop);
    Uninterruptibles.sleepUninterruptibly(WORKLOAD_WARMUP);
    copyUserTables();
    Uninterruptibles.sleepUninterruptibly(WORKLOAD_AFTER_COPY);
    Map<String, CoordinatorBackupRow> backup = backUpCoordinator();
    stop.set(true);
    for (Future<?> future : workload) {
      future.get();
    }

    // The non-snapshot-consistent copy, captured so each crash iteration restarts from the same
    // base.
    Map<String, List<Put>> copySnapshot = snapshotRestoreTables();

    // Baseline: one full restore, to validate the happy path and to measure its duration.
    long startNanos = System.nanoTime();
    cbrlRestore.restore(backup);
    long restoreNanos = System.nanoTime() - startNanos;
    assertThat(findConsistencyViolations()).as("baseline restore consistent").isEmpty();
    assertThat(sumRestoredBalances(TABLE_A, A_TOKEN)).as("baseline conserved").isEqualTo(total);

    CbrlRestore crashingRestore = newCrashingRestore();
    int steps = 4;
    int killed = 0;
    for (int step = 1; step <= steps; step++) {
      resetRestoreTables(copySnapshot); // Fresh non-consistent base for this attempt.

      crashRestore.set(false);
      Throwable[] thrown = {null};
      Thread attempt =
          new Thread(
              () -> {
                try {
                  crashingRestore.restore(backup);
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
      cbrlRestore.restore(backup);

      assertThat(findConsistencyViolations())
          .as("re-run after crash at %d/%d: cross-table consistency", step, steps)
          .isEmpty();
      assertThat(presentKeyCount())
          .as("re-run after crash at %d/%d: every account present", step, steps)
          .isEqualTo(RECORD_COUNT);
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
  private CbrlRestore newCrashingRestore() {
    DistributedStorage crashingStorage = crashing(DistributedStorage.class, storage, false);
    DistributedTransactionManager crashingManager =
        crashing(DistributedTransactionManager.class, manager, true);
    Coordinator crashingCoordinator = new Coordinator(crashingStorage, consensusCommitConfig);
    return new CbrlRestore(
        crashingStorage,
        crashingManager,
        crashingCoordinator,
        admin,
        keyManipulator,
        RESTORE_NAMESPACE,
        userColumnsByTable(),
        REPLAY_BUCKETS,
        REPLAY_WORKERS);
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

  /**
   * Captures the current cbrl_restore rows (with metadata) so a crash iteration can reset to them.
   */
  private Map<String, List<Put>> snapshotRestoreTables() throws Exception {
    Map<String, List<Put>> snapshot = new LinkedHashMap<>();
    for (String table : new String[] {TABLE_A, TABLE_B}) {
      List<Put> puts = new ArrayList<>();
      Scan scan = Scan.newBuilder().namespace(RESTORE_NAMESPACE).table(table).all().build();
      try (Scanner scanner = storage.scan(scan)) {
        for (Result result : scanner.all()) {
          PutBuilder.Buildable builder =
              Put.newBuilder()
                  .namespace(RESTORE_NAMESPACE)
                  .table(table)
                  .partitionKey(result.getPartitionKey().orElseThrow(IllegalStateException::new));
          result.getClusteringKey().ifPresent(builder::clusteringKey);
          for (Column<?> column : result.getColumns().values()) {
            if (!isKeyColumn(column.getName(), table)) {
              builder.value(column);
            }
          }
          puts.add(builder.build());
        }
      }
      snapshot.put(table, puts);
    }
    return snapshot;
  }

  /**
   * Resets cbrl_restore to a snapshot (truncate then re-put) — the pre-restore non-consistent copy.
   */
  private void resetRestoreTables(Map<String, List<Put>> snapshot) throws Exception {
    for (String table : new String[] {TABLE_A, TABLE_B}) {
      admin.truncateTable(RESTORE_NAMESPACE, table);
      for (Put put : snapshot.get(table)) {
        storage.put(put);
      }
    }
  }

  private Map<String, List<String>> userColumnsByTable() {
    Map<String, List<String>> map = new LinkedHashMap<>();
    map.put(TABLE_A, Arrays.asList(A_USER_COLUMNS));
    map.put(TABLE_B, Arrays.asList(B_USER_COLUMNS));
    return map;
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

  /** Reads an account's balance, pulling both tables into the read set so its writes chain. */
  private long readBalance(DistributedTransaction tx, int i) throws Exception {
    Result a = tx.get(getForTableA(SRC_NAMESPACE, i)).orElseThrow(IllegalStateException::new);
    tx.get(getForTableB(SRC_NAMESPACE, i)); // Read so the table_b write links to its prior version.
    return a.getBigInt(A_TOKEN);
  }

  private void writeBalance(DistributedTransaction tx, int i, long balance) throws Exception {
    tx.put(putForTableA(SRC_NAMESPACE, i, balance));
    tx.put(putForTableB(SRC_NAMESPACE, i, balance));
  }

  private long sumRestoredBalances(String table, String balanceColumn) {
    long sum = 0;
    for (int i = 0; i < RECORD_COUNT; i++) {
      Optional<Result> result =
          table.equals(TABLE_A)
              ? getWithRetry(getForTableA(RESTORE_NAMESPACE, i))
              : getWithRetry(getForTableB(RESTORE_NAMESPACE, i));
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
            tx.put(putForTableA(SRC_NAMESPACE, key, token));
            tx.put(putForTableB(SRC_NAMESPACE, key, token));
          });
    }
    return seed;
  }

  private boolean rollDelete() {
    return ThreadLocalRandom.current().nextInt(100) < DELETE_PERCENTAGE;
  }

  /**
   * Raw, storage-level copy of the user tables (with metadata) into cbrl_restore — the
   * non-snapshot-consistent base.
   */
  private void copyUserTables() throws Exception {
    for (String table : new String[] {TABLE_A, TABLE_B}) {
      List<Put> puts = new ArrayList<>();
      Scan scan = Scan.newBuilder().namespace(SRC_NAMESPACE).table(table).all().build();
      try (Scanner scanner = storage.scan(scan)) {
        for (Result result : scanner.all()) {
          PutBuilder.Buildable builder =
              Put.newBuilder()
                  .namespace(RESTORE_NAMESPACE)
                  .table(table)
                  .partitionKey(result.getPartitionKey().orElseThrow(IllegalStateException::new));
          result.getClusteringKey().ifPresent(builder::clusteringKey);
          for (Column<?> column : result.getColumns().values()) {
            if (!isKeyColumn(column.getName(), table)) {
              builder.value(column); // copy user + transaction-metadata columns verbatim
            }
          }
          puts.add(builder.build());
        }
      }
      for (Put put : puts) {
        storage.put(put);
      }
    }
  }

  private static boolean isKeyColumn(String name, String table) {
    if (table.equals(TABLE_A)) {
      return name.equals(A_PK);
    }
    return name.equals(B_PK) || name.equals(B_CK);
  }

  /** Backs up the coordinator WHILE live and closes it over the chain (self-contained). */
  private Map<String, CoordinatorBackupRow> backUpCoordinator() throws Exception {
    Map<String, CoordinatorBackupRow> committedById = new HashMap<>();
    Scan scan =
        Scan.newBuilder().namespace(COORDINATOR_NAMESPACE).table(Coordinator.TABLE).all().build();
    try (Scanner scanner = storage.scan(scan)) {
      for (Result result : scanner.all()) {
        CoordinatorBackupRow row = toBackupRow(result);
        if (row.state == TransactionState.COMMITTED.get() && row.writeSet != null) {
          committedById.put(row.txId, row);
        }
      }
    }
    closeOverChain(committedById);
    return committedById;
  }

  private static CoordinatorBackupRow toBackupRow(Result result) throws Exception {
    String txId = result.getText(Attribute.ID);
    int state = result.isNull(Attribute.STATE) ? 0 : result.getInt(Attribute.STATE);
    String childIdsStr =
        result.isNull(Attribute.CHILD_IDS) ? null : result.getText(Attribute.CHILD_IDS);
    List<String> childIds =
        (childIdsStr == null || childIdsStr.isEmpty())
            ? Collections.emptyList()
            : Arrays.asList(childIdsStr.split(","));
    WriteSet writeSet =
        result.isNull(Attribute.WRITE_SET)
            ? null
            : WriteSet.parseFrom(result.getBlobAsBytes(Attribute.WRITE_SET));
    return new CoordinatorBackupRow(txId, state, childIds, writeSet);
  }

  private void closeOverChain(Map<String, CoordinatorBackupRow> committedById) throws Exception {
    Deque<String> pending = new ArrayDeque<>();
    for (CoordinatorBackupRow row : committedById.values()) {
      collectPrevTxIds(row, committedById, pending);
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
      CoordinatorBackupRow row = fetchCoordinatorRow(rowKey);
      if (row == null || row.state != TransactionState.COMMITTED.get() || row.writeSet == null) {
        continue;
      }
      committedById.put(rowKey, row);
      collectPrevTxIds(row, committedById, pending);
    }
  }

  private static void collectPrevTxIds(
      CoordinatorBackupRow row, Map<String, CoordinatorBackupRow> known, Deque<String> pending) {
    for (EntryGroup group : row.writeSet.getEntryGroupsList()) {
      for (Entry entry : group.getEntriesList()) {
        if (entry.hasPrevTxId() && !known.containsKey(entry.getPrevTxId())) {
          pending.add(entry.getPrevTxId());
        }
      }
    }
  }

  @Nullable
  private CoordinatorBackupRow fetchCoordinatorRow(String txId) throws Exception {
    Get get =
        Get.newBuilder()
            .namespace(COORDINATOR_NAMESPACE)
            .table(Coordinator.TABLE)
            .partitionKey(Key.ofText(Attribute.ID, txId))
            .build();
    Optional<Result> result = storage.get(get);
    return result.isPresent() ? toBackupRow(result.get()) : null;
  }

  private List<String> findConsistencyViolations() {
    List<String> violations = new ArrayList<>();
    for (int i = 0; i < RECORD_COUNT; i++) {
      Optional<Result> a = getWithRetry(getForTableA(RESTORE_NAMESPACE, i));
      Optional<Result> b = getWithRetry(getForTableB(RESTORE_NAMESPACE, i));
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

  private int presentKeyCount() {
    int count = 0;
    for (int i = 0; i < RECORD_COUNT; i++) {
      if (getWithRetry(getForTableA(RESTORE_NAMESPACE, i)).isPresent()) {
        count++;
      }
    }
    return count;
  }

  private Map<Integer, Long> readRestoredTokens() {
    Map<Integer, Long> tokens = new HashMap<>();
    for (int i = 0; i < RECORD_COUNT; i++) {
      Optional<Result> a = getWithRetry(getForTableA(RESTORE_NAMESPACE, i));
      if (a.isPresent()) {
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
