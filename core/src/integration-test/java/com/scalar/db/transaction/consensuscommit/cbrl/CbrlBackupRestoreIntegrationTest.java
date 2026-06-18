package com.scalar.db.transaction.consensuscommit.cbrl;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.util.concurrent.Uninterruptibles;
import com.google.protobuf.ByteString;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteBuilder;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.GetBuilder;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.service.TransactionFactory;
import com.scalar.db.storage.jdbc.JdbcEnv;
import com.scalar.db.transaction.consensuscommit.Attribute;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import com.scalar.db.transaction.consensuscommit.proto.v1.Entry;
import com.scalar.db.transaction.consensuscommit.proto.v1.EntryGroup;
import com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
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
 * commits; the coordinator is backed up (self-contained, closed over the chain); records left
 * in-flight in the copy are recovered (<b>C4 — copy recovery</b>: resolving records the copy caught
 * mid-commit, in the PREPARED state, to a clean committed-or-absent state before replay anchors on
 * them); and the committed redo is replayed <b>forward from each record's copied version</b> onto
 * the copy via the §5 core — not a full rebuild. Each transaction writes a shared {@code token} to
 * {@code table_a[i]} and {@code table_b[i]} (or deletes both), so a consistent image has matching
 * presence and equal tokens per key.
 *
 * <p>Because pre-window state is never logged, the copy is the <b>only</b> source for it — it is
 * genuinely <b>load-bearing</b>, not merely an anchor. This is asserted directly: pre-window-only
 * keys (untouched in the window) have zero redo yet restore to their seeded value, and mid-chain
 * keys (seeded pre-window, updated once in-window) have redo with <b>no insert root</b> yet restore
 * correctly — proof the chain anchored on the copy. {@link #copyIsLoadBearing_negativeControl()}
 * restores the same backup <b>without</b> the copy and shows the pre-window data is then
 * unrecoverable.
 *
 * <p>Validated: consistency (the two tables never disagree), correctness (matches an independent
 * reference: the pre-window seed the test recorded, overlaid with the in-window redo),
 * point-in-time (post-backup tokens absent), idempotency, load-bearing copy, and that the backup
 * ran without pausing the workload. {@link
 * #consistencyCheckDetectsInconsistentImage_negativeControl()} proves the consistency check has
 * teeth.
 *
 * <p>Requires PostgreSQL on localhost:5432 (override with {@code -Dscalardb.jdbc.url}).
 */
@TestInstance(Lifecycle.PER_CLASS)
public class CbrlBackupRestoreIntegrationTest {

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
  // Key partitions (disjoint, in order) used to exercise and assert the windowed property:
  //   [0, PRE_WINDOW_ONLY)                       seeded pre-window, never touched in-window —
  //                                              zero redo, restored from the copy alone.
  //   [PRE_WINDOW_ONLY, MID_CHAIN_END)           seeded pre-window, updated exactly once in-window
  // —
  //                                              redo has no insert root, restored via copy
  //                                              anchor (mid-chain).
  //   [MID_CHAIN_END, RECORD_COUNT)              the random concurrent in-window workload.
  private static final int PRE_WINDOW_ONLY_KEYS = 4;
  private static final int MID_CHAIN_END = PRE_WINDOW_ONLY_KEYS + 4;
  private static final int WORKLOAD_THREADS = 4;
  private static final int DELETE_PERCENTAGE = 30;
  private static final Duration WORKLOAD_WARMUP = Duration.ofMillis(400);
  private static final Duration WORKLOAD_AFTER_COPY = Duration.ofMillis(400);
  private static final int REPLAY_BUCKETS = 8;
  private static final int REPLAY_WORKERS = 4;

  private DistributedTransactionManager manager;
  private DistributedTransactionAdmin admin;
  private DistributedStorage storage;
  private ExecutorService workerExecutor;
  private final AtomicLong tokenCounter = new AtomicLong();
  private final AtomicLong committedCount = new AtomicLong();

  private interface TxBody {
    void run(DistributedTransaction tx) throws Exception;
  }

  /** One captured coordinator-state row: the unit of the coordinator backup. */
  private static final class CoordinatorBackupRow {
    private final String txId;
    private final int state;
    private final long createdAtMillis;
    @Nullable private final WriteSet writeSet;

    private CoordinatorBackupRow(
        String txId, int state, long createdAtMillis, @Nullable WriteSet writeSet) {
      this.txId = txId;
      this.state = state;
      this.createdAtMillis = createdAtMillis;
      this.writeSet = writeSet;
    }
  }

  private Properties properties() {
    Properties properties = JdbcEnv.getProperties(TEST_NAME);
    properties.setProperty(
        DatabaseConfig.TRANSACTION_MANAGER, ConsensusCommitConfig.TRANSACTION_MANAGER_NAME);
    properties.setProperty(ConsensusCommitConfig.COORDINATOR_NAMESPACE, COORDINATOR_NAMESPACE);
    // Start with redo logging OFF; the test opens the backup window (enables it) explicitly so the
    // pre-window base is unlogged and the copy is load-bearing.
    properties.setProperty(ConsensusCommitConfig.REDO_LOGGING_ENABLED, "false");
    return properties;
  }

  @BeforeAll
  void beforeAll() throws Exception {
    Properties properties = properties();
    TransactionFactory transactionFactory = TransactionFactory.create(properties);
    manager = transactionFactory.getTransactionManager();
    admin = transactionFactory.getTransactionAdmin();
    storage = StorageFactory.create(properties).getStorage();
    workerExecutor = Executors.newFixedThreadPool(WORKLOAD_THREADS);

    admin.createCoordinatorTables(true);
    for (String namespace : new String[] {SRC_NAMESPACE, RESTORE_NAMESPACE}) {
      admin.createNamespace(namespace, true);
      admin.createTable(namespace, TABLE_A, TABLE_A_METADATA, true);
      admin.createTable(namespace, TABLE_B, TABLE_B_METADATA, true);
    }
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

  /** CBRL acceptance test (§6.1): non-pausing backup + windowed repair of a load-bearing copy. */
  @Test
  void liveBackup_windowedRepairReconstructsConsistentPointInTimeImage() throws Exception {
    // Seed a pre-window base with redo logging OFF: these commits carry no redo, so the copy
    // is the only source for this state.
    Map<Integer, Long> preWindowSeed = seedPreWindowBase();

    // Open the backup window: redo logging ON. Only in-window commits are logged from here.
    manager.enableRedoLogging();

    // In-window workload on the random partition, concurrent with the copy.
    AtomicBoolean stop = new AtomicBoolean(false);
    List<Future<?>> workload = startWorkload(stop);
    Uninterruptibles.sleepUninterruptibly(WORKLOAD_WARMUP);

    // Non-snapshot-consistent copy of the user tables, taken WHILE the workload commits — the base.
    copyUserTables();
    Uninterruptibles.sleepUninterruptibly(WORKLOAD_AFTER_COPY); // copy falls behind the source

    // Mid-chain keys: exactly one in-window update each, AFTER the copy, so the copy holds
    // their pre-window version and the redo (an UPDATE with no insert root) must anchor on it.
    Map<Integer, Long> midChainTokens = applyMidChainUpdates();

    // Back up the coordinator WHILE live; closed over the chain at backup time (self-contained).
    long commitsBeforeBackup = committedCount.get();
    Map<String, CoordinatorBackupRow> coordinatorBackup = backUpCoordinator();

    // Prove the backup did not pause the workload: commits keep advancing across it.
    long deadlineMillis = System.currentTimeMillis() + 5_000;
    while (committedCount.get() <= commitsBeforeBackup
        && System.currentTimeMillis() < deadlineMillis) {
      Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(5));
    }
    assertThat(committedCount.get())
        .as("non-pausing: workload kept committing across the backup")
        .isGreaterThan(commitsBeforeBackup);

    // Drain, then commit MORE with fresh tokens so the source diverges past the backup point.
    stop.set(true);
    for (Future<?> future : workload) {
      future.get();
    }
    Set<Long> postBackupTokens = runPostBackupUpdates();

    // Restore: copy recovery (C4) then replay the redo forward onto it (windowed repair).
    restore(coordinatorBackup);

    assertThat(findConsistencyViolations()).as("consistency").isEmpty();
    assertThat(presentKeyCount()).as("restored some records").isGreaterThan(0);
    assertThat(findCorrectnessViolations(coordinatorBackup, preWindowSeed))
        .as("correctness vs reference")
        .isEmpty();
    assertThat(readRestoredTokens().values())
        .as("point-in-time: post-backup tokens excluded")
        .doesNotContainAnyElementsOf(postBackupTokens);

    assertCopyLoadBearing(coordinatorBackup, preWindowSeed, midChainTokens);

    Map<Integer, Long> image = readRestoredTokens();
    restore(coordinatorBackup);
    assertThat(readRestoredTokens()).as("idempotent restore").isEqualTo(image);
  }

  /**
   * Asserts the copy is genuinely load-bearing, not just an anchor: pre-window-only keys carry no
   * redo yet are restored from the copy, and mid-chain keys have redo with no insert root yet are
   * restored — both impossible without the copy supplying the base.
   */
  private void assertCopyLoadBearing(
      Map<String, CoordinatorBackupRow> backup,
      Map<Integer, Long> preWindowSeed,
      Map<Integer, Long> midChainTokens) {
    Map<Integer, RedoStat> stats = redoStatsForTableA(backup);
    Map<Integer, Long> restored = readRestoredTokens();

    for (int i = 0; i < PRE_WINDOW_ONLY_KEYS; i++) {
      assertThat(stats.containsKey(i))
          .as("pre-window-only key %d has no redo in the backup", i)
          .isFalse();
      assertThat(restored.get(i))
          .as("pre-window-only key %d restored from the copy alone", i)
          .isEqualTo(preWindowSeed.get(i));
    }

    for (int i = PRE_WINDOW_ONLY_KEYS; i < MID_CHAIN_END; i++) {
      RedoStat stat = stats.get(i);
      assertThat(stat).as("mid-chain key %d has in-window redo", i).isNotNull();
      assertThat(stat.count).as("mid-chain key %d in-window redo op count", i).isPositive();
      assertThat(stat.hasInsertRoot)
          .as("mid-chain key %d redo has no insert root (its base is pre-window)", i)
          .isFalse();
      assertThat(restored.get(i))
          .as("mid-chain key %d restored via the copy anchor", i)
          .isEqualTo(midChainTokens.get(i));
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
    restore(backup);

    for (int i = 0; i < PRE_WINDOW_ONLY_KEYS; i++) {
      assertThat(getWithRetry(getForTableA(RESTORE_NAMESPACE, i)))
          .as("without the copy, pre-window-only key %d is unrecoverable", i)
          .isEmpty();
    }
    assertThat(seed).hasSize(PRE_WINDOW_ONLY_KEYS); // sanity: the keys were in fact seeded
  }

  private List<Future<?>> startWorkload(AtomicBoolean stop) {
    List<Future<?>> futures = new ArrayList<>(WORKLOAD_THREADS);
    int randomKeyCount = RECORD_COUNT - MID_CHAIN_END;
    for (int t = 0; t < WORKLOAD_THREADS; t++) {
      futures.add(
          workerExecutor.submit(
              () -> {
                while (!stop.get()) {
                  // Restricted to the random partition so the pre-window-only and mid-chain keys
                  // stay untouched in the window.
                  mutateOnce(MID_CHAIN_END + ThreadLocalRandom.current().nextInt(randomKeyCount));
                }
              }));
    }
    return futures;
  }

  /** Seeds all keys with a pre-window base (redo logging OFF). Returns key -> seeded token. */
  private Map<Integer, Long> seedPreWindowBase() {
    return seedPreWindowBase(RECORD_COUNT);
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

  /** One in-window UPDATE per mid-chain key (read-then-put). Returns key -> in-window token. */
  private Map<Integer, Long> applyMidChainUpdates() {
    Map<Integer, Long> tokens = new HashMap<>();
    for (int i = PRE_WINDOW_ONLY_KEYS; i < MID_CHAIN_END; i++) {
      long token = tokenCounter.incrementAndGet();
      int key = i;
      tokens.put(i, token);
      withRetry(
          tx -> {
            tx.get(getForTableA(SRC_NAMESPACE, key)); // read first so prev_tx_id links to the seed
            tx.get(getForTableB(SRC_NAMESPACE, key));
            tx.put(putForTableA(SRC_NAMESPACE, key, token));
            tx.put(putForTableB(SRC_NAMESPACE, key, token));
          });
    }
    return tokens;
  }

  /** One transaction that atomically writes a shared token to both tables, or deletes both. */
  private void mutateOnce(int i) {
    long token = tokenCounter.incrementAndGet();
    withRetry(
        tx -> {
          if (tx.get(getForTableA(SRC_NAMESPACE, i)).isPresent() && rollDelete()) {
            tx.delete(deleteForTableA(SRC_NAMESPACE, i));
            tx.delete(deleteForTableB(SRC_NAMESPACE, i));
          } else {
            tx.put(putForTableA(SRC_NAMESPACE, i, token));
            tx.put(putForTableB(SRC_NAMESPACE, i, token));
          }
        });
    committedCount.incrementAndGet();
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
    long createdAt =
        result.isNull(Attribute.CREATED_AT) ? 0 : result.getBigInt(Attribute.CREATED_AT);
    WriteSet writeSet =
        result.isNull(Attribute.WRITE_SET)
            ? null
            : WriteSet.parseFrom(result.getBlobAsBytes(Attribute.WRITE_SET));
    return new CoordinatorBackupRow(txId, state, createdAt, writeSet);
  }

  /**
   * Windowed repair: copy recovery (C4) — resolve records the copy caught in the PREPARED state,
   * via ScalarDB's own recovery — then replay the backup's committed redo onto the copy via the §5
   * core. Each key's cursor anchors at its copy version, so only the redo after that version is
   * applied.
   */
  private void restore(Map<String, CoordinatorBackupRow> coordinatorBackup) throws Exception {
    recoverPreparedRecords(); // C4 (copy recovery): resolve any PREPARED records in the copy.

    List<RedoOp> redoOps = new ArrayList<>();
    for (CoordinatorBackupRow row : coordinatorBackup.values()) {
      for (EntryGroup group : row.writeSet.getEntryGroupsList()) {
        if (!group.getChildId().isEmpty()) {
          throw new UnsupportedOperationException(
              "Group-commit child ids are not handled in this PoC restore");
        }
        for (Entry entry : group.getEntriesList()) {
          if (!entry.hasTxVersion()) {
            // Key-only write set from outside the backup window (logging was off): not redo. The
            // copy carries that state instead.
            continue;
          }
          redoOps.add(new RedoOp(row.txId, row.createdAtMillis, entry));
        }
      }
    }

    List<List<RedoOp>> buckets = new RecordShuffler().shuffle(redoOps, REPLAY_BUCKETS);
    Map<RecordKey, RecordState> finalStates =
        new RecordApplier(this::readCopyState).apply(buckets, REPLAY_WORKERS);

    withRetry(
        tx -> {
          for (Map.Entry<RecordKey, RecordState> entry : finalStates.entrySet()) {
            applyToRestore(tx, entry.getKey(), entry.getValue());
          }
        });
  }

  /**
   * C4 (copy recovery): read every key transactionally so ScalarDB resolves any records the copy
   * caught in the PREPARED state.
   */
  private void recoverPreparedRecords() {
    for (int i = 0; i < RECORD_COUNT; i++) {
      getWithRetry(getForTableA(RESTORE_NAMESPACE, i));
      getWithRetry(getForTableB(RESTORE_NAMESPACE, i));
    }
  }

  /** Reads the (recovered) copy record as the repair base: its committed tx id + user columns. */
  private RecordState readCopyState(RecordKey key) {
    try {
      Optional<Result> result = storage.get(rawGet(key));
      if (!result.isPresent() || result.get().isNull(Attribute.ID)) {
        return RecordState.absent();
      }
      Result record = result.get();
      String currentTxId = record.getText(Attribute.ID);
      String[] userColumns = key.table().equals(TABLE_A) ? A_USER_COLUMNS : B_USER_COLUMNS;
      Map<String, com.scalar.db.transaction.consensuscommit.proto.v1.Column> columns =
          new LinkedHashMap<>();
      for (String name : userColumns) {
        Column<?> column = record.getColumns().get(name);
        if (column != null) {
          columns.put(name, ioColumnToProto(column));
        }
      }
      return RecordState.of(currentTxId, false, columns, Collections.emptySet());
    } catch (Exception e) {
      throw new RuntimeException("Failed to read copy base for " + key, e);
    }
  }

  private Get rawGet(RecordKey key) {
    GetBuilder.BuildableGet builder =
        Get.newBuilder()
            .namespace(RESTORE_NAMESPACE)
            .table(key.table())
            .partitionKey(toIoKey(key.partitionKey()));
    if (key.clusteringKey() != null) {
      builder.clusteringKey(toIoKey(key.clusteringKey()));
    }
    return builder.build();
  }

  private static com.scalar.db.transaction.consensuscommit.proto.v1.Column ioColumnToProto(
      Column<?> column) {
    String name = column.getName();
    com.scalar.db.transaction.consensuscommit.proto.v1.Column.Builder builder =
        com.scalar.db.transaction.consensuscommit.proto.v1.Column.newBuilder().setName(name);
    if (column instanceof IntColumn) {
      com.scalar.db.transaction.consensuscommit.proto.v1.Column.IntValue.Builder value =
          com.scalar.db.transaction.consensuscommit.proto.v1.Column.IntValue.newBuilder();
      if (!column.hasNullValue()) {
        value.setValue(((IntColumn) column).getIntValue());
      }
      return builder.setIntValue(value).build();
    }
    if (column instanceof BigIntColumn) {
      com.scalar.db.transaction.consensuscommit.proto.v1.Column.BigIntValue.Builder value =
          com.scalar.db.transaction.consensuscommit.proto.v1.Column.BigIntValue.newBuilder();
      if (!column.hasNullValue()) {
        value.setValue(((BigIntColumn) column).getBigIntValue());
      }
      return builder.setBigintValue(value).build();
    }
    if (column instanceof BooleanColumn) {
      com.scalar.db.transaction.consensuscommit.proto.v1.Column.BooleanValue.Builder value =
          com.scalar.db.transaction.consensuscommit.proto.v1.Column.BooleanValue.newBuilder();
      if (!column.hasNullValue()) {
        value.setValue(((BooleanColumn) column).getBooleanValue());
      }
      return builder.setBooleanValue(value).build();
    }
    if (column instanceof TextColumn) {
      com.scalar.db.transaction.consensuscommit.proto.v1.Column.TextValue.Builder value =
          com.scalar.db.transaction.consensuscommit.proto.v1.Column.TextValue.newBuilder();
      if (!column.hasNullValue()) {
        value.setValue(((TextColumn) column).getTextValue());
      }
      return builder.setTextValue(value).build();
    }
    if (column instanceof BlobColumn) {
      com.scalar.db.transaction.consensuscommit.proto.v1.Column.BlobValue.Builder value =
          com.scalar.db.transaction.consensuscommit.proto.v1.Column.BlobValue.newBuilder();
      if (!column.hasNullValue()) {
        value.setValue(ByteString.copyFrom(((BlobColumn) column).getBlobValueAsBytes()));
      }
      return builder.setBlobValue(value).build();
    }
    throw new IllegalStateException("Unsupported column type for " + name + ": " + column);
  }

  private void closeOverChain(Map<String, CoordinatorBackupRow> committedById) throws Exception {
    Deque<String> pending = new ArrayDeque<>();
    for (CoordinatorBackupRow row : committedById.values()) {
      collectPrevTxIds(row, committedById, pending);
    }
    while (!pending.isEmpty()) {
      String prevTxId = pending.poll();
      if (committedById.containsKey(prevTxId)) {
        continue;
      }
      CoordinatorBackupRow row = fetchCoordinatorRow(prevTxId);
      if (row == null || row.state != TransactionState.COMMITTED.get() || row.writeSet == null) {
        continue;
      }
      committedById.put(prevTxId, row);
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

  /**
   * Applies one replayed record state to {@code cbrl_restore} within {@code tx} (read-then-write).
   */
  private void applyToRestore(DistributedTransaction tx, RecordKey key, RecordState state)
      throws Exception {
    Key partitionKey = toIoKey(key.partitionKey());
    Key clusteringKey = key.clusteringKey() == null ? null : toIoKey(key.clusteringKey());

    tx.get(rawGet(key)); // read first so the put/delete is an update, not insert-mode

    if (state.present()) {
      PutBuilder.Buildable put =
          Put.newBuilder()
              .namespace(RESTORE_NAMESPACE)
              .table(key.table())
              .partitionKey(partitionKey);
      if (clusteringKey != null) {
        put.clusteringKey(clusteringKey);
      }
      for (com.scalar.db.transaction.consensuscommit.proto.v1.Column column :
          state.columns().values()) {
        put.value(toIoColumn(column));
      }
      tx.put(put.build());
    } else {
      DeleteBuilder.Buildable delete =
          Delete.newBuilder()
              .namespace(RESTORE_NAMESPACE)
              .table(key.table())
              .partitionKey(partitionKey);
      if (clusteringKey != null) {
        delete.clusteringKey(clusteringKey);
      }
      tx.delete(delete.build());
    }
  }

  private static Key toIoKey(com.scalar.db.transaction.consensuscommit.proto.v1.Key protoKey) {
    Key.Builder builder = Key.newBuilder();
    for (com.scalar.db.transaction.consensuscommit.proto.v1.Column column :
        protoKey.getColumnsList()) {
      builder.add(toIoColumn(column));
    }
    return builder.build();
  }

  private static Column<?> toIoColumn(
      com.scalar.db.transaction.consensuscommit.proto.v1.Column column) {
    String name = column.getName();
    if (column.hasIntValue()) {
      return column.getIntValue().hasValue()
          ? IntColumn.of(name, column.getIntValue().getValue())
          : IntColumn.ofNull(name);
    }
    if (column.hasBigintValue()) {
      return column.getBigintValue().hasValue()
          ? BigIntColumn.of(name, column.getBigintValue().getValue())
          : BigIntColumn.ofNull(name);
    }
    if (column.hasBooleanValue()) {
      return column.getBooleanValue().hasValue()
          ? BooleanColumn.of(name, column.getBooleanValue().getValue())
          : BooleanColumn.ofNull(name);
    }
    if (column.hasTextValue()) {
      return column.getTextValue().hasValue()
          ? TextColumn.of(name, column.getTextValue().getValue())
          : TextColumn.ofNull(name);
    }
    if (column.hasBlobValue()) {
      return column.getBlobValue().hasValue()
          ? BlobColumn.of(name, column.getBlobValue().getValue().toByteArray())
          : BlobColumn.ofNull(name);
    }
    throw new IllegalStateException("Unsupported column value in redo entry: " + column);
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

  private Set<Long> runPostBackupUpdates() {
    Set<Long> tokens = new HashSet<>();
    for (int i = 0; i < RECORD_COUNT; i++) {
      long token = tokenCounter.incrementAndGet();
      int key = i;
      tokens.add(token);
      withRetry(
          tx -> {
            tx.get(getForTableA(SRC_NAMESPACE, key));
            tx.get(getForTableB(SRC_NAMESPACE, key));
            tx.put(putForTableA(SRC_NAMESPACE, key, token));
            tx.put(putForTableB(SRC_NAMESPACE, key, token));
          });
    }
    return tokens;
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

  private List<String> findCorrectnessViolations(
      Map<String, CoordinatorBackupRow> backup, Map<Integer, Long> preWindowSeed) {
    Map<Integer, Long> expected = expectedTableATokens(backup, preWindowSeed);
    List<String> violations = new ArrayList<>();
    for (int i = 0; i < RECORD_COUNT; i++) {
      Optional<Result> a = getWithRetry(getForTableA(RESTORE_NAMESPACE, i));
      Long expectedToken = expected.get(i);
      if (a.isPresent() != (expectedToken != null)) {
        violations.add(
            String.format(
                "key %d presence: restored=%s expected=%s",
                i, a.isPresent(), expectedToken != null));
      } else if (a.isPresent() && a.get().getBigInt(A_TOKEN) != expectedToken) {
        violations.add(
            String.format(
                "key %d token: restored=%d expected=%d",
                i, a.get().getBigInt(A_TOKEN), expectedToken));
      }
    }
    return violations;
  }

  /**
   * Independent reference for {@code table_a}: the pre-window seed the test recorded (the base the
   * copy must carry), overlaid with the in-window redo applied in commit order. Deliberately
   * derived from the test's own ground truth plus the backup's redo entries — not from the restore
   * path — so a bug in the applier cannot hide.
   */
  private Map<Integer, Long> expectedTableATokens(
      Map<String, CoordinatorBackupRow> backup, Map<Integer, Long> preWindowSeed) {
    Map<Integer, Long> latestCreatedAt = new HashMap<>();
    Map<Integer, Long> token = new HashMap<>(preWindowSeed); // base: the unlogged pre-window state
    for (CoordinatorBackupRow row : backup.values()) {
      for (EntryGroup group : row.writeSet.getEntryGroupsList()) {
        for (Entry entry : group.getEntriesList()) {
          if (!entry.hasTxVersion() || !entry.getTableName().equals(TABLE_A)) {
            continue; // Only in-window redo entries (logged) overlay the seed.
          }
          int pk = entry.getPartitionKey().getColumns(0).getIntValue().getValue();
          Long prev = latestCreatedAt.get(pk);
          if (prev != null && prev >= row.createdAtMillis) {
            continue;
          }
          latestCreatedAt.put(pk, row.createdAtMillis);
          if (entry.getEntryType() == Entry.EntryType.ENTRY_TYPE_WRITE) {
            token.put(pk, tokenOf(entry));
          } else {
            token.remove(pk);
          }
        }
      }
    }
    return token;
  }

  /** Per-key redo summary for {@code table_a} computed from the backup (in-window entries only). */
  private static final class RedoStat {
    private int count;
    private boolean hasInsertRoot;
  }

  private Map<Integer, RedoStat> redoStatsForTableA(Map<String, CoordinatorBackupRow> backup) {
    Map<Integer, RedoStat> stats = new HashMap<>();
    for (CoordinatorBackupRow row : backup.values()) {
      for (EntryGroup group : row.writeSet.getEntryGroupsList()) {
        for (Entry entry : group.getEntriesList()) {
          if (!entry.hasTxVersion() || !entry.getTableName().equals(TABLE_A)) {
            continue;
          }
          int pk = entry.getPartitionKey().getColumns(0).getIntValue().getValue();
          RedoStat stat = stats.computeIfAbsent(pk, k -> new RedoStat());
          stat.count++;
          if (entry.getEntryType() == Entry.EntryType.ENTRY_TYPE_WRITE && !entry.hasPrevTxId()) {
            stat.hasInsertRoot = true;
          }
        }
      }
    }
    return stats;
  }

  private static long tokenOf(Entry entry) {
    for (com.scalar.db.transaction.consensuscommit.proto.v1.Column column :
        entry.getColumnsList()) {
      if (column.getName().equals(A_TOKEN)) {
        return column.getBigintValue().getValue();
      }
    }
    throw new IllegalStateException("table_a WRITE entry has no token column: " + entry);
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
