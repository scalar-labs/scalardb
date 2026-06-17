package com.scalar.db.transaction.consensuscommit.cbrl;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteBuilder;
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
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
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
 * CBRL backup/restore integration test (spike, §6.1 of the PoC plan).
 *
 * <p>Non-pausing backup: the coordinator is backed up while the workload is still committing, and
 * closed over the {@code prev_tx_id} chain at backup time so it is self-contained (restore never
 * re-reads the source). Each transaction writes a shared {@code token} to {@code table_a[i]} and
 * {@code table_b[i]} (or deletes both), so a transactionally-consistent image has matching presence
 * and equal tokens per key.
 *
 * <p>Validated four ways: <b>consistency</b> (the two tables never disagree), <b>correctness</b>
 * (matches an independent reference computed over the same backup), <b>point-in-time</b> (tokens
 * committed after the backup are absent), and <b>idempotency</b> (re-restoring is identical).
 * {@link #consistencyCheckDetectsTornImage_negativeControl()} proves the consistency check has
 * teeth.
 *
 * <p>Scope of this harness: it logs the full history and reconstructs {@code cbrl_restore} from the
 * backup (full rebuild). Genuine <b>windowed repair of a torn physical copy</b> — anchoring each
 * record at its torn-copy version and replaying only the window's redo forward — is NOT done here:
 * reconciling a live torn copy to a consistent point in time is the open hard problem (the plan's
 * R-risk-1), and an attempt produced inconsistent images. The window gate (R2/R3) is covered by the
 * unit test {@code BackupWindowGateTest}, not wired into this flow.
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

  private static final int RECORD_COUNT = 40;
  private static final int WORKLOAD_THREADS = 4;
  private static final int DELETE_PERCENTAGE = 30;
  private static final Duration WORKLOAD_WARMUP = Duration.ofMillis(600);
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
    properties.setProperty(ConsensusCommitConfig.REDO_LOGGING_ENABLED, "true");
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
    admin.truncateCoordinatorTables();
    for (String namespace : new String[] {SRC_NAMESPACE, RESTORE_NAMESPACE}) {
      admin.truncateTable(namespace, TABLE_A);
      admin.truncateTable(namespace, TABLE_B);
    }
  }

  /**
   * CBRL acceptance test (§6.1): a coordinator backup taken under live load reconstructs a
   * transactionally-consistent, point-in-time image.
   */
  @Test
  void liveBackup_reconstructsConsistentPointInTimeImage() throws Exception {
    AtomicBoolean stop = new AtomicBoolean(false);
    List<Future<?>> workload = startWorkload(stop);
    Uninterruptibles.sleepUninterruptibly(WORKLOAD_WARMUP);

    // Back up the coordinator WHILE the workload commits (non-pausing); closed over the chain at
    // backup time, so restore is self-contained.
    long commitsBeforeBackup = committedCount.get();
    Map<String, CoordinatorBackupRow> coordinatorBackup = backUpCoordinator();

    // Prove the backup did not pause the workload: commits keep advancing across it. The workload
    // is still running here, so this resolves quickly; the bounded wait only guards a fast scan.
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

    restore(coordinatorBackup);

    assertThat(findConsistencyViolations()).as("consistency").isEmpty();
    assertThat(presentKeyCount()).as("restored some records").isGreaterThan(0);
    assertThat(findCorrectnessViolations(coordinatorBackup))
        .as("correctness vs reference")
        .isEmpty();
    assertThat(readRestoredTokens().values())
        .as("point-in-time: post-backup tokens excluded")
        .doesNotContainAnyElementsOf(postBackupTokens);

    Map<Integer, Long> image = readRestoredTokens();
    restore(coordinatorBackup);
    assertThat(readRestoredTokens()).as("idempotent restore").isEqualTo(image);
  }

  /** Negative control: proves the consistency check detects a torn image. */
  @Test
  void consistencyCheckDetectsTornImage_negativeControl() {
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

  private List<Future<?>> startWorkload(AtomicBoolean stop) {
    List<Future<?>> futures = new ArrayList<>(WORKLOAD_THREADS);
    for (int t = 0; t < WORKLOAD_THREADS; t++) {
      futures.add(
          workerExecutor.submit(
              () -> {
                while (!stop.get()) {
                  mutateOnce(ThreadLocalRandom.current().nextInt(RECORD_COUNT));
                }
              }));
    }
    return futures;
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
   * Restores by replaying the backup's committed write sets onto a freshly-truncated {@code
   * cbrl_restore} via the §5 replay core, written back transactionally so restored records carry
   * proper ConsensusCommit metadata.
   */
  private void restore(Map<String, CoordinatorBackupRow> coordinatorBackup) throws Exception {
    List<RedoOp> redoOps = new ArrayList<>();
    for (CoordinatorBackupRow row : coordinatorBackup.values()) {
      for (EntryGroup group : row.writeSet.getEntryGroupsList()) {
        if (!group.getChildId().isEmpty()) {
          throw new UnsupportedOperationException(
              "Group-commit child ids are not handled in this PoC restore");
        }
        for (Entry entry : group.getEntriesList()) {
          redoOps.add(new RedoOp(row.txId, row.createdAtMillis, entry));
        }
      }
    }

    List<List<RedoOp>> buckets = new RecordShuffler().shuffle(redoOps, REPLAY_BUCKETS);
    Map<RecordKey, RecordState> finalStates =
        new RecordApplier(key -> RecordState.absent()).apply(buckets, REPLAY_WORKERS);

    admin.truncateTable(RESTORE_NAMESPACE, TABLE_A);
    admin.truncateTable(RESTORE_NAMESPACE, TABLE_B);
    withRetry(
        tx -> {
          for (Map.Entry<RecordKey, RecordState> entry : finalStates.entrySet()) {
            applyToRestore(tx, entry.getKey(), entry.getValue());
          }
        });
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
   * Applies one replayed record state to the (truncated) {@code cbrl_restore} within {@code tx}.
   */
  private void applyToRestore(DistributedTransaction tx, RecordKey key, RecordState state)
      throws Exception {
    Key partitionKey = toIoKey(key.partitionKey());
    Key clusteringKey = key.clusteringKey() == null ? null : toIoKey(key.clusteringKey());
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

  private List<String> findCorrectnessViolations(Map<String, CoordinatorBackupRow> backup) {
    Map<Integer, Long> expected = expectedTableATokens(backup);
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

  private Map<Integer, Long> expectedTableATokens(Map<String, CoordinatorBackupRow> backup) {
    Map<Integer, Long> latestCreatedAt = new HashMap<>();
    Map<Integer, Long> token = new HashMap<>();
    for (CoordinatorBackupRow row : backup.values()) {
      for (EntryGroup group : row.writeSet.getEntryGroupsList()) {
        for (Entry entry : group.getEntriesList()) {
          if (!entry.getTableName().equals(TABLE_A)) {
            continue;
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
