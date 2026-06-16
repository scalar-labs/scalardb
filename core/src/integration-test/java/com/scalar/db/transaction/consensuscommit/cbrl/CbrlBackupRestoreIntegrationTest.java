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
import com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

/**
 * CBRL backup/restore integration test (spike, §6.1 of the PoC plan), modeled on the cluster
 * replication {@code E2ETest}.
 *
 * <p>Flow:
 *
 * <ol>
 *   <li>start a concurrent upsert/delete workload over {@code cbrl_src} (each commit logs its write
 *       set into the Coordinator's {@code tx_write_set});
 *   <li>back up the user DB mid-flight — copy {@code cbrl_src} → {@code cbrl_restore} while writes
 *       continue, so the copy is torn;
 *   <li>stop the workload and run lazy recovery so PREPARED records resolve;
 *   <li>back up the Coordinator (id, state, tx_write_set) — done last, after quiesce, so it covers
 *       every committed transaction;
 *   <li>restore: replay the committed write sets onto {@code cbrl_restore} (the §5 replay core);
 *   <li>compare {@code cbrl_restore} to {@code cbrl_src} per key on both tables.
 * </ol>
 *
 * <p>The Coordinator is backed up last and after quiesce (C3: coordinator strictly last). That
 * makes the committed write sets cover the full final state, so the restored tables must equal the
 * final {@code cbrl_src} — a deterministic comparison, like {@code E2ETest}'s drain-then-compare.
 *
 * <p>The acceptance test is RED until the replay core lands (PoC milestone 3): {@link
 * #restore(List)} is a no-op, so {@code cbrl_restore} keeps only the torn copy and is missing every
 * mutation committed after the backup.
 *
 * <p>Requires PostgreSQL on localhost:5432 (the project's standard JDBC integration-test backend).
 */
@TestInstance(Lifecycle.PER_CLASS)
public class CbrlBackupRestoreIntegrationTest {

  private static final String TEST_NAME = "cbrl";
  private static final String SRC_NAMESPACE = "cbrl_src";
  private static final String RESTORE_NAMESPACE = "cbrl_restore";
  // A dedicated coordinator namespace, isolated from any leftover default "coordinator" schema in a
  // shared dev database.
  private static final String COORDINATOR_NAMESPACE = "cbrl_coordinator";

  // Table without a clustering key, covering a spread of data types.
  private static final String TABLE_A = "table_a";
  private static final String A_PK = "pk";
  private static final String A_INT = "col_int";
  private static final String A_BIGINT = "col_bigint";
  private static final String A_BOOL = "col_bool";
  private static final String A_DOUBLE = "col_double";
  private static final String A_TEXT = "col_text";
  private static final String A_BLOB = "col_blob";
  private static final TableMetadata TABLE_A_METADATA =
      TableMetadata.newBuilder()
          .addColumn(A_PK, DataType.INT)
          .addColumn(A_INT, DataType.INT)
          .addColumn(A_BIGINT, DataType.BIGINT)
          .addColumn(A_BOOL, DataType.BOOLEAN)
          .addColumn(A_DOUBLE, DataType.DOUBLE)
          .addColumn(A_TEXT, DataType.TEXT)
          .addColumn(A_BLOB, DataType.BLOB)
          .addPartitionKey(A_PK)
          .build();

  // Table with a clustering key.
  private static final String TABLE_B = "table_b";
  private static final String B_PK = "pk";
  private static final String B_CK = "ck";
  private static final String B_TEXT = "col_text";
  private static final String B_INT = "col_int";
  private static final TableMetadata TABLE_B_METADATA =
      TableMetadata.newBuilder()
          .addColumn(B_PK, DataType.INT)
          .addColumn(B_CK, DataType.INT)
          .addColumn(B_TEXT, DataType.TEXT)
          .addColumn(B_INT, DataType.INT)
          .addPartitionKey(B_PK)
          .addClusteringKey(B_CK)
          .build();

  private static final int RECORD_COUNT = 40;
  private static final int WORKLOAD_THREADS = 4;
  private static final int DELETE_PERCENTAGE = 30;
  private static final Duration WORKLOAD_WARMUP = Duration.ofMillis(400);
  private static final Duration WORKLOAD_AFTER_BACKUP = Duration.ofMillis(400);

  private DistributedTransactionManager manager;
  private DistributedTransactionAdmin admin;
  private DistributedStorage storage;
  private ExecutorService workerExecutor;

  /**
   * One captured Coordinator-state row: the unit of the coordinator backup. {@code txId} and {@code
   * writeSet} are captured per the §6.1 spec ("id, state, tx_write_set") but only consumed once the
   * replay core lands (milestone 3); suppress the until-then "unused" warning rather than capture a
   * lossy backup.
   */
  @SuppressWarnings("unused")
  private static final class CoordinatorBackupRow {
    private final String txId;
    private final int state;
    @Nullable private final WriteSet writeSet;

    private CoordinatorBackupRow(String txId, int state, @Nullable WriteSet writeSet) {
      this.txId = txId;
      this.state = state;
      this.writeSet = writeSet;
    }
  }

  private interface TxBody {
    void run(DistributedTransaction tx) throws Exception;
  }

  private Properties properties() {
    Properties properties = JdbcEnv.getProperties(TEST_NAME);
    properties.setProperty(
        DatabaseConfig.TRANSACTION_MANAGER, ConsensusCommitConfig.TRANSACTION_MANAGER_NAME);
    properties.setProperty(ConsensusCommitConfig.COORDINATOR_NAMESPACE, COORDINATOR_NAMESPACE);
    // The CBRL spike toggle: log full (partial) column values in tx_write_set, not just keys.
    properties.setProperty(ConsensusCommitConfig.TX_WRITE_SET_INCLUDE_COLUMNS_ENABLED, "true");
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
   * Positive control for the backup/compare harness. With the source quiesced, a full storage-level
   * copy into the restore namespace must compare equal. This proves the copy and the per-key
   * comparison faithfully report equality — so that {@link
   * #restore_replayingCommittedWriteSets_makesRestoredTablesMatchSource()} failing (and, once
   * replay exists, passing) reflects the restore logic, not a broken harness.
   */
  @Test
  void backupHarness_fullCopyOfQuiescedSource_comparesEqual() throws Exception {
    for (int i = 0; i < RECORD_COUNT; i++) {
      int key = i;
      withRetry(
          tx -> {
            tx.put(upsertForTableA(key));
            tx.put(upsertForTableB(key));
          });
    }

    copyTable(TABLE_A);
    copyTable(TABLE_B);

    assertRestoreEqualsSource();
  }

  /** The CBRL acceptance test (§6.1). See the class javadoc for the flow and why it is RED. */
  @Test
  void restore_replayingCommittedWriteSets_makesRestoredTablesMatchSource() throws Exception {
    // 1. start the concurrent workload over cbrl_src.
    AtomicBoolean stop = new AtomicBoolean(false);
    List<Future<?>> workload = startWorkload(stop);
    Uninterruptibles.sleepUninterruptibly(WORKLOAD_WARMUP);

    // 2. back up the user DB mid-flight — intentionally torn.
    copyTable(TABLE_A);
    copyTable(TABLE_B);

    // Keep mutating after the backup so the torn copy falls genuinely behind the source.
    Uninterruptibles.sleepUninterruptibly(WORKLOAD_AFTER_BACKUP);

    // 3. stop the workload; quiesce; run lazy recovery so PREPARED records resolve.
    stop.set(true);
    for (Future<?> future : workload) {
      future.get();
    }
    runLazyRecovery();

    // 4. back up the Coordinator last (after quiesce), so it covers every committed transaction.
    List<CoordinatorBackupRow> coordinatorBackup = backUpCoordinator();

    // 5. restore: replay the committed write sets onto cbrl_restore. Stubbed until the §5 core.
    restore(coordinatorBackup);

    // 6. compare every key on both tables; cbrl_restore must equal the quiesced cbrl_src.
    assertRestoreEqualsSource();
  }

  /**
   * Restores by replaying the coordinator backup's committed write sets onto {@code cbrl_restore}.
   *
   * <p>TODO(cbrl milestone 3): for each COMMITTED row, explode its {@link WriteSet}'s EntryGroups
   * into RedoOps and replay them via the §5 core (RecordShuffler → RecordApplier,
   * RestoredRecordReader reading {@code cbrl_restore}). Until that core exists this is a no-op,
   * leaving the torn copy in place.
   */
  @SuppressWarnings("unused")
  private void restore(List<CoordinatorBackupRow> coordinatorBackup) {
    // No-op: the replay core does not exist yet.
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

  /** One transaction that upserts or (sometimes) deletes record {@code i} in both tables. */
  private void mutateOnce(int i) {
    withRetry(
        tx -> {
          if (tx.get(getForTableA(i, SRC_NAMESPACE)).isPresent() && rollDelete()) {
            tx.delete(deleteForTableA(i));
          } else {
            tx.put(upsertForTableA(i));
          }
          if (tx.get(getForTableB(i, SRC_NAMESPACE)).isPresent() && rollDelete()) {
            tx.delete(deleteForTableB(i));
          } else {
            tx.put(upsertForTableB(i));
          }
        });
  }

  private boolean rollDelete() {
    return ThreadLocalRandom.current().nextInt(100) < DELETE_PERCENTAGE;
  }

  /** Reads every key once, which triggers lazy recovery on any leftover PREPARED records. */
  private void runLazyRecovery() {
    for (int i = 0; i < RECORD_COUNT; i++) {
      getWithRetry(getForTableA(i, SRC_NAMESPACE));
      getWithRetry(getForTableB(i, SRC_NAMESPACE));
    }
  }

  /** Backs up the Coordinator table, capturing each row's id, state, and write set. */
  private List<CoordinatorBackupRow> backUpCoordinator() throws Exception {
    List<CoordinatorBackupRow> rows = new ArrayList<>();
    Scan scan =
        Scan.newBuilder().namespace(COORDINATOR_NAMESPACE).table(Coordinator.TABLE).all().build();
    try (Scanner scanner = storage.scan(scan)) {
      for (Result result : scanner.all()) {
        String txId = result.getText(Attribute.ID);
        int state = result.isNull(Attribute.STATE) ? 0 : result.getInt(Attribute.STATE);
        WriteSet writeSet =
            result.isNull(Attribute.WRITE_SET)
                ? null
                : WriteSet.parseFrom(result.getBlobAsBytes(Attribute.WRITE_SET));
        rows.add(new CoordinatorBackupRow(txId, state, writeSet));
      }
    }
    return rows;
  }

  /** Full storage-level copy of a table from the source namespace to the restore namespace. */
  private void copyTable(String table) throws Exception {
    Scan scan = Scan.newBuilder().namespace(SRC_NAMESPACE).table(table).all().build();
    List<Put> puts = new ArrayList<>();
    TableMetadata metadata = table.equals(TABLE_A) ? TABLE_A_METADATA : TABLE_B_METADATA;
    try (Scanner scanner = storage.scan(scan)) {
      for (Result result : scanner.all()) {
        PutBuilder.Buildable builder =
            Put.newBuilder()
                .namespace(RESTORE_NAMESPACE)
                .table(table)
                .partitionKey(result.getPartitionKey().orElseThrow(IllegalStateException::new));
        result.getClusteringKey().ifPresent(builder::clusteringKey);
        for (Column<?> column : result.getColumns().values()) {
          if (!metadata.getPartitionKeyNames().contains(column.getName())
              && !metadata.getClusteringKeyNames().contains(column.getName())) {
            builder.value(column);
          }
        }
        puts.add(builder.build());
      }
    }
    for (Put put : puts) {
      storage.put(put);
    }
  }

  /** Compares every key on both tables between cbrl_src and cbrl_restore (absent == absent). */
  private void assertRestoreEqualsSource() {
    for (int i = 0; i < RECORD_COUNT; i++) {
      assertThat(getWithRetry(getForTableA(i, RESTORE_NAMESPACE)))
          .as("table_a key %d", i)
          .isEqualTo(getWithRetry(getForTableA(i, SRC_NAMESPACE)));
      assertThat(getWithRetry(getForTableB(i, RESTORE_NAMESPACE)))
          .as("table_b key %d", i)
          .isEqualTo(getWithRetry(getForTableB(i, SRC_NAMESPACE)));
    }
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

  private Put upsertForTableA(int i) {
    byte[] blob = new byte[8];
    ThreadLocalRandom.current().nextBytes(blob);
    return Put.newBuilder()
        .namespace(SRC_NAMESPACE)
        .table(TABLE_A)
        .partitionKey(Key.ofInt(A_PK, i))
        .intValue(A_INT, ThreadLocalRandom.current().nextInt())
        .bigIntValue(A_BIGINT, ThreadLocalRandom.current().nextLong())
        .booleanValue(A_BOOL, ThreadLocalRandom.current().nextBoolean())
        .doubleValue(A_DOUBLE, ThreadLocalRandom.current().nextDouble())
        .textValue(A_TEXT, Long.toHexString(ThreadLocalRandom.current().nextLong()))
        .blobValue(A_BLOB, blob)
        .build();
  }

  private Put upsertForTableB(int i) {
    return Put.newBuilder()
        .namespace(SRC_NAMESPACE)
        .table(TABLE_B)
        .partitionKey(Key.ofInt(B_PK, i))
        .clusteringKey(Key.ofInt(B_CK, i))
        .textValue(B_TEXT, Long.toHexString(ThreadLocalRandom.current().nextLong()))
        .intValue(B_INT, ThreadLocalRandom.current().nextInt())
        .build();
  }

  private Delete deleteForTableA(int i) {
    return Delete.newBuilder()
        .namespace(SRC_NAMESPACE)
        .table(TABLE_A)
        .partitionKey(Key.ofInt(A_PK, i))
        .build();
  }

  private Delete deleteForTableB(int i) {
    return Delete.newBuilder()
        .namespace(SRC_NAMESPACE)
        .table(TABLE_B)
        .partitionKey(Key.ofInt(B_PK, i))
        .clusteringKey(Key.ofInt(B_CK, i))
        .build();
  }

  private Get getForTableA(int i, String namespace) {
    return Get.newBuilder()
        .namespace(namespace)
        .table(TABLE_A)
        .partitionKey(Key.ofInt(A_PK, i))
        .build();
  }

  private Get getForTableB(int i, String namespace) {
    return Get.newBuilder()
        .namespace(namespace)
        .table(TABLE_B)
        .partitionKey(Key.ofInt(B_PK, i))
        .clusteringKey(Key.ofInt(B_CK, i))
        .build();
  }
}
