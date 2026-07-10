package com.scalar.db.transaction.consensuscommit.cbrl;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.service.TransactionFactory;
import com.scalar.db.storage.jdbc.JdbcEnv;
import com.scalar.db.transaction.consensuscommit.Attribute;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import com.scalar.db.transaction.consensuscommit.proto.v1.EntryGroup;
import com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

/**
 * Integration test for the CBRL backup-mode feature: {@code enableRedoLogging(label)} writes the
 * single {@code backup} coordinator row, a subsequent {@code begin()} captures that label so its
 * commit logs full redo tagged with it, and {@code disableRedoLogging()} deletes the {@code backup}
 * row (recording a {@code BACKED_UP} row in {@code backup_histories}) so later commits stop logging
 * redo.
 *
 * <p>The staleness bound is set to 0 so {@code begin()} always forces a synchronous read of the
 * backup table, making label capture deterministic without waiting for the periodic daemon poll.
 *
 * <p>Requires PostgreSQL on localhost:5432 (override with {@code -Dscalardb.jdbc.url}).
 */
@TestInstance(Lifecycle.PER_CLASS)
public class CbrlBackupModeIntegrationTest {

  private static final String TEST_NAME = "cbrl_backup_mode";
  private static final String USER_NAMESPACE = "cbrl_backup_mode_data";
  private static final String COORDINATOR_NAMESPACE = "cbrl_backup_mode_coordinator";
  private static final String BACKUP_LABEL = "2026-07-03T12:34:56";

  private static final String TABLE = "table_a";
  private static final String PK = "pk";
  private static final String COL = "col";
  private static final TableMetadata TABLE_METADATA =
      TableMetadata.newBuilder()
          .addColumn(PK, DataType.INT)
          .addColumn(COL, DataType.BIGINT)
          .addPartitionKey(PK)
          .build();

  private DistributedTransactionManager manager;
  private DistributedTransactionAdmin admin;
  private DistributedStorage storage;

  private Properties properties() {
    Properties properties = JdbcEnv.getProperties(TEST_NAME);
    properties.setProperty(
        DatabaseConfig.TRANSACTION_MANAGER, ConsensusCommitConfig.TRANSACTION_MANAGER_NAME);
    properties.setProperty(ConsensusCommitConfig.COORDINATOR_NAMESPACE, COORDINATOR_NAMESPACE);
    // begin() always forces a synchronous scan, so a freshly written flag is captured immediately.
    properties.setProperty(ConsensusCommitConfig.BACKUP_STALENESS_BOUND_MILLIS, "0");
    return properties;
  }

  @BeforeAll
  void beforeAll() throws Exception {
    Properties properties = properties();
    TransactionFactory transactionFactory = TransactionFactory.create(properties);
    manager = transactionFactory.getTransactionManager();
    admin = transactionFactory.getTransactionAdmin();
    storage = StorageFactory.create(properties).getStorage();

    admin.createCoordinatorTables(true);
    admin.createNamespace(USER_NAMESPACE, true);
    admin.createTable(USER_NAMESPACE, TABLE, TABLE_METADATA, true);
  }

  @AfterAll
  void afterAll() throws Exception {
    if (admin != null) {
      admin.dropTable(USER_NAMESPACE, TABLE, true);
      admin.dropNamespace(USER_NAMESPACE, true);
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
    admin.truncateTable(USER_NAMESPACE, TABLE);
  }

  @Test
  void enableRedoLogging_ShouldWriteTheBackupRow() throws Exception {
    // Arrange

    // Act
    manager.enableRedoLogging(BACKUP_LABEL);
    List<Result> backupRows = scanBackupTable();

    // Assert
    assertThat(backupRows).hasSize(1);
    Result row = backupRows.get(0);
    assertThat(row.getText(Attribute.BACKUP_ID)).isEqualTo(Coordinator.BACKUP_PARTITION_KEY_VALUE);
    assertThat(row.getText(Attribute.BACKUP_LABEL)).isEqualTo(BACKUP_LABEL);
    assertThat(row.getBigInt(Attribute.BACKUP_CREATED_AT)).isPositive();
    assertThat(row.getText(Attribute.BACKUP_UPDATED_BY)).isNotEmpty();
  }

  @Test
  void begin_WhenBackupModeEnabled_ShouldCaptureLabelAndLogFullRedo() throws Exception {
    // Arrange
    manager.enableRedoLogging(BACKUP_LABEL);

    // Act
    DistributedTransaction tx = manager.begin();
    tx.put(
        Put.newBuilder()
            .namespace(USER_NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofInt(PK, 1))
            .bigIntValue(COL, 42L)
            .build());
    tx.commit();
    WriteSet writeSet = readWriteSet(tx.getId());

    // Assert
    assertThat(writeSet.getEntryGroupsCount()).isEqualTo(1);
    EntryGroup group = writeSet.getEntryGroups(0);
    assertThat(group.getBackupLabel())
        .as("the committed redo is tagged with the captured backup label")
        .isEqualTo(BACKUP_LABEL);
    assertThat(group.getEntriesCount()).isEqualTo(1);
    assertThat(group.getEntries(0).getColumnsCount())
        .as("full redo records the user column value")
        .isEqualTo(1);
    assertThat(group.getEntries(0).getColumns(0).getName()).isEqualTo(COL);
  }

  @Test
  void disableRedoLogging_ShouldDeleteBackupRowRecordHistoryAndStopLoggingRedo() throws Exception {
    // Arrange
    manager.enableRedoLogging(BACKUP_LABEL);
    manager.disableRedoLogging();

    // Act
    List<Result> backupRows = scanBackupTable();
    Optional<Result> history = getBackupHistory(BACKUP_LABEL);
    DistributedTransaction tx = manager.begin();
    tx.put(
        Put.newBuilder()
            .namespace(USER_NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofInt(PK, 2))
            .bigIntValue(COL, 7L)
            .build());
    tx.commit();
    WriteSet writeSet = readWriteSet(tx.getId());

    // Assert
    assertThat(backupRows).as("the backup row is deleted on close").isEmpty();
    assertThat(history).isPresent();
    assertThat(history.get().getText(Attribute.BACKUP_LABEL)).isEqualTo(BACKUP_LABEL);
    assertThat(history.get().getText(Attribute.BACKUP_STATE)).isEqualTo("BACKED_UP");
    assertThat(history.get().getBigInt(Attribute.BACKUP_CLOSED_AT)).isPositive();
    assertThat(writeSet.getEntryGroupsCount()).isEqualTo(1);
    EntryGroup group = writeSet.getEntryGroups(0);
    assertThat(group.getBackupLabel())
        .as("with the window closed, commits carry no backup label")
        .isEmpty();
    assertThat(group.getEntries(0).getColumnsCount())
        .as("with the window closed, redo is keys-only (no column values)")
        .isZero();
  }

  @Test
  void finishTransaction_WhileBackupWindowOpen_ShouldSkipDeletingCoordinatorState()
      throws Exception {
    // Arrange
    manager.enableRedoLogging(BACKUP_LABEL);
    DistributedTransaction tx = manager.begin();
    tx.put(
        Put.newBuilder()
            .namespace(USER_NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofInt(PK, 3))
            .bigIntValue(COL, 9L)
            .build());
    tx.commit();
    String txId = tx.getId();

    // Act
    boolean finished = manager.finishTransaction(txId);
    Optional<Result> state = getCoordinatorState(txId);

    // Assert
    assertThat(finished).as("finishTransaction does not fail during a backup window").isTrue();
    assertThat(state).as("the coordinator state row is not deleted").isPresent();
    assertThat(state.get().isNull(Attribute.WRITE_SET))
        .as("its tx_write_set redo survives")
        .isFalse();
  }

  @Test
  void finishTransaction_AfterBackupWindowClosed_ShouldDeleteCoordinatorState() throws Exception {
    // Arrange
    manager.enableRedoLogging(BACKUP_LABEL);
    DistributedTransaction tx = manager.begin();
    tx.put(
        Put.newBuilder()
            .namespace(USER_NAMESPACE)
            .table(TABLE)
            .partitionKey(Key.ofInt(PK, 4))
            .bigIntValue(COL, 11L)
            .build());
    tx.commit();
    String txId = tx.getId();
    manager.disableRedoLogging();

    // Act
    boolean finished = manager.finishTransaction(txId);
    Optional<Result> state = getCoordinatorState(txId);

    // Assert
    assertThat(finished).as("cleanup proceeds once the window is closed").isTrue();
    assertThat(state).as("the coordinator state row is deleted").isNotPresent();
  }

  private List<Result> scanBackupTable() throws Exception {
    Scan scan =
        Scan.newBuilder()
            .namespace(COORDINATOR_NAMESPACE)
            .table(Coordinator.BACKUP_TABLE)
            .partitionKey(Key.ofText(Attribute.BACKUP_ID, Coordinator.BACKUP_PARTITION_KEY_VALUE))
            .build();
    try (Scanner scanner = storage.scan(scan)) {
      return scanner.all();
    }
  }

  private Optional<Result> getBackupHistory(String label) throws Exception {
    return storage.get(
        Get.newBuilder()
            .namespace(COORDINATOR_NAMESPACE)
            .table(Coordinator.BACKUP_HISTORIES_TABLE)
            .partitionKey(Key.ofText(Attribute.BACKUP_LABEL, label))
            .build());
  }

  private WriteSet readWriteSet(String txId) throws Exception {
    Optional<Result> result = getCoordinatorState(txId);
    assertThat(result).isPresent();
    return WriteSet.parseFrom(result.get().getBlobAsBytes(Attribute.WRITE_SET));
  }

  private Optional<Result> getCoordinatorState(String txId) throws Exception {
    return storage.get(
        Get.newBuilder()
            .namespace(COORDINATOR_NAMESPACE)
            .table(Coordinator.TABLE)
            .partitionKey(Key.ofText(Attribute.ID, txId))
            .build());
  }
}
