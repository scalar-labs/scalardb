package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.api.ConditionBuilder.column;
import static com.scalar.db.api.ConditionBuilder.updateIf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Sets;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Selection;
import com.scalar.db.api.StorageInfo;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.api.TransactionManagerCrudOperable;
import com.scalar.db.api.TransactionState;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.common.DecoratedDistributedTransaction;
import com.scalar.db.common.StorageInfoProvider;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.CoordinatorGroupCommitter.CoordinatorGroupCommitKeyManipulator;
import com.scalar.db.util.groupcommit.GroupCommitKeyManipulator.Keys;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

@SuppressWarnings("UseCorrectAssertInTests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ConsensusCommitSpecificIntegrationTestBase {

  private static final String TEST_NAME = "cc";
  private static final String NAMESPACE_1 = "int_test_" + TEST_NAME + "1";
  private static final String NAMESPACE_2 = "int_test_" + TEST_NAME + "2";
  private static final String TABLE_1 = "test_table1";
  private static final String TABLE_2 = "test_table2";
  protected static final String ACCOUNT_ID = "account_id";
  protected static final String ACCOUNT_TYPE = "account_type";
  protected static final String BALANCE = "balance";
  protected static final String SOME_COLUMN = "some_column";
  private static final int INITIAL_BALANCE = 1000;
  private static final int NEW_BALANCE = 2000;
  private static final int NUM_ACCOUNTS = 4;
  private static final int NUM_TYPES = 4;
  private static final String ANY_ID_1 = "id1";
  private static final String ANY_ID_2 = "id2";

  private DistributedStorage originalStorage;
  private DistributedStorageAdmin admin;
  private DatabaseConfig databaseConfig;
  private ConsensusCommitConfig consensusCommitConfig;
  private ConsensusCommitAdmin consensusCommitAdmin;
  private String namespace1;
  private String namespace2;
  private ParallelExecutor parallelExecutor;

  private DistributedStorage storage;
  private Coordinator coordinator;
  private RecoveryHandler recovery;
  private RecoveryExecutor recoveryExecutor;
  private CommitHandler commit;
  @Nullable private CoordinatorGroupCommitter groupCommitter;

  @BeforeAll
  void beforeAll() throws Exception {
    initialize();
    Properties properties = getProperties(TEST_NAME);

    // Add testName as a coordinator namespace suffix
    ConsensusCommitTestUtils.addSuffixToCoordinatorNamespace(properties, TEST_NAME);

    StorageFactory factory = StorageFactory.create(properties);
    admin = factory.getStorageAdmin();
    databaseConfig = new DatabaseConfig(properties);
    consensusCommitConfig = new ConsensusCommitConfig(databaseConfig);
    consensusCommitAdmin = new ConsensusCommitAdmin(admin, consensusCommitConfig, false);
    namespace1 = getNamespace1();
    namespace2 = getNamespace2();
    createTables();
    originalStorage = factory.getStorage();
    parallelExecutor = new ParallelExecutor(consensusCommitConfig);
  }

  protected void initialize() throws Exception {}

  protected abstract Properties getProperties(String testName);

  protected String getNamespace1() {
    return NAMESPACE_1;
  }

  protected String getNamespace2() {
    return NAMESPACE_2;
  }

  protected TableMetadata getTableMetadata() {
    return TableMetadata.newBuilder()
        .addColumn(ACCOUNT_ID, DataType.INT)
        .addColumn(ACCOUNT_TYPE, DataType.INT)
        .addColumn(BALANCE, DataType.INT)
        .addColumn(SOME_COLUMN, DataType.TEXT)
        .addPartitionKey(ACCOUNT_ID)
        .addClusteringKey(ACCOUNT_TYPE)
        .addSecondaryIndex(BALANCE)
        .build();
  }

  private void createTables() throws ExecutionException {
    Map<String, String> options = getCreationOptions();
    consensusCommitAdmin.createCoordinatorTables(true, options);
    consensusCommitAdmin.createNamespace(namespace1, true, options);
    consensusCommitAdmin.createTable(namespace1, TABLE_1, getTableMetadata(), true, options);
    consensusCommitAdmin.createNamespace(namespace2, true, options);
    consensusCommitAdmin.createTable(namespace2, TABLE_2, getTableMetadata(), true, options);
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  @BeforeEach
  void setUp() throws Exception {
    truncateTables();
  }

  @AfterEach
  public void tearDown() {
    recoveryExecutor.close();
    if (groupCommitter != null) {
      groupCommitter.close();
    }
  }

  private void truncateTables() throws ExecutionException {
    consensusCommitAdmin.truncateTable(namespace1, TABLE_1);
    consensusCommitAdmin.truncateTable(namespace2, TABLE_2);
    consensusCommitAdmin.truncateCoordinatorTables();
  }

  @AfterAll
  void afterAll() throws Exception {
    dropTables();
    consensusCommitAdmin.close();
    originalStorage.close();
    parallelExecutor.close();
  }

  private void dropTables() throws ExecutionException {
    consensusCommitAdmin.dropTable(namespace1, TABLE_1);
    consensusCommitAdmin.dropNamespace(namespace1);
    consensusCommitAdmin.dropTable(namespace2, TABLE_2);
    consensusCommitAdmin.dropNamespace(namespace2);
    consensusCommitAdmin.dropCoordinatorTables();
  }

  @Test
  void begin_CorrectTransactionIdGiven_ShouldNotThrowAnyExceptions() {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SNAPSHOT);

    // Act Assert
    assertThatCode(
            () -> {
              DistributedTransaction transaction = manager.begin(ANY_ID_1);
              transaction.commit();
            })
        .doesNotThrowAnyException();
  }

  @Test
  void begin_EmptyTransactionIdGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> manager.begin("")).isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void get_GetGivenForCommittedRecord_ShouldReturnRecord(Isolation isolation, boolean readOnly)
      throws TransactionException, CoordinatorException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecord(manager, namespace1, TABLE_1);
    reset(coordinator);
    DistributedTransaction transaction = begin(manager, readOnly);
    Get get = prepareGet(0, 0, namespace1, TABLE_1);

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(((TransactionResult) ((FilteredResult) result.get()).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);

    // commit-state should not occur for read-only transactions
    verify(coordinator, never()).putState(any(Coordinator.State.class));
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void scan_ScanGivenForCommittedRecords_ShouldReturnRecords(Isolation isolation, boolean readOnly)
      throws TransactionException, CoordinatorException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecords(manager, namespace1, TABLE_1);
    reset(coordinator);
    DistributedTransaction transaction = begin(manager, readOnly);
    Scan scan = prepareScan(0, namespace1, TABLE_1);

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(4);
    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(results.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(
            ((TransactionResult) ((FilteredResult) results.get(0)).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);
    assertThat(results.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(results.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(
            ((TransactionResult) ((FilteredResult) results.get(1)).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);
    assertThat(results.get(2).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(2).getInt(ACCOUNT_TYPE)).isEqualTo(2);
    assertThat(results.get(2).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(
            ((TransactionResult) ((FilteredResult) results.get(2)).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);
    assertThat(results.get(3).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(3).getInt(ACCOUNT_TYPE)).isEqualTo(3);
    assertThat(results.get(3).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(
            ((TransactionResult) ((FilteredResult) results.get(3)).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);

    // commit-state should not occur for read-only transactions
    verify(coordinator, never()).putState(any(Coordinator.State.class));
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void getScanner_ScanGivenForCommittedRecords_ShouldReturnRecords(
      Isolation isolation, boolean readOnly) throws TransactionException, CoordinatorException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecords(manager, namespace1, TABLE_1);
    reset(coordinator);
    DistributedTransaction transaction = begin(manager, readOnly);
    Scan scan = prepareScan(0, namespace1, TABLE_1);

    // Act Assert
    TransactionCrudOperable.Scanner scanner = transaction.getScanner(scan);

    Optional<Result> result = scanner.one();
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(((TransactionResult) ((FilteredResult) result.get()).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);

    result = scanner.one();
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.get().getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(result.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(((TransactionResult) ((FilteredResult) result.get()).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);

    result = scanner.one();
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.get().getInt(ACCOUNT_TYPE)).isEqualTo(2);
    assertThat(result.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(((TransactionResult) ((FilteredResult) result.get()).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);

    result = scanner.one();
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.get().getInt(ACCOUNT_TYPE)).isEqualTo(3);
    assertThat(result.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(((TransactionResult) ((FilteredResult) result.get()).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);

    assertThat(scanner.one()).isEmpty();

    scanner.close();
    transaction.commit();

    // commit-state should not occur for read-only transactions
    verify(coordinator, never()).putState(any(Coordinator.State.class));
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void get_CalledTwice_ShouldBehaveCorrectly(Isolation isolation, boolean readOnly)
      throws TransactionException, ExecutionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecord(manager, namespace1, TABLE_1);
    DistributedTransaction transaction = begin(manager, readOnly);
    Get get = prepareGet(0, 0, namespace1, TABLE_1);

    // Act
    Optional<Result> result1 = transaction.get(get);
    Optional<Result> result2 = transaction.get(get);

    transaction.commit();

    // Assert
    if (isolation == Isolation.READ_COMMITTED) {
      // In READ_COMMITTED isolation, the storage operation should be called twice because every get
      // should be a new read
      verify(storage, times(2)).get(any(Get.class));
    } else if (isolation == Isolation.SNAPSHOT) {
      // In SNAPSHOT isolation, the storage operation should be called once because the second get
      // should be served from the snapshot
      verify(storage).get(any(Get.class));
    } else {
      assert isolation == Isolation.SERIALIZABLE;

      // In SERIALIZABLE isolation, the storage operation should be called twice because the second
      // get should be served from the snapshot, but the storage operation should additionally be
      // call the by the validation
      verify(storage, times(2)).get(any(Get.class));
    }

    assertThat(result1.isPresent()).isTrue();
    assertThat(result1.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result1.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result1.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    assertThat(result1).isEqualTo(result2);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void get_CalledTwiceAndAnotherTransactionCommitsInBetween_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly) throws TransactionException, ExecutionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecord(manager, namespace1, TABLE_1);
    DistributedTransaction transaction = begin(manager, readOnly);
    Get get = prepareGet(0, 0, namespace1, TABLE_1);

    // Act
    Optional<Result> result1 = transaction.get(get);

    // The record updated by another transaction
    int updatedBalance = 100;
    manager.update(
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, updatedBalance)
            .build());

    Optional<Result> result2 = transaction.get(get);

    // Assert
    if (isolation == Isolation.READ_COMMITTED) {
      // In READ_COMMITTED isolation

      transaction.commit();

      // The storage operation should be called three times: twice for the get by this transaction
      // and once for the update by another transaction
      verify(storage, times(3)).get(any(Get.class));

      // The second record should be the updated one
      assertThat(result1.isPresent()).isTrue();
      assertThat(result1.get().getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result1.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
      assertThat(result1.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

      assertThat(result2.isPresent()).isTrue();
      assertThat(result2.get().getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result2.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
      assertThat(result2.get().getInt(BALANCE)).isEqualTo(updatedBalance);
    } else if (isolation == Isolation.SNAPSHOT) {
      // In SNAPSHOT isolation

      transaction.commit();

      // The storage operation should be called twice: once for the get by this transaction and
      // once for the update by another transaction
      verify(storage, times(2)).get(any(Get.class));

      // The first record should be the same as the second because the second
      // should be returned from the snapshot
      assertThat(result1.isPresent()).isTrue();
      assertThat(result1.get().getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result1.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
      assertThat(result1.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

      assertThat(result1).isEqualTo(result2);
    } else {
      assert isolation == Isolation.SERIALIZABLE;

      // In SERIALIZABLE isolation, an anti-dependency should be detected
      assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
    }
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void scan_CalledTwice_ShouldBehaveCorrectly(Isolation isolation, boolean readOnly)
      throws TransactionException, ExecutionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecords(manager, namespace1, TABLE_1);
    DistributedTransaction transaction = begin(manager, readOnly);
    Scan scan = prepareScan(0, namespace1, TABLE_1);

    // Act
    List<Result> result1 = transaction.scan(scan);
    List<Result> result2 = transaction.scan(scan);
    transaction.commit();

    // Assert
    if (isolation == Isolation.READ_COMMITTED) {
      // In READ_COMMITTED isolation, the storage operation should be called twice because every
      // scan should be a new read
      verify(storage, times(2)).scan(any(Scan.class));
    } else if (isolation == Isolation.SNAPSHOT) {
      // In SNAPSHOT isolation, the storage operation should be called once because the second scan
      // should be served from the snapshot
      verify(storage).scan(any(Scan.class));
    } else {
      assert isolation == Isolation.SERIALIZABLE;

      // In SERIALIZABLE isolation, the storage operation should be called twice because the second
      // scan should be served from the snapshot, but the storage operation should additionally be
      // call the by the validation
      verify(storage, times(2)).scan(any(Scan.class));
    }

    assertThat(result1).hasSize(4);
    assertThat(result1.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result1.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result1.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(
            ((TransactionResult) ((FilteredResult) result1.get(0)).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);
    assertThat(result1.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result1.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(result1.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(
            ((TransactionResult) ((FilteredResult) result1.get(1)).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);
    assertThat(result1.get(2).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result1.get(2).getInt(ACCOUNT_TYPE)).isEqualTo(2);
    assertThat(result1.get(2).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(
            ((TransactionResult) ((FilteredResult) result1.get(2)).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);
    assertThat(result1.get(3).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result1.get(3).getInt(ACCOUNT_TYPE)).isEqualTo(3);
    assertThat(result1.get(3).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(
            ((TransactionResult) ((FilteredResult) result1.get(3)).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);

    assertThat(result1).isEqualTo(result2);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void scan_CalledTwiceAndAnotherTransactionUpdatesRecordInBetween_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly) throws TransactionException, ExecutionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecords(manager, namespace1, TABLE_1);
    DistributedTransaction transaction = begin(manager, readOnly);
    Scan scan = prepareScan(0, namespace1, TABLE_1);

    // Act
    List<Result> result1 = transaction.scan(scan);

    // The record updated by another transaction
    int updatedBalance = 100;
    manager.update(
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, updatedBalance)
            .build());

    List<Result> result2 = transaction.scan(scan);

    // Assert
    if (isolation == Isolation.READ_COMMITTED) {
      // In READ_COMMITTED isolation

      transaction.commit();

      // The storage operation should be called twice because every scan should be a new read
      verify(storage, times(2)).scan(any(Scan.class));

      assertThat(result1).hasSize(4);
      assertThat(result1.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result1.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(0);
      assertThat(result1.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result1.get(0)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
      assertThat(result1.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result1.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);
      assertThat(result1.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result1.get(1)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
      assertThat(result1.get(2).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result1.get(2).getInt(ACCOUNT_TYPE)).isEqualTo(2);
      assertThat(result1.get(2).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result1.get(2)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
      assertThat(result1.get(3).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result1.get(3).getInt(ACCOUNT_TYPE)).isEqualTo(3);
      assertThat(result1.get(3).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result1.get(3)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);

      // The update record should be returned by the second scan
      assertThat(result2).hasSize(4);
      assertThat(result2.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result2.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(0);
      assertThat(result2.get(0).getInt(BALANCE)).isEqualTo(100);
      assertThat(
              ((TransactionResult) ((FilteredResult) result2.get(0)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
      assertThat(result2.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result2.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);
      assertThat(result2.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result2.get(1)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
      assertThat(result2.get(2).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result2.get(2).getInt(ACCOUNT_TYPE)).isEqualTo(2);
      assertThat(result2.get(2).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result2.get(2)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
      assertThat(result2.get(3).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result2.get(3).getInt(ACCOUNT_TYPE)).isEqualTo(3);
      assertThat(result2.get(3).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result2.get(3)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
    } else if (isolation == Isolation.SNAPSHOT) {
      // In SNAPSHOT isolation

      transaction.commit();

      // The storage operation should be called once because the second scan should be served from
      // the snapshot
      verify(storage).scan(any(Scan.class));

      assertThat(result1).hasSize(4);
      assertThat(result1.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result1.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(0);
      assertThat(result1.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result1.get(0)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
      assertThat(result1.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result1.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);
      assertThat(result1.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result1.get(1)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
      assertThat(result1.get(2).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result1.get(2).getInt(ACCOUNT_TYPE)).isEqualTo(2);
      assertThat(result1.get(2).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result1.get(2)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
      assertThat(result1.get(3).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result1.get(3).getInt(ACCOUNT_TYPE)).isEqualTo(3);
      assertThat(result1.get(3).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result1.get(3)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);

      // The same result should as the first scan should be returned by the second scan
      assertThat(result1).isEqualTo(result2);
    } else {
      assert isolation == Isolation.SERIALIZABLE;

      // In SERIALIZABLE isolation, an anti-dependency should be detected
      assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
    }
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void scan_CalledTwiceAndAnotherTransactionInsertsRecordInBetween_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly) throws TransactionException, ExecutionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 2))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    DistributedTransaction transaction = begin(manager, readOnly);
    Scan scan = prepareScan(0, namespace1, TABLE_1);

    // Act
    List<Result> result1 = transaction.scan(scan);

    // The record inserted by another transaction
    manager.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());

    List<Result> result2 = transaction.scan(scan);

    // Assert
    if (isolation == Isolation.READ_COMMITTED) {
      // In READ_COMMITTED isolation

      transaction.commit();

      // The storage operation should be called twice because every scan should be a new read
      verify(storage, times(2)).scan(any(Scan.class));

      assertThat(result1).hasSize(2);
      assertThat(result1.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result1.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(1);
      assertThat(result1.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result1.get(1)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
      assertThat(result1.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result1.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(2);
      assertThat(result1.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

      // The inserted record should be returned by the second scan
      assertThat(result2).hasSize(3);
      assertThat(result2.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result2.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(0);
      assertThat(result2.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result2.get(0)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
      assertThat(result2.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result2.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);
      assertThat(result2.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result2.get(1)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
      assertThat(result2.get(2).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result2.get(2).getInt(ACCOUNT_TYPE)).isEqualTo(2);
      assertThat(result2.get(2).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result2.get(2)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
    } else if (isolation == Isolation.SNAPSHOT) {
      // In SNAPSHOT isolation

      transaction.commit();

      // The storage operation should be called once because the second scan should be served from
      // the snapshot
      verify(storage).scan(any(Scan.class));

      assertThat(result1).hasSize(2);
      assertThat(result1.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result1.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(1);
      assertThat(result1.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result1.get(1)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
      assertThat(result1.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result1.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(2);
      assertThat(result1.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

      // The same result should as the first scan should be returned by the second scan
      assertThat(result1).isEqualTo(result2);
    } else {
      assert isolation == Isolation.SERIALIZABLE;

      // In SERIALIZABLE isolation, an anti-dependency should be detected
      assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
    }
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void getScanner_CalledTwice_ShouldBehaveCorrectly(Isolation isolation, boolean readOnly)
      throws TransactionException, ExecutionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecords(manager, namespace1, TABLE_1);
    DistributedTransaction transaction = begin(manager, readOnly);
    Scan scan = prepareScan(0, namespace1, TABLE_1);

    // Act Assert
    TransactionCrudOperable.Scanner scanner1 = transaction.getScanner(scan);

    Optional<Result> result = scanner1.one();
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(((TransactionResult) ((FilteredResult) result.get()).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);

    result = scanner1.one();
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.get().getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(result.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(((TransactionResult) ((FilteredResult) result.get()).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);

    result = scanner1.one();
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.get().getInt(ACCOUNT_TYPE)).isEqualTo(2);
    assertThat(result.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(((TransactionResult) ((FilteredResult) result.get()).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);

    result = scanner1.one();
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.get().getInt(ACCOUNT_TYPE)).isEqualTo(3);
    assertThat(result.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(((TransactionResult) ((FilteredResult) result.get()).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);

    assertThat(scanner1.one()).isEmpty();

    scanner1.close();

    TransactionCrudOperable.Scanner scanner2 = transaction.getScanner(scan);

    result = scanner2.one();
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(((TransactionResult) ((FilteredResult) result.get()).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);

    result = scanner2.one();
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.get().getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(result.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(((TransactionResult) ((FilteredResult) result.get()).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);

    result = scanner2.one();
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.get().getInt(ACCOUNT_TYPE)).isEqualTo(2);
    assertThat(result.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(((TransactionResult) ((FilteredResult) result.get()).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);

    result = scanner2.one();
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.get().getInt(ACCOUNT_TYPE)).isEqualTo(3);
    assertThat(result.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(((TransactionResult) ((FilteredResult) result.get()).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);

    assertThat(scanner2.one()).isEmpty();

    scanner2.close();

    transaction.commit();

    if (isolation == Isolation.READ_COMMITTED) {
      // In READ_COMMITTED isolation, the storage operation should be called twice because every
      // scan should be a new read
      verify(storage, times(2)).scan(any(Scan.class));
    } else if (isolation == Isolation.SNAPSHOT) {
      // In SNAPSHOT isolation, the storage operation should be called once because the second scan
      // should be served from the snapshot
      verify(storage).scan(any(Scan.class));
    } else {
      assert isolation == Isolation.SERIALIZABLE;

      // In SERIALIZABLE isolation, the storage operation should be called twice because the second
      // scan should be served from the snapshot, but the storage operation should additionally be
      // call the by the validation
      verify(storage, times(2)).scan(any(Scan.class));
    }
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void getScanner_CalledTwiceAndAnotherTransactionUpdateRecordInBetween_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly) throws TransactionException, ExecutionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecords(manager, namespace1, TABLE_1);
    DistributedTransaction transaction = begin(manager, readOnly);
    Scan scan = prepareScan(0, namespace1, TABLE_1);

    // Act
    TransactionCrudOperable.Scanner scanner1 = transaction.getScanner(scan);
    List<Result> result1 = scanner1.all();
    scanner1.close();

    // The record updated by another transaction
    int updatedBalance = 100;
    manager.update(
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, updatedBalance)
            .build());

    TransactionCrudOperable.Scanner scanner2 = transaction.getScanner(scan);
    List<Result> result2 = scanner2.all();
    scanner2.close();

    // Assert
    if (isolation == Isolation.READ_COMMITTED) {
      // In READ_COMMITTED isolation

      transaction.commit();

      // The storage operation should be called twice because every scan should be a new read
      verify(storage, times(2)).scan(any(Scan.class));

      assertThat(result1).hasSize(4);
      assertThat(result1.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result1.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(0);
      assertThat(result1.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result1.get(0)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
      assertThat(result1.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result1.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);
      assertThat(result1.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result1.get(1)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
      assertThat(result1.get(2).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result1.get(2).getInt(ACCOUNT_TYPE)).isEqualTo(2);
      assertThat(result1.get(2).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result1.get(2)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
      assertThat(result1.get(3).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result1.get(3).getInt(ACCOUNT_TYPE)).isEqualTo(3);
      assertThat(result1.get(3).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result1.get(3)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);

      // The update record should be returned by the second scan
      assertThat(result2).hasSize(4);
      assertThat(result2.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result2.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(0);
      assertThat(result2.get(0).getInt(BALANCE)).isEqualTo(100);
      assertThat(
              ((TransactionResult) ((FilteredResult) result2.get(0)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
      assertThat(result2.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result2.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);
      assertThat(result2.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result2.get(1)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
      assertThat(result2.get(2).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result2.get(2).getInt(ACCOUNT_TYPE)).isEqualTo(2);
      assertThat(result2.get(2).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result2.get(2)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
      assertThat(result2.get(3).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result2.get(3).getInt(ACCOUNT_TYPE)).isEqualTo(3);
      assertThat(result2.get(3).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result2.get(3)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
    } else if (isolation == Isolation.SNAPSHOT) {
      // In SNAPSHOT isolation

      transaction.commit();

      // The storage operation should be called once because the second scan should be served from
      // the snapshot
      verify(storage).scan(any(Scan.class));

      assertThat(result1).hasSize(4);
      assertThat(result1.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result1.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(0);
      assertThat(result1.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result1.get(0)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
      assertThat(result1.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result1.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);
      assertThat(result1.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result1.get(1)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
      assertThat(result1.get(2).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result1.get(2).getInt(ACCOUNT_TYPE)).isEqualTo(2);
      assertThat(result1.get(2).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result1.get(2)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
      assertThat(result1.get(3).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result1.get(3).getInt(ACCOUNT_TYPE)).isEqualTo(3);
      assertThat(result1.get(3).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result1.get(3)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);

      // The same result should as the first scan should be returned by the second scan
      assertThat(result1).isEqualTo(result2);
    } else {
      assert isolation == Isolation.SERIALIZABLE;

      // In SERIALIZABLE isolation, an anti-dependency should be detected
      assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
    }
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void getScanner_CalledTwiceAndAnotherTransactionInsertsRecordInBetween_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly) throws TransactionException, ExecutionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 2))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    DistributedTransaction transaction = begin(manager, readOnly);
    Scan scan = prepareScan(0, namespace1, TABLE_1);

    // Act
    TransactionCrudOperable.Scanner scanner1 = transaction.getScanner(scan);
    List<Result> result1 = scanner1.all();
    scanner1.close();

    // The record inserted by another transaction
    manager.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());

    TransactionCrudOperable.Scanner scanner2 = transaction.getScanner(scan);
    List<Result> result2 = scanner2.all();
    scanner2.close();

    // Assert
    if (isolation == Isolation.READ_COMMITTED) {
      // In READ_COMMITTED isolation

      transaction.commit();

      // The storage operation should be called twice because every scan should be a new read
      verify(storage, times(2)).scan(any(Scan.class));

      assertThat(result1).hasSize(2);
      assertThat(result1.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result1.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(1);
      assertThat(result1.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result1.get(1)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
      assertThat(result1.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result1.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(2);
      assertThat(result1.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

      // The inserted record should be returned by the second scan
      assertThat(result2).hasSize(3);
      assertThat(result2.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result2.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(0);
      assertThat(result2.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result2.get(0)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
      assertThat(result2.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result2.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);
      assertThat(result2.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result2.get(1)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
      assertThat(result2.get(2).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result2.get(2).getInt(ACCOUNT_TYPE)).isEqualTo(2);
      assertThat(result2.get(2).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result2.get(2)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
    } else if (isolation == Isolation.SNAPSHOT) {
      // In SNAPSHOT isolation

      transaction.commit();

      // The storage operation should be called once because the second scan should be served from
      // the snapshot
      verify(storage).scan(any(Scan.class));

      assertThat(result1).hasSize(2);
      assertThat(result1.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result1.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(1);
      assertThat(result1.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
      assertThat(
              ((TransactionResult) ((FilteredResult) result1.get(1)).getOriginalResult())
                  .getState())
          .isEqualTo(TransactionState.COMMITTED);
      assertThat(result1.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
      assertThat(result1.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(2);
      assertThat(result1.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

      // The same result should as the first scan should be returned by the second scan
      assertThat(result1).isEqualTo(result2);
    } else {
      assert isolation == Isolation.SERIALIZABLE;

      // In SERIALIZABLE isolation, an anti-dependency should be detected
      assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
    }
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void get_GetGivenForNonExisting_ShouldReturnEmpty(Isolation isolation, boolean readOnly)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecord(manager, namespace1, TABLE_1);
    DistributedTransaction transaction = begin(manager, readOnly);
    Get get = prepareGet(0, 4, namespace1, TABLE_1);

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isFalse();
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void scan_ScanGivenForNonExisting_ShouldReturnEmpty(Isolation isolation, boolean readOnly)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecords(manager, namespace1, TABLE_1);
    DistributedTransaction transaction = begin(manager, readOnly);
    Scan scan = prepareScan(0, 4, 4, namespace1, TABLE_1);

    // Act
    List<Result> results = transaction.scan(scan);
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(0);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void getScanner_ScanGivenForNonExisting_ShouldReturnEmpty(Isolation isolation, boolean readOnly)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecords(manager, namespace1, TABLE_1);
    DistributedTransaction transaction = begin(manager, readOnly);
    Scan scan = prepareScan(0, 4, 4, namespace1, TABLE_1);

    // Act Assert
    TransactionCrudOperable.Scanner scanner = transaction.getScanner(scan);
    assertThat(scanner.one()).isEmpty();
    scanner.close();
    transaction.commit();
  }

  private void
      selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldBehaveCorrectly(
          Selection s,
          boolean useScanner,
          Isolation isolation,
          boolean readOnly,
          CommitType commitType)
          throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    long current = System.currentTimeMillis();
    String ongoingTxId =
        populatePreparedRecordAndCoordinatorStateRecord(
            storage,
            namespace1,
            TABLE_1,
            TransactionState.PREPARED,
            current,
            TransactionState.COMMITTED,
            commitType);
    DistributedTransaction transaction = begin(manager, readOnly);

    // Act
    TransactionResult result;
    if (s instanceof Get) {
      Optional<Result> r = transaction.get((Get) s);
      assertThat(r).isPresent();
      result = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    } else {
      List<Result> results;
      if (!useScanner) {
        results = transaction.scan((Scan) s);
      } else {
        try (TransactionCrudOperable.Scanner scanner = transaction.getScanner((Scan) s)) {
          results = scanner.all();
        }
      }
      assertThat(results.size()).isEqualTo(1);
      result = (TransactionResult) ((FilteredResult) results.get(0)).getOriginalResult();
    }
    transaction.commit();

    waitForRecoveryCompletion(transaction);

    // Assert
    if (isolation == Isolation.READ_COMMITTED) {
      // In READ_COMMITTED isolation

      // The rolled backed record should be returned
      assertThat(result.getId()).isEqualTo(ANY_ID_1);
      assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
      assertThat(result.getVersion()).isEqualTo(1);
      assertThat(result.getCommittedAt()).isEqualTo(1);

      if (readOnly) {
        // In read-only mode, recovery should not occur
        verify(recovery, never())
            .recover(any(Selection.class), any(TransactionResult.class), any());
        verify(recovery, never())
            .rollforwardRecord(any(Selection.class), any(TransactionResult.class));
      } else {
        // In read-write mode, recovery should occur
        verify(recovery).recover(any(Selection.class), any(TransactionResult.class), any());
        verify(recovery).rollforwardRecord(any(Selection.class), any(TransactionResult.class));
      }
    } else {
      // In SNAPSHOT or SERIALIZABLE isolation

      // The rolled forward record should be returned
      assertThat(result.getId()).isEqualTo(ongoingTxId);
      assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
      assertThat(result.getVersion()).isEqualTo(2);
      assertThat(result.getCommittedAt()).isGreaterThan(0);

      // Recovery should occur
      verify(recovery).recover(any(Selection.class), any(TransactionResult.class), any());
      verify(recovery).rollforwardRecord(any(Selection.class), any(TransactionResult.class));
    }
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void get_GetGivenForPreparedWhenCoordinatorStateCommitted_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldBehaveCorrectly(
        get, false, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void scan_ScanGivenForPreparedWhenCoordinatorStateCommitted_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldBehaveCorrectly(
        scan, false, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void getScanner_ScanGivenForPreparedWhenCoordinatorStateCommitted_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldBehaveCorrectly(
        scan, true, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void scanAll_ScanAllGivenForPreparedWhenCoordinatorStateCommitted_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldBehaveCorrectly(
        scanAll, false, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void getScanner_ScanAllGivenForPreparedWhenCoordinatorStateCommitted_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldBehaveCorrectly(
        scanAll, true, isolation, readOnly, commitType);
  }

  private void selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldBehaveCorrectly(
      Selection s, boolean useScanner, Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage,
        namespace1,
        TABLE_1,
        TransactionState.PREPARED,
        current,
        TransactionState.ABORTED,
        commitType);
    DistributedTransaction transaction = begin(manager, readOnly);

    // Act
    TransactionResult result;
    if (s instanceof Get) {
      Optional<Result> r = transaction.get((Get) s);
      assertThat(r).isPresent();
      result = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    } else {
      List<Result> results;
      if (!useScanner) {
        results = transaction.scan((Scan) s);
      } else {
        try (TransactionCrudOperable.Scanner scanner = transaction.getScanner((Scan) s)) {
          results = scanner.all();
        }
      }
      assertThat(results.size()).isEqualTo(1);
      result = (TransactionResult) ((FilteredResult) results.get(0)).getOriginalResult();
    }

    transaction.commit();

    waitForRecoveryCompletion(transaction);

    // Assert

    // In all isolations, the rolled back record should be returned
    assertThat(result.getId()).isEqualTo(ANY_ID_1);
    assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
    assertThat(result.getCommittedAt()).isEqualTo(1);

    if (isolation == Isolation.READ_COMMITTED && readOnly) {
      // In READ_COMMITTED isolation and read-only mode, recovery should not occur
      verify(recovery, never()).recover(any(Selection.class), any(TransactionResult.class), any());
      verify(recovery, never()).rollbackRecord(any(Selection.class), any(TransactionResult.class));
    } else {
      // In other cases, recovery should occur
      verify(recovery).recover(any(Selection.class), any(TransactionResult.class), any());
      verify(recovery).rollbackRecord(any(Selection.class), any(TransactionResult.class));
    }
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void get_GetGivenForPreparedWhenCoordinatorStateAborted_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws TransactionException, ExecutionException, CoordinatorException {
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldBehaveCorrectly(
        get, false, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void scan_ScanGivenForPreparedWhenCoordinatorStateAborted_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws TransactionException, ExecutionException, CoordinatorException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldBehaveCorrectly(
        scan, false, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void getScanner_ScanGivenForPreparedWhenCoordinatorStateAborted_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws TransactionException, ExecutionException, CoordinatorException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldBehaveCorrectly(
        scan, true, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void scanAll_ScanAllGivenForPreparedWhenCoordinatorStateAborted_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws TransactionException, ExecutionException, CoordinatorException {
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldBehaveCorrectly(
        scanAll, false, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void getScanner_ScanAllGivenForPreparedWhenCoordinatorStateAborted_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws TransactionException, ExecutionException, CoordinatorException {
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldBehaveCorrectly(
        scanAll, true, isolation, readOnly, commitType);
  }

  private void
      selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldBehaveCorrectly(
          Selection s,
          boolean useScanner,
          Isolation isolation,
          boolean readOnly,
          CommitType commitType)
          throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    long prepared_at = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, namespace1, TABLE_1, TransactionState.PREPARED, prepared_at, null, commitType);
    DistributedTransaction transaction = begin(manager, readOnly);

    // Act Assert
    if (isolation == Isolation.READ_COMMITTED) {
      // In READ_COMMITTED isolation

      // UncommittedRecordException should not be thrown
      assertThatCode(
              () -> {
                TransactionResult result;
                if (s instanceof Get) {
                  Optional<Result> r = transaction.get((Get) s);
                  assertThat(r).isPresent();
                  result = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
                } else {
                  List<Result> results;
                  if (!useScanner) {
                    results = transaction.scan((Scan) s);
                  } else {
                    try (TransactionCrudOperable.Scanner scanner =
                        transaction.getScanner((Scan) s)) {
                      results = scanner.all();
                    }
                  }
                  assertThat(results.size()).isEqualTo(1);
                  result =
                      (TransactionResult) ((FilteredResult) results.get(0)).getOriginalResult();
                }

                // The rolled back record should be returned
                assertThat(result.getId()).isEqualTo(ANY_ID_1);
                assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
                assertThat(result.getVersion()).isEqualTo(1);
                assertThat(result.getCommittedAt()).isEqualTo(1);
              })
          .doesNotThrowAnyException();

      transaction.commit();
    } else {
      // In SNAPSHOT or SERIALIZABLE isolation

      // UncommittedRecordException should be thrown
      assertThatThrownBy(
              () -> {
                if (s instanceof Get) {
                  transaction.get((Get) s);
                } else {
                  if (!useScanner) {
                    transaction.scan((Scan) s);
                  } else {
                    try (TransactionCrudOperable.Scanner scanner =
                        transaction.getScanner((Scan) s)) {
                      scanner.all();
                    }
                  }
                }
              })
          .isInstanceOf(UncommittedRecordException.class);

      transaction.rollback();
    }

    // In all cases, recovery should not occur
    verify(recovery, never()).recover(any(Selection.class), any(TransactionResult.class), any());
    verify(coordinator, never()).putState(any(Coordinator.State.class));
    verify(recovery, never()).rollbackRecord(any(Selection.class), any(TransactionResult.class));
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void get_GetGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldBehaveCorrectly(
        get, false, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void scan_ScanGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldBehaveCorrectly(
        scan, false, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void
      getScanner_ScanGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldBehaveCorrectly(
          Isolation isolation, boolean readOnly, CommitType commitType)
          throws ExecutionException, CoordinatorException, TransactionException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldBehaveCorrectly(
        scan, true, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void
      scanAll_ScanAllGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldBehaveCorrectly(
          Isolation isolation, boolean readOnly, CommitType commitType)
          throws ExecutionException, CoordinatorException, TransactionException {
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldBehaveCorrectly(
        scanAll, false, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void
      getScanner_ScanAllGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldBehaveCorrectly(
          Isolation isolation, boolean readOnly, CommitType commitType)
          throws ExecutionException, CoordinatorException, TransactionException {
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldBehaveCorrectly(
        scanAll, true, isolation, readOnly, commitType);
  }

  private void
      selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldBehaveCorrectly(
          Selection s,
          boolean useScanner,
          Isolation isolation,
          boolean readOnly,
          CommitType commitType)
          throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    long prepared_at = System.currentTimeMillis() - RecoveryHandler.TRANSACTION_LIFETIME_MILLIS - 1;
    String ongoingTxId =
        populatePreparedRecordAndCoordinatorStateRecord(
            storage, namespace1, TABLE_1, TransactionState.PREPARED, prepared_at, null, commitType);
    DistributedTransaction transaction = begin(manager, readOnly);

    // Act
    TransactionResult result;
    if (s instanceof Get) {
      Optional<Result> r = transaction.get((Get) s);
      assertThat(r).isPresent();
      result = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    } else {
      List<Result> results;
      if (!useScanner) {
        results = transaction.scan((Scan) s);
      } else {
        try (TransactionCrudOperable.Scanner scanner = transaction.getScanner((Scan) s)) {
          results = scanner.all();
        }
      }
      assertThat(results.size()).isEqualTo(1);
      result = (TransactionResult) ((FilteredResult) results.get(0)).getOriginalResult();
    }

    transaction.commit();

    waitForRecoveryCompletion(transaction);

    // Assert

    // In all isolations, the rolled back record should be returned
    assertThat(result.getId()).isEqualTo(ANY_ID_1);
    assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
    assertThat(result.getCommittedAt()).isEqualTo(1);

    if (isolation == Isolation.READ_COMMITTED && readOnly) {
      // In READ_COMMITTED isolation and read-only mode, recovery should not occur
      verify(recovery, never()).recover(any(Selection.class), any(TransactionResult.class), any());
      verify(coordinator, never()).putState(any(Coordinator.State.class));
      verify(recovery, never()).rollbackRecord(any(Selection.class), any(TransactionResult.class));
    } else {
      // In other cases, recovery should occur

      verify(recovery).recover(any(Selection.class), any(TransactionResult.class), any());
      verify(coordinator).putState(new Coordinator.State(ongoingTxId, TransactionState.ABORTED));
      verify(recovery).rollbackRecord(any(Selection.class), any(TransactionResult.class));
    }
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void get_GetGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldBehaveCorrectly(
        get, false, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void scan_ScanGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldBehaveCorrectly(
        scan, false, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void getScanner_ScanGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldBehaveCorrectly(
        scan, true, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void
      getScanner_ScanAllGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldBehaveCorrectly(
          Isolation isolation, boolean readOnly, CommitType commitType)
          throws ExecutionException, CoordinatorException, TransactionException {
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldBehaveCorrectly(
        scanAll, true, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void scanAll_ScanAllGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldBehaveCorrectly(
        scanAll, false, isolation, readOnly, commitType);
  }

  private void
      selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldBehaveCorrectly(
          Selection s,
          boolean useScanner,
          Isolation isolation,
          boolean readOnly,
          CommitType commitType)
          throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage,
        namespace1,
        TABLE_1,
        TransactionState.DELETED,
        current,
        TransactionState.COMMITTED,
        commitType);
    DistributedTransaction transaction = begin(manager, readOnly);

    // Act
    @Nullable TransactionResult result;
    if (s instanceof Get) {
      Optional<Result> r = transaction.get((Get) s);
      result =
          r.map(value -> (TransactionResult) ((FilteredResult) value).getOriginalResult())
              .orElse(null);
    } else {
      List<Result> results;
      if (!useScanner) {
        results = transaction.scan((Scan) s);
      } else {
        try (TransactionCrudOperable.Scanner scanner = transaction.getScanner((Scan) s)) {
          results = scanner.all();
        }
      }

      if (!results.isEmpty()) {
        assertThat(results.size()).isEqualTo(1);
        result = (TransactionResult) ((FilteredResult) results.get(0)).getOriginalResult();
      } else {
        result = null;
      }
    }

    transaction.commit();

    waitForRecoveryCompletion(transaction);

    // Assert
    if (isolation == Isolation.READ_COMMITTED) {
      // In READ_COMMITTED isolation

      // The rolled back record should be returned
      assertThat(result).isNotNull();
      assertThat(result.getId()).isEqualTo(ANY_ID_1);
      assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
      assertThat(result.getVersion()).isEqualTo(1);
      assertThat(result.getCommittedAt()).isEqualTo(1);

      if (readOnly) {
        // In read-only mode, recovery should not occur
        verify(recovery, never())
            .recover(any(Selection.class), any(TransactionResult.class), any());
        verify(recovery, never())
            .rollforwardRecord(any(Selection.class), any(TransactionResult.class));
      } else {
        // In read-write mode, recovery should occur
        verify(recovery).recover(any(Selection.class), any(TransactionResult.class), any());
        verify(recovery).rollforwardRecord(any(Selection.class), any(TransactionResult.class));
      }
    } else {
      // In SNAPSHOT or SERIALIZABLE isolation

      // The rolled forward record should be returned
      assertThat(result).isNull();

      // Recovery should occur
      verify(recovery).recover(any(Selection.class), any(TransactionResult.class), any());
      verify(recovery).rollforwardRecord(any(Selection.class), any(TransactionResult.class));
    }
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void get_GetGivenForDeletedWhenCoordinatorStateCommitted_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldBehaveCorrectly(
        get, false, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void scan_ScanGivenForDeletedWhenCoordinatorStateCommitted_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldBehaveCorrectly(
        scan, false, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void getScanner_ScanGivenForDeletedWhenCoordinatorStateCommitted_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldBehaveCorrectly(
        scan, true, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void scanAll_ScanAllGivenForDeletedWhenCoordinatorStateCommitted_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldBehaveCorrectly(
        scanAll, false, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void getScanner_ScanAllGivenForDeletedWhenCoordinatorStateCommitted_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldBehaveCorrectly(
        scanAll, true, isolation, readOnly, commitType);
  }

  private void selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldBehaveCorrectly(
      Selection s, boolean useScanner, Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage,
        namespace1,
        TABLE_1,
        TransactionState.DELETED,
        current,
        TransactionState.ABORTED,
        commitType);
    DistributedTransaction transaction = begin(manager, readOnly);

    // Act
    TransactionResult result;
    if (s instanceof Get) {
      Optional<Result> r = transaction.get((Get) s);
      assertThat(r).isPresent();
      result = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    } else {
      List<Result> results;
      if (!useScanner) {
        results = transaction.scan((Scan) s);
      } else {
        try (TransactionCrudOperable.Scanner scanner = transaction.getScanner((Scan) s)) {
          results = scanner.all();
        }
      }
      assertThat(results.size()).isEqualTo(1);
      result = (TransactionResult) ((FilteredResult) results.get(0)).getOriginalResult();
    }

    transaction.commit();

    waitForRecoveryCompletion(transaction);

    // Assert

    // In all isolations, the rolled back record should be returned
    assertThat(result.getId()).isEqualTo(ANY_ID_1);
    assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
    assertThat(result.getCommittedAt()).isEqualTo(1);

    if (isolation == Isolation.READ_COMMITTED && readOnly) {
      // In READ_COMMITTED isolation and read-only mode, recovery should not occur
      verify(recovery, never()).recover(any(Selection.class), any(TransactionResult.class), any());
      verify(recovery, never()).rollbackRecord(any(Selection.class), any(TransactionResult.class));
    } else {
      // In other cases, recovery should occur
      verify(recovery).recover(any(Selection.class), any(TransactionResult.class), any());
      verify(recovery).rollbackRecord(any(Selection.class), any(TransactionResult.class));
    }
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void get_GetGivenForDeletedWhenCoordinatorStateAborted_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldBehaveCorrectly(
        get, false, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void scan_ScanGivenForDeletedWhenCoordinatorStateAborted_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldBehaveCorrectly(
        scan, false, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void getScanner_ScanGivenForDeletedWhenCoordinatorStateAborted_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldBehaveCorrectly(
        scan, true, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void scanAll_ScanAllGivenForDeletedWhenCoordinatorStateAborted_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldBehaveCorrectly(
        scanAll, false, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void getScanner_ScanAllGivenForDeletedWhenCoordinatorStateAborted_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldBehaveCorrectly(
        scanAll, true, isolation, readOnly, commitType);
  }

  private void
      selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldBehaveCorrectly(
          Selection s,
          boolean useScanner,
          Isolation isolation,
          boolean readOnly,
          CommitType commitType)
          throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    long prepared_at = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, namespace1, TABLE_1, TransactionState.DELETED, prepared_at, null, commitType);
    DistributedTransaction transaction = begin(manager, readOnly);

    // Act Assert
    if (isolation == Isolation.READ_COMMITTED) {
      // In READ_COMMITTED isolation

      // UncommittedRecordException should not be thrown
      assertThatCode(
              () -> {
                TransactionResult result;
                if (s instanceof Get) {
                  Optional<Result> r = transaction.get((Get) s);
                  assertThat(r).isPresent();
                  result = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
                } else {
                  List<Result> results;
                  if (!useScanner) {
                    results = transaction.scan((Scan) s);
                  } else {
                    try (TransactionCrudOperable.Scanner scanner =
                        transaction.getScanner((Scan) s)) {
                      results = scanner.all();
                    }
                  }
                  assertThat(results.size()).isEqualTo(1);
                  result =
                      (TransactionResult) ((FilteredResult) results.get(0)).getOriginalResult();
                }

                // The rolled back record should be returned
                assertThat(result.getId()).isEqualTo(ANY_ID_1);
                assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
                assertThat(result.getVersion()).isEqualTo(1);
                assertThat(result.getCommittedAt()).isEqualTo(1);
              })
          .doesNotThrowAnyException();

      transaction.commit();
    } else {
      // In SNAPSHOT or SERIALIZABLE isolation

      // UncommittedRecordException should be thrown
      assertThatThrownBy(
              () -> {
                if (s instanceof Get) {
                  transaction.get((Get) s);
                } else {
                  if (!useScanner) {
                    transaction.scan((Scan) s);
                  } else {
                    try (TransactionCrudOperable.Scanner scanner =
                        transaction.getScanner((Scan) s)) {
                      scanner.all();
                    }
                  }
                }
              })
          .isInstanceOf(UncommittedRecordException.class);

      transaction.rollback();
    }

    // In all cases, recovery should not occur
    verify(recovery, never()).recover(any(Selection.class), any(TransactionResult.class), any());
    verify(coordinator, never()).putState(any(Coordinator.State.class));
    verify(recovery, never()).rollbackRecord(any(Selection.class), any(TransactionResult.class));
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void get_GetGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldBehaveCorrectly(
        get, false, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void scan_ScanGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldBehaveCorrectly(
        scan, false, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void
      getScanner_ScanGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldBehaveCorrectly(
          Isolation isolation, boolean readOnly, CommitType commitType)
          throws ExecutionException, CoordinatorException, TransactionException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldBehaveCorrectly(
        scan, true, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void
      scanAll_ScanAllGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldBehaveCorrectly(
          Isolation isolation, boolean readOnly, CommitType commitType)
          throws ExecutionException, CoordinatorException, TransactionException {
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldBehaveCorrectly(
        scanAll, false, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void
      getScanner_ScanAllGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldBehaveCorrectly(
          Isolation isolation, boolean readOnly, CommitType commitType)
          throws ExecutionException, CoordinatorException, TransactionException {
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldBehaveCorrectly(
        scanAll, true, isolation, readOnly, commitType);
  }

  private void
      selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldBehaveCorrectly(
          Selection s,
          boolean useScanner,
          Isolation isolation,
          boolean readOnly,
          CommitType commitType)
          throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    long prepared_at = System.currentTimeMillis() - RecoveryHandler.TRANSACTION_LIFETIME_MILLIS - 1;
    String ongoingTxId =
        populatePreparedRecordAndCoordinatorStateRecord(
            storage, namespace1, TABLE_1, TransactionState.DELETED, prepared_at, null, commitType);
    DistributedTransaction transaction = begin(manager, readOnly);

    // Act
    TransactionResult result;
    if (s instanceof Get) {
      Optional<Result> r = transaction.get((Get) s);
      assertThat(r).isPresent();
      result = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    } else {
      List<Result> results;
      if (!useScanner) {
        results = transaction.scan((Scan) s);
      } else {
        try (TransactionCrudOperable.Scanner scanner = transaction.getScanner((Scan) s)) {
          results = scanner.all();
        }
      }
      assertThat(results.size()).isEqualTo(1);
      result = (TransactionResult) ((FilteredResult) results.get(0)).getOriginalResult();
    }

    transaction.commit();

    // Wait for the recovery to complete
    waitForRecoveryCompletion(transaction);

    // Assert

    // In all isolations, the rolled back record should be returned
    assertThat(result.getId()).isEqualTo(ANY_ID_1);
    assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
    assertThat(result.getCommittedAt()).isEqualTo(1);

    if (isolation == Isolation.READ_COMMITTED && readOnly) {
      // In READ_COMMITTED isolation and read-only mode, recovery should not occur
      verify(recovery, never()).recover(any(Selection.class), any(TransactionResult.class), any());
      verify(recovery, never()).rollbackRecord(any(Selection.class), any(TransactionResult.class));
    } else {
      // In other cases, recovery should occur
      verify(recovery).recover(any(Selection.class), any(TransactionResult.class), any());
      verify(coordinator).putState(new Coordinator.State(ongoingTxId, TransactionState.ABORTED));
      verify(recovery).rollbackRecord(any(Selection.class), any(TransactionResult.class));
    }
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void get_GetGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldBehaveCorrectly(
        get, false, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void scan_ScanGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldBehaveCorrectly(
        scan, false, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void getScanner_ScanGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldBehaveCorrectly(
        scan, true, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void scanAll_ScanAllGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldBehaveCorrectly(
        scanAll, false, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyModeAndCommitType")
  void
      getScanner_ScanAllGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldBehaveCorrectly(
          Isolation isolation, boolean readOnly, CommitType commitType)
          throws ExecutionException, CoordinatorException, TransactionException {
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldBehaveCorrectly(
        scanAll, true, isolation, readOnly, commitType);
  }

  @ParameterizedTest
  @MethodSource("isolationAndCommitType")
  void update_UpdateGivenForPreparedWhenCoordinatorStateCommitted_ShouldBehaveCorrectly(
      Isolation isolation, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage,
        namespace1,
        TABLE_1,
        TransactionState.PREPARED,
        current,
        TransactionState.COMMITTED,
        commitType);

    Get get =
        Get.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .build();

    DistributedTransaction transaction = manager.begin();

    // Act Assert
    Optional<Result> result = transaction.get(get);
    assertThat(result.isPresent()).isTrue();

    if (isolation == Isolation.READ_COMMITTED) {
      // In READ_COMMITTED isolation

      // The rolled back record should be returned
      assertThat(result.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    } else {
      // In SNAPSHOT or SERIALIZABLE isolation

      // The rolled forward record should be returned
      assertThat(result.get().getInt(BALANCE)).isEqualTo(NEW_BALANCE);
    }

    transaction.update(
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, result.get().getInt(BALANCE) + 100)
            .build());

    if (isolation == Isolation.READ_COMMITTED) {
      assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
      transaction.rollback();
    } else {
      // In SNAPSHOT or SERIALIZABLE isolation
      // Should commit without any exceptions
      transaction.commit();
    }

    Optional<Result> actual = manager.get(get);
    assertThat(actual.isPresent()).isTrue();

    if (isolation == Isolation.READ_COMMITTED) {
      // In READ_COMMITTED isolation

      // The transaction should not have committed
      assertThat(actual.get().getInt(BALANCE)).isEqualTo(NEW_BALANCE);
    } else {
      // In SNAPSHOT or SERIALIZABLE isolation

      // The transaction should have committed
      assertThat(actual.get().getInt(BALANCE)).isEqualTo(NEW_BALANCE + 100);
    }

    // In all isolations, recovery should occur
    verify(recovery).recover(any(Selection.class), any(TransactionResult.class), any());
    verify(recovery).rollforwardRecord(any(Selection.class), any(TransactionResult.class));
  }

  @ParameterizedTest
  @MethodSource("isolationAndCommitType")
  void update_UpdateGivenForPreparedWhenCoordinatorStateAborted_ShouldBehaveCorrectly(
      Isolation isolation, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage,
        namespace1,
        TABLE_1,
        TransactionState.PREPARED,
        current,
        TransactionState.ABORTED,
        commitType);

    Get get =
        Get.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .build();

    DistributedTransaction transaction = manager.begin();

    // Act Assert
    Optional<Result> result = transaction.get(get);

    // In all isolations, the rolled back record should be returned
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    transaction.update(
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, result.get().getInt(BALANCE) + 100)
            .build());

    transaction.commit();

    Optional<Result> actual = manager.get(get);

    // In all isolations, no anomalies occur
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE + 100);

    // In all isolations, recovery should occur
    verify(recovery).recover(any(Selection.class), any(TransactionResult.class), any());
    verify(recovery).rollbackRecord(any(Selection.class), any(TransactionResult.class));
  }

  @ParameterizedTest
  @MethodSource("isolationAndCommitType")
  void update_UpdateGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldBehaveCorrectly(
      Isolation isolation, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    long prepared_at = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, namespace1, TABLE_1, TransactionState.PREPARED, prepared_at, null, commitType);

    DistributedTransaction transaction = manager.begin();

    Update update =
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, 100)
            .build();

    // Act Assert
    if (isolation == Isolation.READ_COMMITTED) {
      // In READ_COMMITTED isolation

      // UncommittedRecordException should not be thrown when trying to update
      assertThatCode(() -> transaction.update(update)).doesNotThrowAnyException();

      // CommitConflictException should be thrown at the commit phase
      assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
    } else {
      // In SNAPSHOT or SERIALIZABLE isolation

      // UncommittedRecordException should be thrown when trying to update
      assertThatThrownBy(() -> transaction.update(update))
          .isInstanceOf(UncommittedRecordException.class);

      transaction.rollback();
    }

    // In all isolations, recovery should not occur
    verify(recovery, never()).recover(any(Selection.class), any(TransactionResult.class), any());
    verify(recovery, never()).rollbackRecord(any(Selection.class), any(TransactionResult.class));
    verify(coordinator, never()).putState(any(Coordinator.State.class));
  }

  @ParameterizedTest
  @MethodSource("isolationAndCommitType")
  void update_UpdateGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldBehaveCorrectly(
      Isolation isolation, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    long prepared_at = System.currentTimeMillis() - RecoveryHandler.TRANSACTION_LIFETIME_MILLIS - 1;
    String ongoingTxId =
        populatePreparedRecordAndCoordinatorStateRecord(
            storage, namespace1, TABLE_1, TransactionState.PREPARED, prepared_at, null, commitType);

    Get get =
        Get.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .build();

    DistributedTransaction transaction = manager.begin();

    // Act Assert
    Optional<Result> result = transaction.get(get);

    // In all isolations, the rolled back record should be returned
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    transaction.update(
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, result.get().getInt(BALANCE) + 100)
            .build());

    transaction.commit();

    Optional<Result> actual = manager.get(get);

    // In all isolations, no anomalies occur
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE + 100);

    // In all isolations, recovery should occur
    verify(recovery).recover(any(Selection.class), any(TransactionResult.class), any());
    verify(coordinator).putState(new Coordinator.State(ongoingTxId, TransactionState.ABORTED));
    verify(recovery).rollbackRecord(any(Selection.class), any(TransactionResult.class));
  }

  @ParameterizedTest
  @MethodSource("isolationAndCommitType")
  void insert_InsertGivenForDeletedWhenCoordinatorStateCommitted_ShouldBehaveCorrectly(
      Isolation isolation, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage,
        namespace1,
        TABLE_1,
        TransactionState.DELETED,
        current,
        TransactionState.COMMITTED,
        commitType);

    Get get =
        Get.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .build();

    DistributedTransaction transaction = manager.begin();

    // Act Assert
    Optional<Result> result = transaction.get(get);

    if (isolation == Isolation.READ_COMMITTED) {
      // In READ_COMMITTED isolation, the rolled back record should be returned
      assertThat(result.isPresent()).isTrue();
      assertThat(result.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    } else {
      // In SNAPSHOT or SERIALIZABLE isolation, no record should be returned
      assertThat(result.isPresent()).isFalse();
    }

    int expectedBalance = 100;

    transaction.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, expectedBalance)
            .build());

    transaction.commit();

    Optional<Result> actual = manager.get(get);

    // In all isolations, the inserted record should be returned
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getInt(BALANCE)).isEqualTo(expectedBalance);

    // In all isolations, recovery should occur
    verify(recovery).recover(any(Selection.class), any(TransactionResult.class), any());
    verify(recovery).rollforwardRecord(any(Selection.class), any(TransactionResult.class));
  }

  @ParameterizedTest
  @MethodSource("isolationAndCommitType")
  void update_UpdateGivenForDeletedWhenCoordinatorStateAborted_ShouldBehaveCorrectly(
      Isolation isolation, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage,
        namespace1,
        TABLE_1,
        TransactionState.DELETED,
        current,
        TransactionState.ABORTED,
        commitType);

    Get get =
        Get.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .build();

    DistributedTransaction transaction = manager.begin();

    // Act
    Optional<Result> result = transaction.get(get);

    // In all isolations, the rolled back record should be returned
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    transaction.update(
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, result.get().getInt(BALANCE) + 100)
            .build());

    transaction.commit();

    Optional<Result> actual = manager.get(get);

    // In all isolations, no anomalies occur
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE + 100);

    // In all isolations, recovery should occur
    verify(recovery).recover(any(Selection.class), any(TransactionResult.class), any());
    verify(recovery).rollbackRecord(any(Selection.class), any(TransactionResult.class));
  }

  @ParameterizedTest
  @MethodSource("isolationAndCommitType")
  void update_UpdateGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldBehaveCorrectly(
      Isolation isolation, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    long prepared_at = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        storage, namespace1, TABLE_1, TransactionState.DELETED, prepared_at, null, commitType);

    DistributedTransaction transaction = manager.begin();

    Update update =
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, 100)
            .build();

    // Act Assert
    if (isolation == Isolation.READ_COMMITTED) {
      // In READ_COMMITTED isolation

      // UncommittedRecordException should not be thrown when trying to update
      assertThatCode(() -> transaction.update(update)).doesNotThrowAnyException();

      // CommitConflictException should be thrown at the commit phase
      assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
    } else {
      // In SNAPSHOT or SERIALIZABLE isolation

      // UncommittedRecordException should be thrown when trying to update
      assertThatThrownBy(() -> transaction.update(update))
          .isInstanceOf(UncommittedRecordException.class);

      transaction.rollback();
    }

    // In all isolations, recovery should not occur
    verify(recovery, never()).recover(any(Selection.class), any(TransactionResult.class), any());
    verify(recovery, never()).rollbackRecord(any(Selection.class), any(TransactionResult.class));
    verify(coordinator, never()).putState(any(Coordinator.State.class));
  }

  @ParameterizedTest
  @MethodSource("isolationAndCommitType")
  void update_UpdateGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldBehaveCorrectly(
      Isolation isolation, CommitType commitType)
      throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    long prepared_at = System.currentTimeMillis() - RecoveryHandler.TRANSACTION_LIFETIME_MILLIS - 1;
    String ongoingTxId =
        populatePreparedRecordAndCoordinatorStateRecord(
            storage, namespace1, TABLE_1, TransactionState.DELETED, prepared_at, null, commitType);

    Get get =
        Get.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .build();

    DistributedTransaction transaction = manager.begin();

    // Act Assert
    Optional<Result> result = transaction.get(get);

    // In all isolations, the rolled back record should be returned
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    transaction.update(
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, result.get().getInt(BALANCE) + 100)
            .build());

    transaction.commit();

    Optional<Result> actual = manager.get(get);

    // In all isolations, no anomalies occur
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE + 100);

    // In all isolations, recovery should occur
    verify(recovery).recover(any(Selection.class), any(TransactionResult.class), any());
    verify(coordinator).putState(new Coordinator.State(ongoingTxId, TransactionState.ABORTED));
    verify(recovery).rollbackRecord(any(Selection.class), any(TransactionResult.class));
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void getThenScanAndGet_CommitHappenedInBetween_BehaveCorrectly(
      Isolation isolation, boolean readOnly) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    DistributedTransaction transaction = manager.begin();
    transaction.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, 1).build());
    transaction.commit();

    DistributedTransaction transaction1 = begin(manager, readOnly);
    Optional<Result> result1 = transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));

    DistributedTransaction transaction2 = manager.begin();
    transaction2.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction2.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, 2).build());
    transaction2.commit();

    // Act Assert
    Result result2 = transaction1.scan(prepareScan(0, 0, 0, namespace1, TABLE_1)).get(0);
    Optional<Result> result3 = transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));

    if (isolation == Isolation.READ_COMMITTED) {
      // In READ_COMMITTED isolation

      transaction1.commit();

      // Each operation reads the committed value
      assertThat(result1).isPresent();
      assertThat(result1.get().getInt(BALANCE)).isEqualTo(1);

      assertThat(result2.getInt(BALANCE)).isEqualTo(2);

      assertThat(result3).isPresent();
      assertThat(result3.get().getInt(BALANCE)).isEqualTo(2);
    } else if (isolation == Isolation.SNAPSHOT) {
      // In SNAPSHOT or SERIALIZABLE isolation

      transaction1.commit();

      // Only get reads repeatably
      assertThat(result1).isPresent();
      assertThat(result1.get().getInt(BALANCE)).isEqualTo(1);
      assertThat(result3).isPresent();
      assertThat(result3.get().getInt(BALANCE)).isEqualTo(1);

      // Scan reads the committed value
      assertThat(result2.getInt(BALANCE)).isEqualTo(2);
    } else {
      assert isolation == Isolation.SERIALIZABLE;

      // In SERIALIZABLE isolation, an anti-dependency should be detected
      assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
    }
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void putAndCommit_PutGivenForNonExisting_ShouldCreateRecord(Isolation isolation)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    int expected = INITIAL_BALANCE;
    Put put =
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, expected).build();
    DistributedTransaction transaction = manager.begin();

    // Act
    transaction.put(put);
    transaction.commit();

    // Assert
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    DistributedTransaction another = manager.beginReadOnly();
    Optional<Result> r = another.get(get);
    another.commit();

    assertThat(r).isPresent();
    TransactionResult result = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    assertThat(getBalance(result)).isEqualTo(expected);
    assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void putAndCommit_PutWithImplicitPreReadEnabledGivenForNonExisting_ShouldCreateRecord(
      Isolation isolation) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    int expected = INITIAL_BALANCE;
    Put put =
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1))
            .intValue(BALANCE, expected)
            .enableImplicitPreRead()
            .build();
    DistributedTransaction transaction = manager.begin();

    // Act
    transaction.put(put);
    transaction.commit();

    // Assert
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    DistributedTransaction another = manager.beginReadOnly();
    Optional<Result> r = another.get(get);
    another.commit();

    assertThat(r).isPresent();
    TransactionResult result = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    assertThat(getBalance(result)).isEqualTo(expected);
    assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void putAndCommit_PutGivenForExistingAfterRead_ShouldUpdateRecord(Isolation isolation)
      throws TransactionException, ExecutionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecord(manager, namespace1, TABLE_1);
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();

    // Act
    Optional<Result> result = transaction.get(get);
    assertThat(result).isPresent();
    int expected = getBalance(result.get()) + 100;
    Put put =
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, expected).build();
    transaction.put(put);
    transaction.commit();

    // Assert
    verify(storage).get(any(Get.class));

    DistributedTransaction another = manager.beginReadOnly();
    Optional<Result> r = another.get(get);
    another.commit();

    assertThat(r).isPresent();
    TransactionResult actual = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    assertThat(getBalance(actual)).isEqualTo(expected);
    assertThat(actual.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(actual.getVersion()).isEqualTo(2);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void putAndCommit_PutWithImplicitPreReadEnabledGivenForExisting_ShouldUpdateRecord(
      Isolation isolation) throws TransactionException, ExecutionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecord(manager, namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();

    // Act
    int expected = INITIAL_BALANCE + 100;
    Put put =
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1))
            .intValue(BALANCE, expected)
            .enableImplicitPreRead()
            .build();
    transaction.put(put);
    transaction.commit();

    // Assert
    verify(storage).get(any(Get.class));

    DistributedTransaction another = manager.beginReadOnly();
    Optional<Result> r = another.get(prepareGet(0, 0, namespace1, TABLE_1));
    another.commit();

    assertThat(r).isPresent();
    TransactionResult actual = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    assertThat(getBalance(actual)).isEqualTo(expected);
    assertThat(actual.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(actual.getVersion()).isEqualTo(2);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void putAndCommit_PutGivenForExisting_ShouldThrowCommitConflictException(Isolation isolation)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecord(manager, namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();

    // Act Assert
    Put put =
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1))
            .intValue(BALANCE, INITIAL_BALANCE + 100)
            .build();
    transaction.put(put);
    assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void putAndCommit_PutWithInsertModeEnabledGivenForNonExisting_ShouldCreateRecord(
      Isolation isolation) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    int expected = INITIAL_BALANCE;
    Put put =
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1))
            .intValue(BALANCE, expected)
            .enableInsertMode()
            .build();
    DistributedTransaction transaction = manager.begin();

    // Act
    transaction.put(put);
    transaction.commit();

    // Assert
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    DistributedTransaction another = manager.beginReadOnly();
    Optional<Result> r = another.get(get);
    another.commit();

    assertThat(r).isPresent();
    TransactionResult result = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    assertThat(getBalance(result)).isEqualTo(expected);
    assertThat(result.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(result.getVersion()).isEqualTo(1);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void putAndCommit_PutWithInsertModeEnabledGivenForNonExistingAfterRead_ShouldCreateRecord(
      Isolation isolation) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    Get get = prepareGet(0, 0, namespace1, TABLE_1);

    int expected = INITIAL_BALANCE;
    Put put =
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1))
            .intValue(BALANCE, expected)
            .enableInsertMode()
            .build();

    DistributedTransaction transaction = manager.begin();

    // Act
    Optional<Result> result = transaction.get(get);
    assertThat(result).isNotPresent();

    transaction.put(put);
    transaction.commit();

    // Assert
    DistributedTransaction another = manager.beginReadOnly();
    Optional<Result> r = another.get(get);
    another.commit();

    assertThat(r).isPresent();
    TransactionResult actual = (TransactionResult) ((FilteredResult) r.get()).getOriginalResult();
    assertThat(getBalance(actual)).isEqualTo(expected);
    assertThat(actual.getState()).isEqualTo(TransactionState.COMMITTED);
    assertThat(actual.getVersion()).isEqualTo(1);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void putAndCommit_PutWithInsertModeGivenForExistingAfterRead_ShouldThrowCommitConflictException(
      Isolation isolation) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecord(manager, namespace1, TABLE_1);
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();

    // Act Assert
    Optional<Result> result = transaction.get(get);
    assertThat(result).isPresent();

    Put put =
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1))
            .intValue(BALANCE, getBalance(result.get()) + 100)
            .enableInsertMode()
            .build();
    transaction.put(put);
    assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
  }

  private void putAndCommit_GetsAndPutsGiven_ShouldCommitProperly(
      Isolation isolation,
      String fromNamespace,
      String fromTable,
      String toNamespace,
      String toTable)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    boolean differentTables = !fromNamespace.equals(toNamespace) || !fromTable.equals(toTable);

    populateRecords(manager, fromNamespace, fromTable);
    if (differentTables) {
      populateRecords(manager, toNamespace, toTable);
    }

    List<Get> fromGets = prepareGets(fromNamespace, fromTable);
    List<Get> toGets = differentTables ? prepareGets(toNamespace, toTable) : fromGets;

    int amount = 100;
    int from = 0;
    int to = NUM_TYPES;

    // Act
    prepareTransfer(manager, from, fromNamespace, fromTable, to, toNamespace, toTable, amount)
        .commit();

    // Assert
    DistributedTransaction another = manager.beginReadOnly();
    Optional<Result> fromResult = another.get(fromGets.get(from));
    assertThat(fromResult).isPresent();
    assertThat(fromResult.get().contains(BALANCE)).isTrue();
    assertThat(fromResult.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE - amount);
    Optional<Result> toResult = another.get(toGets.get(to));
    assertThat(toResult).isPresent();
    assertThat(toResult.get().contains(BALANCE)).isTrue();
    assertThat(toResult.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE + amount);
    another.commit();
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void putAndCommit_GetsAndPutsForSameTableGiven_ShouldCommitProperly(Isolation isolation)
      throws TransactionException {
    putAndCommit_GetsAndPutsGiven_ShouldCommitProperly(
        isolation, namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void putAndCommit_GetsAndPutsForDifferentTablesGiven_ShouldCommitProperly(Isolation isolation)
      throws TransactionException {
    putAndCommit_GetsAndPutsGiven_ShouldCommitProperly(
        isolation, namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void commit_ConflictingPutsGivenForNonExisting_ShouldCommitOneAndAbortTheOther(
      Isolation isolation, String namespace1, String table1, String namespace2, String table2)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    boolean differentTables = !namespace1.equals(namespace2) || !table1.equals(table2);

    int expected = INITIAL_BALANCE;
    List<Put> puts1 = preparePuts(namespace1, table1);
    List<Put> puts2 = differentTables ? preparePuts(namespace2, table2) : puts1;
    List<Get> gets1 = prepareGets(namespace1, table1);
    List<Get> gets2 = differentTables ? prepareGets(namespace2, table2) : gets1;

    int from = 0;
    int to = NUM_TYPES;
    int anotherFrom = to;
    int anotherTo = NUM_TYPES * 2;
    puts1.set(from, Put.newBuilder(puts1.get(from)).intValue(BALANCE, expected).build());
    puts2.set(to, Put.newBuilder(puts2.get(to)).intValue(BALANCE, expected).build());

    DistributedTransaction transaction1 = manager.begin();
    transaction1.get(gets1.get(from));
    transaction1.get(gets2.get(to));
    transaction1.put(puts1.get(from));
    transaction1.put(puts2.get(to));

    DistributedTransaction transaction2 = manager.begin();
    puts1.set(anotherTo, Put.newBuilder(puts1.get(anotherTo)).intValue(BALANCE, expected).build());

    // Act Assert
    assertThatCode(
            () -> {
              transaction2.put(puts2.get(anotherFrom));
              transaction2.put(puts1.get(anotherTo));
              transaction2.commit();
            })
        .doesNotThrowAnyException();

    assertThatThrownBy(transaction1::commit).isInstanceOf(CommitException.class);

    // Assert
    verify(commit).rollbackRecords(any(TransactionContext.class));

    DistributedTransaction another = manager.beginReadOnly();
    assertThat(another.get(gets1.get(from)).isPresent()).isFalse();
    Optional<Result> toResult = another.get(gets2.get(to));
    assertThat(toResult).isPresent();
    assertThat(getBalance(toResult.get())).isEqualTo(expected);
    Optional<Result> anotherToResult = another.get(gets1.get(anotherTo));
    assertThat(anotherToResult).isPresent();
    assertThat(getBalance(anotherToResult.get())).isEqualTo(expected);
    another.commit();
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void commit_ConflictingPutsForSameTableGivenForNonExisting_ShouldCommitOneAndAbortTheOther(
      Isolation isolation) throws TransactionException {
    commit_ConflictingPutsGivenForNonExisting_ShouldCommitOneAndAbortTheOther(
        isolation, namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void commit_ConflictingPutsForDifferentTablesGivenForNonExisting_ShouldCommitOneAndAbortTheOther(
      Isolation isolation) throws TransactionException {
    commit_ConflictingPutsGivenForNonExisting_ShouldCommitOneAndAbortTheOther(
        isolation, namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void commit_ConflictingPutAndDeleteGivenForExisting_ShouldCommitPutAndAbortDelete(
      Isolation isolation, String namespace1, String table1, String namespace2, String table2)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    boolean differentTables = !namespace1.equals(namespace2) || !table1.equals(table2);

    int amount = 200;
    int from = 0;
    int to = NUM_TYPES;
    int anotherFrom = to;
    int anotherTo = NUM_TYPES * 2;

    populateRecords(manager, namespace1, table1);
    if (differentTables) {
      populateRecords(manager, namespace2, table2);
    }

    DistributedTransaction transaction = manager.begin();
    List<Get> gets1 = prepareGets(namespace1, table1);
    List<Delete> deletes1 = prepareDeletes(namespace1, table1);
    List<Get> gets2 = differentTables ? prepareGets(namespace2, table2) : gets1;
    List<Delete> deletes2 = differentTables ? prepareDeletes(namespace2, table2) : deletes1;
    transaction.get(gets1.get(from));
    transaction.delete(deletes1.get(from));
    transaction.get(gets2.get(to));
    transaction.delete(deletes2.get(to));

    // Act Assert
    assertThatCode(
            () ->
                prepareTransfer(
                        manager,
                        anotherFrom,
                        namespace2,
                        table2,
                        anotherTo,
                        namespace1,
                        table1,
                        amount)
                    .commit())
        .doesNotThrowAnyException();

    assertThatThrownBy(transaction::commit).isInstanceOf(CommitException.class);

    // Assert
    verify(commit).rollbackRecords(any(TransactionContext.class));
    DistributedTransaction another = manager.beginReadOnly();
    Optional<Result> fromResult = another.get(gets1.get(from));
    assertThat(fromResult).isPresent();
    assertThat(getBalance(fromResult.get())).isEqualTo(INITIAL_BALANCE);
    Optional<Result> toResult = another.get(gets2.get(to));
    assertThat(toResult).isPresent();
    assertThat(getBalance(toResult.get())).isEqualTo(INITIAL_BALANCE - amount);
    Optional<Result> anotherToResult = another.get(gets1.get(anotherTo));
    assertThat(anotherToResult).isPresent();
    assertThat(getBalance(anotherToResult.get())).isEqualTo(INITIAL_BALANCE + amount);
    another.commit();
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void commit_ConflictingPutAndDeleteForSameTableGivenForExisting_ShouldCommitPutAndAbortDelete(
      Isolation isolation) throws TransactionException {
    commit_ConflictingPutAndDeleteGivenForExisting_ShouldCommitPutAndAbortDelete(
        isolation, namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void
      commit_ConflictingPutAndDeleteForDifferentTableGivenForExisting_ShouldCommitPutAndAbortDelete(
          Isolation isolation) throws TransactionException {
    commit_ConflictingPutAndDeleteGivenForExisting_ShouldCommitPutAndAbortDelete(
        isolation, namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void commit_ConflictingPutsGivenForExisting_ShouldCommitOneAndAbortTheOther(
      Isolation isolation, String namespace1, String table1, String namespace2, String table2)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    boolean differentTables = !namespace1.equals(namespace2) || !table1.equals(table2);

    int amount1 = 100;
    int amount2 = 200;
    int from = 0;
    int to = NUM_TYPES;
    int anotherFrom = to;
    int anotherTo = NUM_TYPES * 2;

    populateRecords(manager, namespace1, table1);
    if (differentTables) {
      populateRecords(manager, namespace2, table2);
    }

    DistributedTransaction transaction =
        prepareTransfer(manager, from, namespace1, table1, to, namespace2, table2, amount1);

    // Act Assert
    assertThatCode(
            () ->
                prepareTransfer(
                        manager,
                        anotherFrom,
                        namespace2,
                        table2,
                        anotherTo,
                        namespace1,
                        table1,
                        amount2)
                    .commit())
        .doesNotThrowAnyException();

    assertThatThrownBy(transaction::commit).isInstanceOf(CommitException.class);

    // Assert
    verify(commit).rollbackRecords(any(TransactionContext.class));
    List<Get> gets1 = prepareGets(namespace1, table1);
    List<Get> gets2 = prepareGets(namespace2, table2);

    DistributedTransaction another = manager.beginReadOnly();
    Optional<Result> fromResult = another.get(gets1.get(from));
    assertThat(fromResult).isPresent();
    assertThat(getBalance(fromResult.get())).isEqualTo(INITIAL_BALANCE);
    Optional<Result> toResult = another.get(gets2.get(to));
    assertThat(toResult).isPresent();
    assertThat(getBalance(toResult.get())).isEqualTo(INITIAL_BALANCE - amount2);
    Optional<Result> anotherToResult = another.get(gets1.get(anotherTo));
    assertThat(anotherToResult).isPresent();
    assertThat(getBalance(anotherToResult.get())).isEqualTo(INITIAL_BALANCE + amount2);
    another.commit();
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void commit_ConflictingPutsForSameTableGivenForExisting_ShouldCommitOneAndAbortTheOther(
      Isolation isolation) throws TransactionException {
    commit_ConflictingPutsGivenForExisting_ShouldCommitOneAndAbortTheOther(
        isolation, namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void commit_ConflictingPutsForDifferentTablesGivenForExisting_ShouldCommitOneAndAbortTheOther(
      Isolation isolation) throws TransactionException {
    commit_ConflictingPutsGivenForExisting_ShouldCommitOneAndAbortTheOther(
        isolation, namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void commit_NonConflictingPutsGivenForExisting_ShouldCommitBoth(
      Isolation isolation, String namespace1, String table1, String namespace2, String table2)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    boolean differentTables = !namespace1.equals(namespace2) || !table1.equals(table2);

    int amount1 = 100;
    int amount2 = 200;
    int from = 0;
    int to = NUM_TYPES;
    int anotherFrom = NUM_TYPES * 2;
    int anotherTo = NUM_TYPES * 3;

    populateRecords(manager, namespace1, table1);
    if (differentTables) {
      populateRecords(manager, namespace2, table2);
    }

    DistributedTransaction transaction =
        prepareTransfer(manager, from, namespace1, table1, to, namespace2, table2, amount1);

    // Act Assert
    assertThatCode(
            () ->
                prepareTransfer(
                        manager,
                        anotherFrom,
                        namespace2,
                        table2,
                        anotherTo,
                        namespace1,
                        table1,
                        amount2)
                    .commit())
        .doesNotThrowAnyException();

    assertThatCode(transaction::commit).doesNotThrowAnyException();

    // Assert
    List<Get> gets1 = prepareGets(namespace1, table1);
    List<Get> gets2 = prepareGets(namespace2, table2);

    DistributedTransaction another = manager.beginReadOnly();
    Optional<Result> fromResult = another.get(gets1.get(from));
    assertThat(fromResult).isPresent();
    assertThat(getBalance(fromResult.get())).isEqualTo(INITIAL_BALANCE - amount1);
    Optional<Result> toResult = another.get(gets2.get(to));
    assertThat(toResult).isPresent();
    assertThat(getBalance(toResult.get())).isEqualTo(INITIAL_BALANCE + amount1);
    Optional<Result> anotherFromResult = another.get(gets2.get(anotherFrom));
    assertThat(anotherFromResult).isPresent();
    assertThat(getBalance(anotherFromResult.get())).isEqualTo(INITIAL_BALANCE - amount2);
    Optional<Result> anotherToResult = another.get(gets1.get(anotherTo));
    assertThat(anotherToResult).isPresent();
    assertThat(getBalance(anotherToResult.get())).isEqualTo(INITIAL_BALANCE + amount2);
    another.commit();
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void commit_NonConflictingPutsForSameTableGivenForExisting_ShouldCommitBoth(Isolation isolation)
      throws TransactionException {
    commit_NonConflictingPutsGivenForExisting_ShouldCommitBoth(
        isolation, namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void commit_NonConflictingPutsForDifferentTablesGivenForExisting_ShouldCommitBoth(
      Isolation isolation) throws TransactionException {
    commit_NonConflictingPutsGivenForExisting_ShouldCommitBoth(
        isolation, namespace1, TABLE_1, namespace2, TABLE_2);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void putAndCommit_GetsAndPutsForSameKeyButDifferentTablesGiven_ShouldCommitBoth(
      Isolation isolation) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    int expected = INITIAL_BALANCE;
    List<Put> puts1 = preparePuts(namespace1, TABLE_1);
    List<Put> puts2 = preparePuts(namespace2, TABLE_2);

    int from = 0;
    int to = NUM_TYPES;
    int anotherFrom = from;
    int anotherTo = to;
    puts1.set(from, Put.newBuilder(puts1.get(from)).intValue(BALANCE, expected).build());
    puts1.set(to, Put.newBuilder(puts1.get(to)).intValue(BALANCE, expected).build());

    DistributedTransaction transaction1 = manager.begin();
    transaction1.put(puts1.get(from));
    transaction1.put(puts1.get(to));

    DistributedTransaction transaction2 = manager.begin();
    puts2.set(from, Put.newBuilder(puts2.get(from)).intValue(BALANCE, expected).build());
    puts2.set(to, Put.newBuilder(puts2.get(to)).intValue(BALANCE, expected).build());

    // Act Assert
    assertThatCode(
            () -> {
              transaction2.put(puts2.get(anotherFrom));
              transaction2.put(puts2.get(anotherTo));
              transaction2.commit();
            })
        .doesNotThrowAnyException();

    assertThatCode(transaction1::commit).doesNotThrowAnyException();

    // Assert
    List<Get> gets1 = prepareGets(namespace1, TABLE_1);
    List<Get> gets2 = prepareGets(namespace2, TABLE_2);
    DistributedTransaction another = manager.beginReadOnly();
    Optional<Result> fromResult = another.get(gets1.get(from));
    assertThat(fromResult).isPresent();
    assertThat(getBalance(fromResult.get())).isEqualTo(expected);
    Optional<Result> toResult = another.get(gets1.get(to));
    assertThat(toResult).isPresent();
    assertThat(getBalance(toResult.get())).isEqualTo(expected);
    Optional<Result> anotherFromResult = another.get(gets2.get(anotherFrom));
    assertThat(anotherFromResult).isPresent();
    assertThat(getBalance(anotherFromResult.get())).isEqualTo(expected);
    Optional<Result> anotherToResult = another.get(gets2.get(anotherTo));
    assertThat(anotherToResult).isPresent();
    assertThat(getBalance(anotherToResult.get())).isEqualTo(expected);
    another.commit();
  }

  private void commit_ConflictingDeletesGivenForExisting_ShouldCommitOneAndAbortTheOther(
      Isolation isolation, String namespace1, String table1, String namespace2, String table2)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    boolean differentTables = !namespace1.equals(namespace2) || !table1.equals(table2);

    int account1 = 0;
    int account2 = NUM_TYPES;
    int account3 = NUM_TYPES * 2;

    populateRecords(manager, namespace1, table1);
    if (differentTables) {
      populateRecords(manager, namespace2, table2);
    }

    DistributedTransaction transaction =
        prepareDeletes(manager, account1, namespace1, table1, account2, namespace2, table2);

    // Act
    assertThatCode(
            () ->
                prepareDeletes(manager, account2, namespace2, table2, account3, namespace1, table1)
                    .commit())
        .doesNotThrowAnyException();

    assertThatThrownBy(transaction::commit).isInstanceOf(CommitException.class);

    // Assert
    verify(commit).rollbackRecords(any(TransactionContext.class));
    List<Get> gets1 = prepareGets(namespace1, table1);
    List<Get> gets2 = differentTables ? prepareGets(namespace2, table2) : gets1;

    DistributedTransaction another = manager.beginReadOnly();
    Optional<Result> result = another.get(gets1.get(account1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE);
    assertThat(another.get(gets2.get(account2)).isPresent()).isFalse();
    assertThat(another.get(gets1.get(account3)).isPresent()).isFalse();
    another.commit();
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void commit_ConflictingDeletesForSameTableGivenForExisting_ShouldCommitOneAndAbortTheOther(
      Isolation isolation) throws TransactionException {
    commit_ConflictingDeletesGivenForExisting_ShouldCommitOneAndAbortTheOther(
        isolation, namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void commit_ConflictingDeletesForDifferentTablesGivenForExisting_ShouldCommitOneAndAbortTheOther(
      Isolation isolation) throws TransactionException {
    commit_ConflictingDeletesGivenForExisting_ShouldCommitOneAndAbortTheOther(
        isolation, namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void commit_NonConflictingDeletesGivenForExisting_ShouldCommitBoth(
      Isolation isolation, String namespace1, String table1, String namespace2, String table2)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    boolean differentTables = !namespace1.equals(namespace2) || !table1.equals(table2);

    int account1 = 0;
    int account2 = NUM_TYPES;
    int account3 = NUM_TYPES * 2;
    int account4 = NUM_TYPES * 3;

    populateRecords(manager, namespace1, table1);
    if (differentTables) {
      populateRecords(manager, namespace2, table2);
    }

    DistributedTransaction transaction =
        prepareDeletes(manager, account1, namespace1, table1, account2, namespace2, table2);

    // Act
    assertThatCode(
            () ->
                prepareDeletes(manager, account3, namespace2, table2, account4, namespace1, table1)
                    .commit())
        .doesNotThrowAnyException();

    assertThatCode(transaction::commit).doesNotThrowAnyException();

    // Assert
    List<Get> gets1 = prepareGets(namespace1, table1);
    List<Get> gets2 = differentTables ? prepareGets(namespace2, table2) : gets1;
    DistributedTransaction another = manager.beginReadOnly();
    assertThat(another.get(gets1.get(account1)).isPresent()).isFalse();
    assertThat(another.get(gets2.get(account2)).isPresent()).isFalse();
    assertThat(another.get(gets2.get(account3)).isPresent()).isFalse();
    assertThat(another.get(gets1.get(account4)).isPresent()).isFalse();
    another.commit();
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void commit_NonConflictingDeletesForSameTableGivenForExisting_ShouldCommitBoth(
      Isolation isolation) throws TransactionException {
    commit_NonConflictingDeletesGivenForExisting_ShouldCommitBoth(
        isolation, namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void commit_NonConflictingDeletesForDifferentTablesGivenForExisting_ShouldCommitBoth(
      Isolation isolation) throws TransactionException {
    commit_NonConflictingDeletesGivenForExisting_ShouldCommitBoth(
        isolation, namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void commit_WriteSkewOnExistingRecords_ShouldBehaveCorrectly(
      Isolation isolation, String namespace1, String table1, String namespace2, String table2)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    List<Put> puts =
        Arrays.asList(
            Put.newBuilder(preparePut(0, 0, namespace1, table1)).intValue(BALANCE, 1).build(),
            Put.newBuilder(preparePut(0, 1, namespace2, table2)).intValue(BALANCE, 1).build());
    DistributedTransaction transaction = manager.begin();
    transaction.put(puts);
    transaction.commit();

    // Act Assert
    DistributedTransaction transaction1 = manager.begin();
    DistributedTransaction transaction2 = manager.begin();

    Get get1_1 = prepareGet(0, 1, namespace2, table2);
    Optional<Result> result1 = transaction1.get(get1_1);
    assertThat(result1).isPresent();
    int current1 = getBalance(result1.get());
    Get get1_2 = prepareGet(0, 0, namespace1, table1);
    transaction1.get(get1_2);
    Get get2_1 = prepareGet(0, 0, namespace1, table1);
    Optional<Result> result2 = transaction2.get(get2_1);
    assertThat(result2).isPresent();
    int current2 = getBalance(result2.get());
    Get get2_2 = prepareGet(0, 1, namespace2, table2);
    transaction2.get(get2_2);
    Put put1 =
        Put.newBuilder(preparePut(0, 0, namespace1, table1))
            .intValue(BALANCE, current1 + 1)
            .build();
    transaction1.put(put1);
    Put put2 =
        Put.newBuilder(preparePut(0, 1, namespace2, table2))
            .intValue(BALANCE, current2 + 1)
            .build();
    transaction2.put(put2);
    transaction1.commit();

    if (isolation == Isolation.SERIALIZABLE) {
      // In SERIALIZABLE isolation, one transaction should commit and the other should throw
      // CommitConflictException

      Throwable thrown = catchThrowable(transaction2::commit);
      assertThat(thrown).isInstanceOf(CommitConflictException.class);

      transaction = manager.beginReadOnly();
      result1 = transaction.get(get1_1);
      assertThat(result1).isPresent();
      assertThat(getBalance(result1.get())).isEqualTo(1);
      result2 = transaction.get(get2_1);
      assertThat(result2).isPresent();
      assertThat(getBalance(result2.get())).isEqualTo(2);
      transaction.commit();
    } else {
      assert isolation == Isolation.READ_COMMITTED || isolation == Isolation.SNAPSHOT;

      // In READ_COMMITTED or SNAPSHOT isolation, both transactions should commit successfully

      transaction2.commit();

      transaction = manager.beginReadOnly();
      // The results can not be produced by executing the transactions serially
      result1 = transaction.get(get1_1);
      assertThat(result1).isPresent();
      assertThat(getBalance(result1.get())).isEqualTo(2);
      result2 = transaction.get(get2_1);
      assertThat(result2).isPresent();
      assertThat(getBalance(result2.get())).isEqualTo(2);
      transaction.commit();
    }
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void commit_WriteSkewOnExistingRecordsInSameTable_ShouldBehaveCorrectly(Isolation isolation)
      throws TransactionException {
    commit_WriteSkewOnExistingRecords_ShouldBehaveCorrectly(
        isolation, namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void commit_WriteSkewOnExistingRecordsInDifferentTables_ShouldBehaveCorrectly(Isolation isolation)
      throws TransactionException {
    commit_WriteSkewOnExistingRecords_ShouldBehaveCorrectly(
        isolation, namespace1, TABLE_1, namespace2, TABLE_2);
  }

  private void commit_WriteSkewOnNonExistingRecords_ShouldBehaveCorrectly(
      Isolation isolation, String namespace1, String table1, String namespace2, String table2)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);

    // no records

    // Act
    DistributedTransaction transaction1 = manager.begin();
    DistributedTransaction transaction2 = manager.begin();
    Get get1_1 = prepareGet(0, 1, namespace2, table2);
    Optional<Result> result1 = transaction1.get(get1_1);
    Get get1_2 = prepareGet(0, 0, namespace1, table1);
    transaction1.get(get1_2);
    int current1 = 0;
    Get get2_1 = prepareGet(0, 0, namespace1, table1);
    Optional<Result> result2 = transaction2.get(get2_1);
    Get get2_2 = prepareGet(0, 1, namespace2, table2);
    transaction2.get(get2_2);
    int current2 = 0;
    Put put1 =
        Put.newBuilder(preparePut(0, 0, namespace1, table1))
            .intValue(BALANCE, current1 + 1)
            .build();
    transaction1.put(put1);
    Put put2 =
        Put.newBuilder(preparePut(0, 1, namespace2, table2))
            .intValue(BALANCE, current2 + 1)
            .build();
    transaction2.put(put2);
    Throwable thrown1 = catchThrowable(transaction1::commit);
    Throwable thrown2 = catchThrowable(transaction2::commit);

    // Assert
    assertThat(result1.isPresent()).isFalse();
    assertThat(result2.isPresent()).isFalse();

    if (isolation == Isolation.SERIALIZABLE) {
      // In SERIALIZABLE isolation, one transaction should commit and the other should throw
      // CommitConflictException

      DistributedTransaction transaction = manager.beginReadOnly();
      result1 = transaction.get(get1_1);
      assertThat(result1.isPresent()).isFalse();
      result2 = transaction.get(get2_1);
      assertThat(result2.isPresent()).isTrue();
      assertThat(getBalance(result2.get())).isEqualTo(1);
      transaction.commit();

      assertThat(thrown1).doesNotThrowAnyException();
      assertThat(thrown2).isInstanceOf(CommitConflictException.class);
    } else {
      assert isolation == Isolation.READ_COMMITTED || isolation == Isolation.SNAPSHOT;

      // In READ_COMMITTED or SNAPSHOT isolation, both transactions should commit successfully

      DistributedTransaction transaction = manager.beginReadOnly();
      // The results can not be produced by executing the transactions serially
      result1 = transaction.get(get1_1);
      assertThat(result1.isPresent()).isTrue();
      assertThat(getBalance(result1.get())).isEqualTo(1);
      result2 = transaction.get(get2_1);
      assertThat(result2.isPresent()).isTrue();
      assertThat(getBalance(result2.get())).isEqualTo(1);
      transaction.commit();

      assertThat(thrown1).doesNotThrowAnyException();
      assertThat(thrown2).doesNotThrowAnyException();
    }
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void commit_WriteSkewOnNonExistingRecordsInSameTable_ShouldBehaveCorrectly(Isolation isolation)
      throws TransactionException {
    commit_WriteSkewOnNonExistingRecords_ShouldBehaveCorrectly(
        isolation, namespace1, TABLE_1, namespace1, TABLE_1);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void commit_WriteSkewOnNonExistingRecordsInDifferentTables_ShouldBehaveCorrectly(
      Isolation isolation) throws TransactionException {
    commit_WriteSkewOnNonExistingRecords_ShouldBehaveCorrectly(
        isolation, namespace1, TABLE_1, namespace2, TABLE_2);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void commit_WriteSkewWithScanOnExistingRecords_ShouldBehaveCorrectly(Isolation isolation)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    List<Put> puts =
        Arrays.asList(
            Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, 1).build(),
            Put.newBuilder(preparePut(0, 1, namespace1, TABLE_1)).intValue(BALANCE, 1).build());
    DistributedTransaction transaction = manager.begin();
    transaction.put(puts);
    transaction.commit();

    // Act
    DistributedTransaction transaction1 = manager.begin();
    DistributedTransaction transaction2 = manager.begin();
    List<Result> results1 = transaction1.scan(prepareScan(0, 0, 1, namespace1, TABLE_1));
    int count1 = results1.size();
    List<Result> results2 = transaction2.scan(prepareScan(0, 0, 1, namespace1, TABLE_1));
    int count2 = results2.size();
    Put put1 =
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, count1 + 1).build();
    transaction1.put(put1);
    Put put2 =
        Put.newBuilder(preparePut(0, 1, namespace1, TABLE_1)).intValue(BALANCE, count2 + 1).build();
    transaction2.put(put2);
    Throwable thrown1 = catchThrowable(transaction1::commit);
    Throwable thrown2 = catchThrowable(transaction2::commit);

    // Assert
    if (isolation == Isolation.SERIALIZABLE) {
      // In SERIALIZABLE isolation, one transaction should commit and the other should throw
      // CommitConflictException

      transaction = manager.beginReadOnly();
      Optional<Result> result1 = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
      assertThat(result1).isPresent();
      assertThat(getBalance(result1.get())).isEqualTo(3);
      Optional<Result> result2 = transaction.get(prepareGet(0, 1, namespace1, TABLE_1));
      assertThat(result2).isPresent();
      assertThat(getBalance(result2.get())).isEqualTo(1);
      transaction.commit();

      assertThat(thrown1).doesNotThrowAnyException();
      assertThat(thrown2).isInstanceOf(CommitConflictException.class);
    } else {
      assert isolation == Isolation.READ_COMMITTED || isolation == Isolation.SNAPSHOT;

      // In READ_COMMITTED or SNAPSHOT isolation, both transactions should commit successfully

      transaction = manager.beginReadOnly();
      // The results can not be produced by executing the transactions serially
      Optional<Result> result1 = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
      assertThat(result1).isPresent();
      assertThat(getBalance(result1.get())).isEqualTo(3);
      Optional<Result> result2 = transaction.get(prepareGet(0, 1, namespace1, TABLE_1));
      assertThat(result2).isPresent();
      assertThat(getBalance(result2.get())).isEqualTo(3);
      transaction.commit();

      assertThat(thrown1).doesNotThrowAnyException();
      assertThat(thrown2).doesNotThrowAnyException();
    }
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void commit_WriteSkewWithScanOnNonExistingRecords_ShouldBehaveCorrectly(Isolation isolation)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);

    // no records

    // Act
    DistributedTransaction transaction1 = manager.begin();
    DistributedTransaction transaction2 = manager.begin();
    List<Result> results1 = transaction1.scan(prepareScan(0, 0, 1, namespace1, TABLE_1));
    int count1 = results1.size();
    List<Result> results2 = transaction2.scan(prepareScan(0, 0, 1, namespace1, TABLE_1));
    int count2 = results2.size();
    Put put1 =
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, count1 + 1).build();
    transaction1.put(put1);
    Put put2 =
        Put.newBuilder(preparePut(0, 1, namespace1, TABLE_1)).intValue(BALANCE, count2 + 1).build();
    transaction2.put(put2);
    Throwable thrown1 = catchThrowable(transaction1::commit);
    Throwable thrown2 = catchThrowable(transaction2::commit);

    // Assert
    assertThat(results1).isEmpty();
    assertThat(results2).isEmpty();

    if (isolation == Isolation.SERIALIZABLE) {
      // In SERIALIZABLE isolation, one transaction should commit and the other should throw
      // CommitConflictException

      DistributedTransaction transaction = manager.beginReadOnly();
      Optional<Result> result1 = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
      assertThat(result1.isPresent()).isTrue();
      assertThat(getBalance(result1.get())).isEqualTo(1);
      Optional<Result> result2 = transaction.get(prepareGet(0, 1, namespace1, TABLE_1));
      assertThat(result2.isPresent()).isFalse();
      transaction.commit();

      assertThat(thrown1).doesNotThrowAnyException();
      assertThat(thrown2).isInstanceOf(CommitConflictException.class);
    } else {
      assert isolation == Isolation.READ_COMMITTED || isolation == Isolation.SNAPSHOT;

      // In READ_COMMITTED or SNAPSHOT isolation, both transactions should commit successfully

      DistributedTransaction transaction = manager.beginReadOnly();
      // The results can not be produced by executing the transactions serially
      Optional<Result> result1 = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
      assertThat(result1.isPresent()).isTrue();
      assertThat(getBalance(result1.get())).isEqualTo(1);
      Optional<Result> result2 = transaction.get(prepareGet(0, 1, namespace1, TABLE_1));
      assertThat(result2.isPresent()).isTrue();
      assertThat(getBalance(result2.get())).isEqualTo(1);
      transaction.commit();

      assertThat(thrown1).doesNotThrowAnyException();
      assertThat(thrown2).doesNotThrowAnyException();
    }
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void commit_DeleteGivenWithoutRead_ShouldNotThrowAnyExceptions(Isolation isolation)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    Delete delete = prepareDelete(0, 0, namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();

    // Act Assert
    transaction.delete(delete);
    assertThatCode(transaction::commit).doesNotThrowAnyException();
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void commit_DeleteGivenForNonExisting_ShouldNotThrowAnyExceptions(Isolation isolation)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    Delete delete = prepareDelete(0, 0, namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();

    // Act Assert
    transaction.get(get);
    transaction.delete(delete);
    assertThatCode(transaction::commit).doesNotThrowAnyException();
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void commit_DeleteGivenForExistingAfterRead_ShouldDeleteRecord(Isolation isolation)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecord(manager, namespace1, TABLE_1);
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    Delete delete = prepareDelete(0, 0, namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.delete(delete);
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isTrue();
    DistributedTransaction another = manager.beginReadOnly();
    assertThat(another.get(get).isPresent()).isFalse();
    another.commit();
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void putAndCommit_DeleteGivenInBetweenTransactions_ShouldProduceSerializableResults(
      Isolation isolation) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    DistributedTransaction transaction = manager.begin();
    transaction.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, 2).build());
    transaction.commit();

    // Act
    DistributedTransaction transaction1 = manager.begin();
    Optional<Result> result1 = transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));
    int balance1 = 0;
    if (result1.isPresent()) {
      balance1 = getBalance(result1.get());
    }
    transaction1.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1))
            .intValue(BALANCE, balance1 + 1)
            .build());

    DistributedTransaction transaction2 = manager.begin();
    transaction2.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction2.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    transaction2.commit();

    // the same transaction processing as transaction1
    DistributedTransaction transaction3 = manager.begin();
    Optional<Result> result3 = transaction3.get(prepareGet(0, 0, namespace1, TABLE_1));
    int balance3 = 0;
    if (result3.isPresent()) {
      balance3 = getBalance(result3.get());
    }
    transaction3.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1))
            .intValue(BALANCE, balance3 + 1)
            .build());
    transaction3.commit();

    Throwable thrown = catchThrowable(transaction1::commit);

    // Assert
    assertThat(thrown).isInstanceOf(CommitConflictException.class);
    transaction = manager.beginReadOnly();
    Optional<Result> result = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction.commit();
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(1);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void deleteAndCommit_DeleteGivenInBetweenTransactions_ShouldProduceSerializableResults(
      Isolation isolation) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    DistributedTransaction transaction = manager.begin();
    transaction.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, 2).build());
    transaction.commit();

    // Act
    DistributedTransaction transaction1 = manager.begin();
    transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction1.delete(prepareDelete(0, 0, namespace1, TABLE_1));

    DistributedTransaction transaction2 = manager.begin();
    transaction2.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction2.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    transaction2.commit();

    // the same transaction processing as transaction1
    DistributedTransaction transaction3 = manager.begin();
    Optional<Result> result3 = transaction3.get(prepareGet(0, 0, namespace1, TABLE_1));
    int balance3 = 0;
    if (result3.isPresent()) {
      balance3 = getBalance(result3.get());
    }
    transaction3.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1))
            .intValue(BALANCE, balance3 + 1)
            .build());
    transaction3.commit();

    Throwable thrown = catchThrowable(transaction1::commit);

    // Assert
    assertThat(thrown).isInstanceOf(CommitConflictException.class);
    transaction = manager.beginReadOnly();
    Optional<Result> result = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction.commit();
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(1);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void get_PutThenGetWithoutConjunctionReturnEmptyFromStorage_ShouldReturnResult(
      Isolation isolation) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    DistributedTransaction transaction = manager.begin();

    // Act
    transaction.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, 1).build());
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    Optional<Result> result = transaction.get(get);
    assertThatCode(transaction::commit).doesNotThrowAnyException();

    // Assert
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(1);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void get_PutThenGetWithConjunctionReturnEmptyFromStorageAndMatchedWithPut_ShouldReturnResult(
      Isolation isolation) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    DistributedTransaction transaction = manager.begin();

    // Act
    transaction.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, 1).build());
    Get get =
        Get.newBuilder(prepareGet(0, 0, namespace1, TABLE_1))
            .where(column(BALANCE).isEqualToInt(1))
            .build();
    Optional<Result> result = transaction.get(get);
    assertThatCode(transaction::commit).doesNotThrowAnyException();

    // Assert
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(1);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void get_PutThenGetWithConjunctionReturnEmptyFromStorageAndUnmatchedWithPut_ShouldReturnEmpty(
      Isolation isolation) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    DistributedTransaction transaction = manager.begin();

    // Act
    transaction.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, 1).build());
    Get get =
        Get.newBuilder(prepareGet(0, 0, namespace1, TABLE_1))
            .where(column(BALANCE).isEqualToInt(0))
            .build();
    Optional<Result> result = transaction.get(get);
    assertThatCode(transaction::commit).doesNotThrowAnyException();

    // Assert
    assertThat(result).isNotPresent();
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void get_PutThenGetWithConjunctionReturnResultFromStorageAndMatchedWithPut_ShouldReturnResult(
      Isolation isolation) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecord(manager, namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();
    Put put = Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, 1).build();
    Get get =
        Get.newBuilder(prepareGet(0, 0, namespace1, TABLE_1))
            .where(column(BALANCE).isLessThanOrEqualToInt(INITIAL_BALANCE))
            .build();

    // Act
    transaction.put(put);
    Optional<Result> result = transaction.get(get);
    assertThatCode(transaction::commit).doesNotThrowAnyException();

    // Assert
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(1);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void get_PutThenGetWithConjunctionReturnResultFromStorageButUnmatchedWithPut_ShouldReturnEmpty(
      Isolation isolation) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecord(manager, namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();
    Put put = Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, 1).build();
    Get get =
        Get.newBuilder(prepareGet(0, 0, namespace1, TABLE_1))
            .where(column(BALANCE).isEqualToInt(0))
            .build();

    // Act
    transaction.put(put);
    Optional<Result> result = transaction.get(get);
    assertThat(catchThrowable(transaction::commit)).isInstanceOf(CommitConflictException.class);
    transaction.rollback();

    // Assert
    assertThat(result).isNotPresent();
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void get_DeleteCalledBefore_ShouldReturnEmpty(Isolation isolation) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    DistributedTransaction transaction = manager.begin();
    transaction.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, 1).build());
    transaction.commit();

    // Act
    DistributedTransaction transaction1 = manager.begin();
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    Optional<Result> resultBefore = transaction1.get(get);
    transaction1.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    Optional<Result> resultAfter = transaction1.get(get);
    assertThatCode(transaction1::commit).doesNotThrowAnyException();

    // Assert
    assertThat(resultBefore.isPresent()).isTrue();
    assertThat(resultAfter.isPresent()).isFalse();
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void delete_PutCalledBefore_ShouldDelete(Isolation isolation) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    DistributedTransaction transaction = manager.begin();
    transaction.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, 1).build());
    transaction.commit();

    // Act
    DistributedTransaction transaction1 = manager.begin();
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    Optional<Result> resultBefore = transaction1.get(get);
    transaction1.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, 2).build());
    transaction1.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    assertThatCode(transaction1::commit).doesNotThrowAnyException();

    // Assert
    DistributedTransaction transaction2 = manager.beginReadOnly();
    Optional<Result> resultAfter = transaction2.get(get);
    transaction2.commit();
    assertThat(resultBefore.isPresent()).isTrue();
    assertThat(resultAfter.isPresent()).isFalse();
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void put_DeleteCalledBefore_ShouldThrowIllegalArgumentException(Isolation isolation)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    DistributedTransaction transaction = manager.begin();
    transaction.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, 1).build());
    transaction.commit();

    // Act
    DistributedTransaction transaction1 = manager.begin();
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    transaction1.get(get);
    transaction1.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    Throwable thrown =
        catchThrowable(
            () ->
                transaction1.put(
                    Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1))
                        .intValue(BALANCE, 2)
                        .build()));
    transaction1.rollback();

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void scan_OverlappingPutGivenBefore_ShouldThrowIllegalArgumentException(Isolation isolation)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    DistributedTransaction transaction = manager.begin();
    transaction.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, 1).build());

    // Act
    Scan scan = prepareScan(0, 0, 0, namespace1, TABLE_1);
    Throwable thrown = catchThrowable(() -> transaction.scan(scan));
    transaction.rollback();

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void scan_NonOverlappingPutGivenBefore_ShouldScan(Isolation isolation)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    DistributedTransaction transaction = manager.begin();
    transaction.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, 1).build());

    // Act
    Scan scan = prepareScan(0, 1, 1, namespace1, TABLE_1);
    Throwable thrown = catchThrowable(() -> transaction.scan(scan));
    transaction.commit();

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void scan_PutWithOverlappedClusteringKeyAndNonOverlappedConjunctionsGivenBefore_ShouldScan(
      Isolation isolation) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    DistributedTransaction transaction = manager.begin();
    transaction.put(
        Put.newBuilder(preparePut(0, 1, namespace1, TABLE_1)).intValue(BALANCE, 1).build());
    Scan scan =
        Scan.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .start(Key.ofInt(ACCOUNT_TYPE, 0))
            .where(column(BALANCE).isNotEqualToInt(1))
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> transaction.scan(scan));
    transaction.commit();

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void scan_NonOverlappingPutGivenButOverlappingPutExists_ShouldThrowIllegalArgumentException(
      Isolation isolation) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecords(manager, namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();
    transaction.put(
        Put.newBuilder(preparePut(0, 1, namespace1, TABLE_1)).intValue(BALANCE, 9999).build());
    Scan scan =
        Scan.newBuilder(prepareScan(0, 1, 1, namespace1, TABLE_1))
            .where(column(BALANCE).isLessThanOrEqualToInt(INITIAL_BALANCE))
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> transaction.scan(scan));
    transaction.rollback();

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void scan_OverlappingPutWithConjunctionsGivenBefore_ShouldThrowIllegalArgumentException(
      Isolation isolation) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    DistributedTransaction transaction = manager.begin();
    transaction.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1))
            .intValue(BALANCE, 999)
            .textValue(SOME_COLUMN, "aaa")
            .build());
    Scan scan =
        Scan.newBuilder(prepareScan(0, namespace1, TABLE_1))
            .where(column(BALANCE).isLessThanInt(1000))
            .and(column(SOME_COLUMN).isEqualToText("aaa"))
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> transaction.scan(scan));
    transaction.rollback();

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  public void
      scanWithIndex_PutWithOverlappedIndexKeyAndNonOverlappedConjunctionsGivenBefore_ShouldScan(
          Isolation isolation) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    DistributedTransaction transaction = manager.begin();
    transaction.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1))
            .intValue(BALANCE, 1)
            .textValue(SOME_COLUMN, "aaa")
            .build());
    Scan scan =
        Scan.newBuilder(prepareScanWithIndex(namespace1, TABLE_1, 1))
            .where(column(SOME_COLUMN).isGreaterThanText("aaa"))
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> transaction.scan(scan));
    transaction.commit();

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  public void
      scanWithIndex_OverlappingPutWithNonIndexedColumnGivenBefore_ShouldThrowIllegalArgumentException(
          Isolation isolation) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecord(manager, namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();
    transaction.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1))
            .textValue(SOME_COLUMN, "aaa")
            .build());

    // Act
    Scan scan = prepareScanWithIndex(namespace1, TABLE_1, INITIAL_BALANCE);
    Throwable thrown = catchThrowable(() -> transaction.scan(scan));
    transaction.rollback();

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  public void
      scanWithIndex_NonOverlappingPutWithIndexedColumnGivenBefore_ShouldThrowIllegalArgumentException(
          Isolation isolation) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecord(manager, namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();
    transaction.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, 999).build());

    // Act
    Scan scan = prepareScanWithIndex(namespace1, TABLE_1, INITIAL_BALANCE);
    Throwable thrown = catchThrowable(() -> transaction.scan(scan));
    transaction.rollback();

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  public void
      scanWithIndex_OverlappingPutWithIndexedColumnGivenBefore_ShouldThrowIllegalArgumentException(
          Isolation isolation) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecord(manager, namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();
    transaction.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, 999).build());

    // Act
    Scan scan = prepareScanWithIndex(namespace1, TABLE_1, 999);
    Throwable thrown = catchThrowable(() -> transaction.scan(scan));
    transaction.rollback();

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  public void
      scanWithIndex_OverlappingPutWithIndexedColumnAndConjunctionsGivenBefore_ShouldThrowIllegalArgumentException(
          Isolation isolation) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    DistributedTransaction transaction = manager.begin();
    transaction.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1))
            .intValue(BALANCE, 999)
            .textValue(SOME_COLUMN, "aaa")
            .build());
    Scan scan =
        Scan.newBuilder(prepareScanWithIndex(namespace1, TABLE_1, 999))
            .where(column(BALANCE).isLessThanInt(1000))
            .and(column(SOME_COLUMN).isEqualToText("aaa"))
            .build();

    // Act
    Throwable thrown = catchThrowable(() -> transaction.scan(scan));
    transaction.rollback();

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void scan_DeleteGivenBefore_ShouldThrowIllegalArgumentException(Isolation isolation)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    DistributedTransaction transaction = manager.begin();
    transaction.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, 1).build());
    transaction.put(
        Put.newBuilder(preparePut(0, 1, namespace1, TABLE_1)).intValue(BALANCE, 1).build());
    transaction.commit();

    // Act Assert
    DistributedTransaction transaction1 = manager.begin();
    transaction1.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    Scan scan = prepareScan(0, 0, 1, namespace1, TABLE_1);
    assertThatThrownBy(() -> transaction1.scan(scan)).isInstanceOf(IllegalArgumentException.class);
    transaction1.rollback();
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void scanAll_DeleteGivenBefore_ShouldThrowIllegalArgumentException(Isolation isolation)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    DistributedTransaction transaction = manager.begin();
    transaction.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, 1).build());
    transaction.put(
        Put.newBuilder(preparePut(0, 1, namespace1, TABLE_1)).intValue(BALANCE, 1).build());
    transaction.commit();

    // Act Assert
    DistributedTransaction transaction1 = manager.begin();
    transaction1.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    assertThatThrownBy(() -> transaction1.scan(scanAll))
        .isInstanceOf(IllegalArgumentException.class);
    transaction1.rollback();
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void scanAll_NonOverlappingPutGivenBefore_ShouldScanAll(Isolation isolation)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    DistributedTransaction transaction = manager.begin();
    transaction.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, 1).build());

    // Act
    Scan scanAll = prepareScanAll(namespace2, TABLE_2);
    Throwable thrown = catchThrowable(() -> transaction.scan(scanAll));
    transaction.commit();

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void scanAll_OverlappingPutGivenBefore_ShouldThrowIllegalArgumentException(Isolation isolation)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    DistributedTransaction transaction = manager.begin();
    transaction.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, 1).build());

    // Act
    Scan scanAll = prepareScanAll(namespace1, TABLE_1);
    Throwable thrown = catchThrowable(() -> transaction.scan(scanAll));
    transaction.rollback();

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void scanAll_ScanAllGivenForCommittedRecord_ShouldReturnRecord(
      Isolation isolation, boolean readOnly) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecords(manager, namespace1, TABLE_1);
    DistributedTransaction transaction = begin(manager, readOnly);
    Scan scanAll = Scan.newBuilder(prepareScanAll(namespace1, TABLE_1)).limit(1).build();

    // Act
    List<Result> results = transaction.scan(scanAll);
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(1);
    assertThat(
            ((TransactionResult) ((FilteredResult) results.get(0)).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void scanAll_ScanAllGivenForNonExisting_ShouldReturnEmpty(Isolation isolation, boolean readOnly)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    DistributedTransaction putTransaction = manager.begin();
    putTransaction.put(preparePut(0, 0, namespace1, TABLE_1));
    putTransaction.commit();

    DistributedTransaction transaction = begin(manager, readOnly);
    Scan scanAll = prepareScanAll(namespace2, TABLE_2);

    // Act
    List<Result> results = transaction.scan(scanAll);
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(0);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void get_GetWithMatchedConjunctionsGivenForCommittedRecord_ShouldReturnRecord(
      Isolation isolation, boolean readOnly) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecord(manager, namespace1, TABLE_1);
    DistributedTransaction transaction = begin(manager, readOnly);
    Get get =
        Get.newBuilder(prepareGet(0, 0, namespace1, TABLE_1))
            .where(column(BALANCE).isEqualToInt(INITIAL_BALANCE))
            .and(column(SOME_COLUMN).isNullText())
            .build();

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE);
    assertThat(result.get().getText(SOME_COLUMN)).isNull();
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void get_GetWithUnmatchedConjunctionsGivenForCommittedRecord_ShouldReturnEmpty(
      Isolation isolation, boolean readOnly) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecord(manager, namespace1, TABLE_1);
    DistributedTransaction transaction = begin(manager, readOnly);
    Get get =
        Get.newBuilder(prepareGet(0, 0, namespace1, TABLE_1))
            .where(column(BALANCE).isEqualToInt(INITIAL_BALANCE))
            .and(column(SOME_COLUMN).isEqualToText("aaa"))
            .build();

    // Act
    Optional<Result> result = transaction.get(get);
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isFalse();
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void scan_CalledTwiceWithSameConditionsAndUpdateForHappenedInBetween_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    DistributedTransaction transaction = manager.begin();
    transaction.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, 1).build());
    transaction.commit();

    // Act Assert
    DistributedTransaction transaction1 = begin(manager, readOnly);
    Scan scan =
        Scan.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .start(Key.ofInt(ACCOUNT_TYPE, 0))
            .where(column(BALANCE).isEqualToInt(1))
            .build();
    List<Result> result1 = transaction1.scan(scan);

    // The record is updated by another transaction
    manager.update(
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, 0)
            .build());

    List<Result> result2 = transaction1.scan(scan);

    if (isolation == Isolation.READ_COMMITTED) {
      // In READ_COMMITTED isolation

      transaction1.commit();

      // The first scan should return the record
      assertThat(result1.size()).isEqualTo(1);
      assertThat(result1.get(0).getInt(BALANCE)).isEqualTo(1);

      // The second scan should return empty as the record was updated
      assertThat(result2).isEmpty();
    } else if (isolation == Isolation.SNAPSHOT) {
      // In SNAPSHOT isolation

      transaction1.commit();

      // Both scans should return the same result
      assertThat(result1.size()).isEqualTo(1);
      assertThat(result2.size()).isEqualTo(1);
      assertThat(result1.get(0)).isEqualTo(result2.get(0));
    } else {
      assert isolation == Isolation.SERIALIZABLE;

      // In SERIALIZABLE isolation, an anti-dependency should be detected
      assertThatThrownBy(transaction1::commit).isInstanceOf(CommitConflictException.class);
    }
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void scan_CalledTwiceWithDifferentConditionsAndUpdateHappenedInBetween_ShouldBehaveCorrectly(
      Isolation isolation, boolean readOnly) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    DistributedTransaction transaction = manager.begin();
    transaction.put(
        Put.newBuilder(preparePut(0, 0, namespace1, TABLE_1)).intValue(BALANCE, 1).build());
    transaction.commit();

    DistributedTransaction transaction1 = begin(manager, readOnly);
    Scan scan1 =
        Scan.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .start(Key.ofInt(ACCOUNT_TYPE, 0))
            .where(column(BALANCE).isEqualToInt(1))
            .build();
    Scan scan2 =
        Scan.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .start(Key.ofInt(ACCOUNT_TYPE, 0))
            .where(column(BALANCE).isGreaterThanInt(1))
            .build();

    // Act Assert
    List<Result> result1 = transaction1.scan(scan1);

    // The record is updated by another transaction
    manager.update(
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, 2)
            .build());

    List<Result> result2 = transaction1.scan(scan2);

    if (isolation == Isolation.SERIALIZABLE) {
      // In SERIALIZABLE isolation, an anti-dependency should be detected
      assertThatThrownBy(transaction1::commit).isInstanceOf(CommitConflictException.class);
    } else {
      assert isolation == Isolation.READ_COMMITTED || isolation == Isolation.SNAPSHOT;

      // In READ_COMMITTED or SNAPSHOT isolation

      transaction1.commit();

      // The first scan should return the record
      assertThat(result1.size()).isEqualTo(1);
      assertThat(result1.get(0).getInt(BALANCE)).isEqualTo(1);

      // The second scan should return the updated record
      assertThat(result2.size()).isEqualTo(1);
      assertThat(result2.get(0).getInt(BALANCE)).isEqualTo(2);
    }
  }

  @Test
  void scan_RecordUpdatedByAnotherTransaction_WithSerializable_ShouldThrowCommitConflictException()
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    List<Result> results = transaction.scan(prepareScan(0, namespace1, TABLE_1));

    assertThat(results).hasSize(2);
    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(results.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(results.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    DistributedTransaction another = manager.begin();
    another.update(
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
            .intValue(BALANCE, 1)
            .build());
    another.commit();

    assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
  }

  @Test
  void scan_RecordUpdatedByMyself_WithSerializable_ShouldNotThrowAnyException()
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    List<Result> results = transaction.scan(prepareScan(0, namespace1, TABLE_1));

    assertThat(results).hasSize(2);
    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(results.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(results.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    transaction.update(
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
            .intValue(BALANCE, 1)
            .build());

    assertThatCode(transaction::commit).doesNotThrowAnyException();
  }

  @Test
  void
      scan_FirstRecordInsertedByAnotherTransaction_WithSerializable_ShouldThrowCommitConflictException()
          throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 2))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    List<Result> results = transaction.scan(prepareScan(0, namespace1, TABLE_1));

    assertThat(results).hasSize(2);
    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(results.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(2);
    assertThat(results.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    DistributedTransaction another = manager.begin();
    another.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());
    another.commit();

    assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
  }

  @Test
  void scan_FirstRecordInsertedByMyself_WithSerializable_ShouldNotThrowAnyException()
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 2))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    List<Result> results = transaction.scan(prepareScan(0, namespace1, TABLE_1));

    assertThat(results).hasSize(2);
    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(results.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(2);
    assertThat(results.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    transaction.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());

    assertThatCode(transaction::commit).doesNotThrowAnyException();
  }

  @Test
  void
      scan_LastRecordInsertedByAnotherTransaction_WithSerializable_ShouldThrowCommitConflictException()
          throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    List<Result> results = transaction.scan(prepareScan(0, namespace1, TABLE_1));

    assertThat(results).hasSize(2);
    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(results.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(results.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    DistributedTransaction another = manager.begin();
    another.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 2))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());
    another.commit();

    assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
  }

  @Test
  void scan_LastRecordInsertedByMyself_WithSerializable_ShouldNotThrowAnyException()
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    List<Result> results = transaction.scan(prepareScan(0, namespace1, TABLE_1));

    assertThat(results).hasSize(2);
    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(results.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(results.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    transaction.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 2))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());

    assertThatCode(transaction::commit).doesNotThrowAnyException();
  }

  @Test
  void
      scan_FirstRecordDeletedByAnotherTransaction_WithSerializable_ShouldThrowCommitConflictException()
          throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    List<Result> results = transaction.scan(prepareScan(0, namespace1, TABLE_1));

    assertThat(results).hasSize(2);
    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(results.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(results.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    DistributedTransaction another = manager.begin();
    another.delete(
        Delete.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .build());
    another.commit();

    assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
  }

  @Test
  void scan_FirstRecordDeletedByMyself_WithSerializable_ShouldNotThrowAnyException()
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    List<Result> results = transaction.scan(prepareScan(0, namespace1, TABLE_1));

    assertThat(results).hasSize(2);
    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(results.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(results.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    transaction.delete(
        Delete.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .build());

    assertThatCode(transaction::commit).doesNotThrowAnyException();
  }

  @Test
  void scan_ScanWithLimitGiven_WithSerializable_ShouldNotThrowAnyException()
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    List<Result> results =
        transaction.scan(
            Scan.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .limit(1)
                .build());

    assertThat(results).hasSize(1);
    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(results.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    assertThatCode(transaction::commit).doesNotThrowAnyException();
  }

  @Test
  void
      scan_ScanWithLimitGiven_RecordInsertedByAnotherTransaction_WithSerializable_ShouldNotThrowAnyException()
          throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    List<Result> results =
        transaction.scan(
            Scan.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .limit(2)
                .build());

    assertThat(results).hasSize(2);
    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(results.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(results.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    DistributedTransaction another = manager.begin();
    another.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 2))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());
    another.commit();

    assertThatCode(transaction::commit).doesNotThrowAnyException();
  }

  @Test
  void
      scan_ScanWithLimitGiven_FirstRecordInsertedByAnotherTransaction_WithSerializable_ShouldThrowCommitConflictException()
          throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 2))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    List<Result> results =
        transaction.scan(
            Scan.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .limit(2)
                .build());

    assertThat(results).hasSize(2);
    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(results.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(2);
    assertThat(results.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    DistributedTransaction another = manager.begin();
    another.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());
    another.commit();

    assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
  }

  @Test
  void
      scan_ScanWithLimitGiven_FirstRecordInsertedByMyself_WithSerializable_ShouldNotThrowAnyException()
          throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 2))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    List<Result> results =
        transaction.scan(
            Scan.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .limit(2)
                .build());

    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(results.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(2);
    assertThat(results.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    transaction.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());

    assertThatCode(transaction::commit).doesNotThrowAnyException();
  }

  @Test
  void
      scan_ScanWithLimitGiven_LastRecordInsertedByAnotherTransaction_WithSerializable_ShouldThrowCommitConflictException()
          throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    List<Result> results =
        transaction.scan(
            Scan.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .limit(3)
                .build());

    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(results.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(results.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    DistributedTransaction another = manager.begin();
    another.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 2))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());
    another.commit();

    assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
  }

  @Test
  void
      scan_ScanWithLimitGiven_LastRecordInsertedByMyself_WithSerializable_ShouldNotThrowAnyException()
          throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    List<Result> results =
        transaction.scan(
            Scan.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .limit(3)
                .build());

    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(results.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(results.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    transaction.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 2))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());

    assertThatCode(transaction::commit).doesNotThrowAnyException();
  }

  @Test
  void scan_ScanAllGiven_WithSerializable_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 1))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 2))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 3))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 4))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    List<Result> results =
        transaction.scan(Scan.newBuilder().namespace(namespace1).table(TABLE_1).all().build());

    assertThat(results).hasSize(5);

    Set<Integer> expectedIds = Sets.newHashSet(0, 1, 2, 3, 4);
    for (Result result : results) {
      expectedIds.remove(result.getInt(ACCOUNT_ID));
      assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
      assertThat(result.getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    }
    assertThat(expectedIds).isEmpty();

    assertThatCode(transaction::commit).doesNotThrowAnyException();
  }

  @Test
  void scan_ScanAllWithLimitGiven_WithSerializable_ShouldNotThrowAnyException()
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 1))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 2))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 3))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 4))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    List<Result> results =
        transaction.scan(
            Scan.newBuilder().namespace(namespace1).table(TABLE_1).all().limit(3).build());

    assertThat(results).hasSize(3);

    Set<Integer> expectedIds = Sets.newHashSet(0, 1, 2, 3, 4);
    for (Result result : results) {
      expectedIds.remove(result.getInt(ACCOUNT_ID));
      assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
      assertThat(result.getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    }
    assertThat(expectedIds).hasSize(2);

    assertThatCode(transaction::commit).doesNotThrowAnyException();
  }

  @Test
  public void scan_ScanWithIndexGiven_WithSerializable_ShouldNotThrowAnyException()
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 1))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 2))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 3))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 4))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    List<Result> results =
        transaction.scan(
            Scan.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .indexKey(Key.ofInt(BALANCE, INITIAL_BALANCE))
                .build());

    assertThat(results).hasSize(5);

    Set<Integer> expectedIds = Sets.newHashSet(0, 1, 2, 3, 4);
    for (Result result : results) {
      expectedIds.remove(result.getInt(ACCOUNT_ID));
      assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
      assertThat(result.getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    }
    assertThat(expectedIds).isEmpty();

    assertThatCode(transaction::commit).doesNotThrowAnyException();
  }

  @Test
  public void
      scan_ScanWithIndexGiven_RecordUpdatedByAnotherTransaction_WithSerializable_ShouldThrowCommitConflictException()
          throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 1))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 2))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 3))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 4))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    List<Result> results =
        transaction.scan(
            Scan.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .indexKey(Key.ofInt(BALANCE, INITIAL_BALANCE))
                .build());

    assertThat(results).hasSize(5);

    Set<Integer> expectedIds = Sets.newHashSet(0, 1, 2, 3, 4);
    for (Result result : results) {
      expectedIds.remove(result.getInt(ACCOUNT_ID));
      assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
      assertThat(result.getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    }
    assertThat(expectedIds).isEmpty();

    // The record is updated by another transaction
    manager.update(
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 1))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, 100)
            .build());

    assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      scan_ScanWithIndexGiven_RecordUpdatedByMyself_WithSerializable_ShouldNotThrowAnyException()
          throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 1))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 2))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 3))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 4))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    List<Result> results =
        transaction.scan(
            Scan.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .indexKey(Key.ofInt(BALANCE, INITIAL_BALANCE))
                .build());

    assertThat(results).hasSize(5);

    Set<Integer> expectedIds = Sets.newHashSet(0, 1, 2, 3, 4);
    for (Result result : results) {
      expectedIds.remove(result.getInt(ACCOUNT_ID));
      assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
      assertThat(result.getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    }
    assertThat(expectedIds).isEmpty();

    transaction.update(
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 1))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, 100)
            .build());

    assertThatCode(transaction::commit).doesNotThrowAnyException();
  }

  @Test
  public void
      scan_ScanWithIndexGiven_RecordDeletedByAnotherTransaction_WithSerializable_ShouldThrowCommitConflictException()
          throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 1))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 2))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 3))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 4))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    List<Result> results =
        transaction.scan(
            Scan.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .indexKey(Key.ofInt(BALANCE, INITIAL_BALANCE))
                .build());

    assertThat(results).hasSize(5);

    Set<Integer> expectedIds = Sets.newHashSet(0, 1, 2, 3, 4);
    for (Result result : results) {
      expectedIds.remove(result.getInt(ACCOUNT_ID));
      assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
      assertThat(result.getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    }
    assertThat(expectedIds).isEmpty();

    // The record is deleted by another transaction
    manager.delete(
        Delete.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 1))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .build());

    assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      scan_ScanWithIndexGiven_RecordDeletedByMyself_WithSerializable_ShouldNotThrowAnyException()
          throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 1))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 2))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 3))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 4))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    List<Result> results =
        transaction.scan(
            Scan.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .indexKey(Key.ofInt(BALANCE, INITIAL_BALANCE))
                .build());

    assertThat(results).hasSize(5);

    Set<Integer> expectedIds = Sets.newHashSet(0, 1, 2, 3, 4);
    for (Result result : results) {
      expectedIds.remove(result.getInt(ACCOUNT_ID));
      assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
      assertThat(result.getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    }
    assertThat(expectedIds).isEmpty();

    transaction.delete(
        Delete.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 1))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .build());

    assertThatCode(transaction::commit).doesNotThrowAnyException();
  }

  @Test
  public void scan_ScanWithIndexWithLimitGiven_WithSerializable_ShouldNotThrowAnyException()
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 1))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 2))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 3))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 4))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    List<Result> results =
        transaction.scan(
            Scan.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .indexKey(Key.ofInt(BALANCE, INITIAL_BALANCE))
                .limit(3)
                .build());

    assertThat(results).hasSize(3);

    Set<Integer> expectedIds = Sets.newHashSet(0, 1, 2, 3, 4);
    for (Result result : results) {
      expectedIds.remove(result.getInt(ACCOUNT_ID));
      assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
      assertThat(result.getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    }
    assertThat(expectedIds).hasSize(2);

    assertThatCode(transaction::commit).doesNotThrowAnyException();
  }

  @Test
  public void get_GetWithIndexGiven_WithSerializable_ShouldNotThrowAnyException()
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    Optional<Result> actual =
        transaction.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .indexKey(Key.ofInt(BALANCE, INITIAL_BALANCE))
                .build());

    assertThat(actual).isPresent();
    assertThat(actual.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(actual.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(actual.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    assertThatCode(transaction::commit).doesNotThrowAnyException();
  }

  @Test
  public void
      get_GetWithIndexGiven_RecordUpdatedByAnotherTransaction_WithSerializable_ShouldThrowCommitConflictException()
          throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    Optional<Result> actual =
        transaction.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .indexKey(Key.ofInt(BALANCE, INITIAL_BALANCE))
                .build());

    assertThat(actual).isPresent();
    assertThat(actual.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(actual.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(actual.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    // The record is updated by another transaction
    manager.update(
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, 100)
            .build());

    assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      get_GetWithIndexGiven_RecordUpdatedByMyself_WithSerializable_ShouldNotThrowAnyException()
          throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    Optional<Result> actual =
        transaction.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .indexKey(Key.ofInt(BALANCE, INITIAL_BALANCE))
                .build());

    assertThat(actual).isPresent();
    assertThat(actual.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(actual.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(actual.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    transaction.update(
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, 100)
            .build());

    assertThatCode(transaction::commit).doesNotThrowAnyException();
  }

  @Test
  public void
      get_GetWithIndexGiven_RecordDeletedByAnotherTransaction_WithSerializable_ShouldThrowCommitConflictException()
          throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    Optional<Result> actual =
        transaction.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .indexKey(Key.ofInt(BALANCE, INITIAL_BALANCE))
                .build());

    assertThat(actual).isPresent();
    assertThat(actual.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(actual.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(actual.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    // The record is deleted by another transaction
    manager.delete(
        Delete.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .build());

    assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      get_GetWithIndexGiven_RecordDeletedByMyself_WithSerializable_ShouldNotThrowAnyException()
          throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    Optional<Result> actual =
        transaction.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .indexKey(Key.ofInt(BALANCE, INITIAL_BALANCE))
                .build());

    assertThat(actual).isPresent();
    assertThat(actual.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(actual.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(actual.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    transaction.delete(
        Delete.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .build());

    assertThatCode(transaction::commit).doesNotThrowAnyException();
  }

  @Test
  void getScanner_WithSerializable_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 2))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    Scan scan = prepareScan(0, namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();

    // Act Assert
    TransactionCrudOperable.Scanner scanner = transaction.getScanner(scan);
    Optional<Result> result1 = scanner.one();
    assertThat(result1).isNotEmpty();
    assertThat(result1.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result1.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result1.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    Optional<Result> result2 = scanner.one();
    assertThat(result2).isNotEmpty();
    assertThat(result2.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result2.get().getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(result2.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    scanner.close();

    assertThatCode(transaction::commit).doesNotThrowAnyException();
  }

  @Test
  void
      getScanner_FirstInsertedRecordByAnotherTransaction_WithSerializable_ShouldNotThrowCommitConflictException()
          throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 2))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    Scan scan = prepareScan(0, namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();

    // Act Assert
    TransactionCrudOperable.Scanner scanner = transaction.getScanner(scan);
    Optional<Result> result1 = scanner.one();
    assertThat(result1).isNotEmpty();
    assertThat(result1.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result1.get().getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(result1.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    Optional<Result> result2 = scanner.one();
    assertThat(result2).isNotEmpty();
    assertThat(result2.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result2.get().getInt(ACCOUNT_TYPE)).isEqualTo(2);
    assertThat(result2.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    scanner.close();

    DistributedTransaction another = manager.begin();
    another.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());
    another.commit();

    assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
  }

  @Test
  void getScanner_RecordInsertedByAnotherTransaction_WithSerializable_ShouldNotThrowAnyException()
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    Scan scan = prepareScan(0, namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();

    // Act Assert
    TransactionCrudOperable.Scanner scanner = transaction.getScanner(scan);
    Optional<Result> result1 = scanner.one();
    assertThat(result1).isNotEmpty();
    assertThat(result1.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result1.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result1.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    Optional<Result> result2 = scanner.one();
    assertThat(result2).isNotEmpty();
    assertThat(result2.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result2.get().getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(result2.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    scanner.close();

    DistributedTransaction another = manager.begin();
    another.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 2))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());
    another.commit();

    assertThatCode(transaction::commit).doesNotThrowAnyException();
  }

  @Test
  void
      getScanner_RecordUpdatedByAnotherTransaction_WithSerializable_ShouldThrowCommitConflictException()
          throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    Scan scan = prepareScan(0, namespace1, TABLE_1);
    DistributedTransaction transaction = manager.begin();

    // Act Assert
    TransactionCrudOperable.Scanner scanner = transaction.getScanner(scan);
    Optional<Result> result1 = scanner.one();
    assertThat(result1).isNotEmpty();
    assertThat(result1.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result1.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result1.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    Optional<Result> result2 = scanner.one();
    assertThat(result2).isNotEmpty();
    assertThat(result2.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result2.get().getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(result2.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    scanner.close();

    DistributedTransaction another = manager.begin();
    another.update(
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, 0)
            .build());
    another.commit();

    assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      get_GetWithIndexGiven_NoRecordsInIndexRange_WithSerializable_ShouldNotThrowAnyException()
          throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    Optional<Result> actual =
        transaction.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .indexKey(Key.ofInt(BALANCE, INITIAL_BALANCE))
                .build());

    assertThat(actual).isEmpty();

    assertThatCode(transaction::commit).doesNotThrowAnyException();
  }

  @Test
  public void
      get_GetWithIndexGiven_RecordInsertedIntoIndexRangeByMyself_WithSerializable_ShouldNotThrowAnyException()
          throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    Optional<Result> actual =
        transaction.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .indexKey(Key.ofInt(BALANCE, INITIAL_BALANCE))
                .build());

    assertThat(actual).isPresent();
    assertThat(actual.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(actual.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(actual.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    transaction.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 1))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());

    assertThatCode(transaction::commit).doesNotThrowAnyException();
  }

  @Test
  public void
      get_GetWithIndexGiven_RecordInsertedIntoIndexRangeByAnotherTransaction_WithSerializable_ShouldThrowCommitConflictException()
          throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    manager.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    Optional<Result> actual =
        transaction.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .indexKey(Key.ofInt(BALANCE, INITIAL_BALANCE))
                .build());

    assertThat(actual).isPresent();
    assertThat(actual.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(actual.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(actual.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    DistributedTransaction another = manager.begin();
    another.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 1))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());
    another.commit();

    assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void
      get_GetWithIndexGiven_NoRecordsInIndexRange_RecordInsertedIntoIndexRangeByMyself_WithSerializable_ShouldNotThrowAnyException()
          throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    Optional<Result> actual =
        transaction.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .indexKey(Key.ofInt(BALANCE, INITIAL_BALANCE))
                .build());

    assertThat(actual).isEmpty();

    transaction.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());

    assertThatCode(transaction::commit).doesNotThrowAnyException();
  }

  @Test
  public void
      get_GetWithIndexGiven_NoRecordsInIndexRange_RecordInsertedIntoIndexRangeByAnotherTransaction_WithSerializable_ShouldThrowCommitConflictException()
          throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    Optional<Result> actual =
        transaction.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .indexKey(Key.ofInt(BALANCE, INITIAL_BALANCE))
                .build());

    assertThat(actual).isEmpty();

    DistributedTransaction another = manager.begin();
    another.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());
    another.commit();

    assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  public void getAndUpdate_GetWithIndexGiven_ShouldUpdate(Isolation isolation)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    manager.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    Optional<Result> result =
        transaction.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .indexKey(Key.ofInt(BALANCE, INITIAL_BALANCE))
                .build());

    assertThat(result).isPresent();
    assertThat(result.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    transaction.update(
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, 1)
            .build());

    transaction.commit();

    Optional<Result> actual =
        manager.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .build());
    assertThat(actual).isPresent();
    assertThat(actual.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(actual.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(actual.get().getInt(BALANCE)).isEqualTo(1);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  public void scanAndUpdate_ScanWithIndexGiven_ShouldUpdate(Isolation isolation)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 2))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Act Assert
    DistributedTransaction transaction = manager.begin();
    List<Result> results =
        transaction.scan(
            Scan.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .indexKey(Key.ofInt(BALANCE, INITIAL_BALANCE))
                .build());

    assertThat(results).hasSize(3);
    Set<Integer> expectedTypes = Sets.newHashSet(0, 1, 2);
    for (Result result : results) {
      assertThat(result.getInt(ACCOUNT_ID)).isEqualTo(0);
      expectedTypes.remove(result.getInt(ACCOUNT_TYPE));
      assertThat(result.getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    }
    assertThat(expectedTypes).isEmpty();

    transaction.update(
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, 1)
            .build());
    transaction.update(
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 2))
            .intValue(BALANCE, 2)
            .build());

    transaction.commit();

    transaction = manager.beginReadOnly();
    Optional<Result> actual1 =
        transaction.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .build());
    Optional<Result> actual2 =
        transaction.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .build());
    Optional<Result> actual3 =
        transaction.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 2))
                .build());
    transaction.commit();

    assertThat(actual1).isPresent();
    assertThat(actual1.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(actual1.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(actual1.get().getInt(BALANCE)).isEqualTo(1);

    assertThat(actual2).isPresent();
    assertThat(actual2.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(actual2.get().getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(actual2.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    assertThat(actual3).isPresent();
    assertThat(actual3.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(actual3.get().getInt(ACCOUNT_TYPE)).isEqualTo(2);
    assertThat(actual3.get().getInt(BALANCE)).isEqualTo(2);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void
      get_WithConjunction_ForPreparedRecordWhoseBeforeImageMatchesConjunction_ShouldReturnRecordAfterLazyRecovery(
          Isolation isolation, boolean readOnly)
          throws UnknownTransactionStatusException, CrudException, ExecutionException,
              CommitException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    manager.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());

    // Create a prepared record without before image
    Put put =
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, 100)
            .build();
    Optional<Result> result =
        originalStorage.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .build());
    String transactionId = UUID.randomUUID().toString();
    PrepareMutationComposer prepareMutationComposer =
        new PrepareMutationComposer(
            transactionId,
            System.currentTimeMillis() - (RecoveryHandler.TRANSACTION_LIFETIME_MILLIS + 1),
            new TransactionTableMetadataManager(admin, 0));
    prepareMutationComposer.add(put, result.map(TransactionResult::new).orElse(null));
    originalStorage.mutate(prepareMutationComposer.get());

    // Act Assert
    DistributedTransaction transaction = begin(manager, readOnly);
    Optional<Result> actual =
        transaction.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .where(column(BALANCE).isEqualToInt(INITIAL_BALANCE))
                .build());
    transaction.commit();

    assertThat(actual).isPresent();
    assertThat(actual.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(actual.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(actual.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void
      get_WithConjunction_ForCommittedRecordWhoseBeforeImageMatchesConjunction_ShouldNotReturnRecord(
          Isolation isolation, boolean readOnly)
          throws UnknownTransactionStatusException, CrudException, ExecutionException,
              CommitException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    manager.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());

    // Create a committed record with before image to simulate an old committed record that has both
    // after and before images
    Put put =
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, 100)
            .build();
    Optional<Result> result =
        originalStorage.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .build());
    String transactionId = UUID.randomUUID().toString();
    PrepareMutationComposer prepareMutationComposer =
        new PrepareMutationComposer(
            transactionId,
            System.currentTimeMillis() - (RecoveryHandler.TRANSACTION_LIFETIME_MILLIS + 1),
            new TransactionTableMetadataManager(admin, 0));
    prepareMutationComposer.add(put, result.map(TransactionResult::new).orElse(null));
    originalStorage.mutate(prepareMutationComposer.get());
    originalStorage.put(
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(Attribute.STATE, TransactionState.COMMITTED.get())
            .bigIntValue(Attribute.COMMITTED_AT, System.currentTimeMillis())
            .build());

    // Act Assert
    DistributedTransaction transaction = begin(manager, readOnly);
    Optional<Result> actual =
        transaction.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .where(column(BALANCE).isEqualToInt(INITIAL_BALANCE))
                .build());
    transaction.commit();

    assertThat(actual).isNotPresent();
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void
      scan_WithConjunction_ForPreparedRecordWhoseBeforeImageMatchesConjunction_ShouldReturnRecordAfterLazyRecovery(
          Isolation isolation, boolean readOnly)
          throws UnknownTransactionStatusException, CrudException, ExecutionException,
              CommitException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Create a prepared record without before image
    Optional<Result> result =
        originalStorage.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .build());
    Put put =
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, 100)
            .build();
    String transactionId = UUID.randomUUID().toString();
    PrepareMutationComposer prepareMutationComposer =
        new PrepareMutationComposer(
            transactionId,
            System.currentTimeMillis() - (RecoveryHandler.TRANSACTION_LIFETIME_MILLIS + 1),
            new TransactionTableMetadataManager(admin, 0));
    prepareMutationComposer.add(put, result.map(TransactionResult::new).orElse(null));
    originalStorage.mutate(prepareMutationComposer.get());

    // Act Assert
    DistributedTransaction transaction = begin(manager, readOnly);
    List<Result> results =
        transaction.scan(
            Scan.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .where(column(BALANCE).isEqualToInt(INITIAL_BALANCE))
                .build());
    transaction.commit();

    assertThat(results).hasSize(2);
    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(results.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(results.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void
      scan_WithConjunction_ForCommittedRecordWhoseBeforeImageMatchesConjunction_ShouldNotReturnRecord(
          Isolation isolation, boolean readOnly)
          throws UnknownTransactionStatusException, CrudException, ExecutionException,
              CommitException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Create a committed record with before image to simulate an old committed record that has both
    // after and before images
    Optional<Result> result =
        originalStorage.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .build());
    Put put =
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, 100)
            .build();
    String transactionId = UUID.randomUUID().toString();
    PrepareMutationComposer prepareMutationComposer =
        new PrepareMutationComposer(
            transactionId,
            System.currentTimeMillis() - (RecoveryHandler.TRANSACTION_LIFETIME_MILLIS + 1),
            new TransactionTableMetadataManager(admin, 0));
    prepareMutationComposer.add(put, result.map(TransactionResult::new).orElse(null));
    originalStorage.mutate(prepareMutationComposer.get());
    originalStorage.put(
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(Attribute.STATE, TransactionState.COMMITTED.get())
            .bigIntValue(Attribute.COMMITTED_AT, System.currentTimeMillis())
            .build());

    // Act Assert
    DistributedTransaction transaction = begin(manager, readOnly);
    List<Result> results =
        transaction.scan(
            Scan.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .where(column(BALANCE).isEqualToInt(INITIAL_BALANCE))
                .build());
    transaction.commit();

    assertThat(results).hasSize(1);
    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(results.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void
      scan_WithConjunctionAndLimit_ForCommittedRecordWhoseBeforeImageMatchesConjunction_ShouldNotReturnRecord(
          Isolation isolation, boolean readOnly)
          throws UnknownTransactionStatusException, CrudException, ExecutionException,
              CommitException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 2))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 3))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Create a committed record with before image to simulate an old committed record that has both
    // after and before images
    Optional<Result> result =
        originalStorage.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .build());
    Put put =
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, 100)
            .build();
    String transactionId = UUID.randomUUID().toString();
    PrepareMutationComposer prepareMutationComposer =
        new PrepareMutationComposer(
            transactionId,
            System.currentTimeMillis() - (RecoveryHandler.TRANSACTION_LIFETIME_MILLIS + 1),
            new TransactionTableMetadataManager(admin, 0));
    prepareMutationComposer.add(put, result.map(TransactionResult::new).orElse(null));
    originalStorage.mutate(prepareMutationComposer.get());
    originalStorage.put(
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(Attribute.STATE, TransactionState.COMMITTED.get())
            .bigIntValue(Attribute.COMMITTED_AT, System.currentTimeMillis())
            .build());

    // Act Assert
    DistributedTransaction transaction = begin(manager, readOnly);
    List<Result> results =
        transaction.scan(
            Scan.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .where(column(BALANCE).isEqualToInt(INITIAL_BALANCE))
                .limit(2)
                .build());
    transaction.commit();

    assertThat(results).hasSize(2);
    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(results.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(2);
    assertThat(results.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void
      getScanner_WithConjunction_ForPreparedRecordWhoseBeforeImageMatchesConjunction_ShouldReturnRecordAfterLazyRecovery(
          Isolation isolation, boolean readOnly)
          throws UnknownTransactionStatusException, CrudException, ExecutionException,
              CommitException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Create a prepared record without before image
    Optional<Result> result =
        originalStorage.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .build());
    Put put =
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, 100)
            .build();
    String transactionId = UUID.randomUUID().toString();
    PrepareMutationComposer prepareMutationComposer =
        new PrepareMutationComposer(
            transactionId,
            System.currentTimeMillis() - (RecoveryHandler.TRANSACTION_LIFETIME_MILLIS + 1),
            new TransactionTableMetadataManager(admin, 0));
    prepareMutationComposer.add(put, result.map(TransactionResult::new).orElse(null));
    originalStorage.mutate(prepareMutationComposer.get());

    // Act Assert
    DistributedTransaction transaction = begin(manager, readOnly);
    List<Result> results;
    try (TransactionCrudOperable.Scanner scanner =
        transaction.getScanner(
            Scan.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .where(column(BALANCE).isEqualToInt(INITIAL_BALANCE))
                .build())) {
      results = scanner.all();
    }
    transaction.commit();

    assertThat(results).hasSize(2);
    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(results.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(results.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void
      getScanner_WithConjunction_ForCommittedRecordWhoseBeforeImageMatchesConjunction_ShouldNotReturnRecord(
          Isolation isolation, boolean readOnly)
          throws UnknownTransactionStatusException, CrudException, ExecutionException,
              CommitException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Create a committed record with before image to simulate an old committed record that has both
    // after and before images
    Optional<Result> result =
        originalStorage.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .build());
    Put put =
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, 100)
            .build();
    String transactionId = UUID.randomUUID().toString();
    PrepareMutationComposer prepareMutationComposer =
        new PrepareMutationComposer(
            transactionId,
            System.currentTimeMillis() - (RecoveryHandler.TRANSACTION_LIFETIME_MILLIS + 1),
            new TransactionTableMetadataManager(admin, 0));
    prepareMutationComposer.add(put, result.map(TransactionResult::new).orElse(null));
    originalStorage.mutate(prepareMutationComposer.get());
    originalStorage.put(
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(Attribute.STATE, TransactionState.COMMITTED.get())
            .bigIntValue(Attribute.COMMITTED_AT, System.currentTimeMillis())
            .build());

    // Act Assert
    DistributedTransaction transaction = begin(manager, readOnly);
    List<Result> results;
    try (TransactionCrudOperable.Scanner scanner =
        transaction.getScanner(
            Scan.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .where(column(BALANCE).isEqualToInt(INITIAL_BALANCE))
                .build())) {
      results = scanner.all();
    }
    transaction.commit();

    assertThat(results).hasSize(1);
    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(results.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
  }

  @ParameterizedTest
  @MethodSource("isolationAndReadOnlyMode")
  void
      getScanner_WithConjunctionAndLimit_ForCommittedRecordWhoseBeforeImageMatchesConjunction_ShouldNotReturnRecord(
          Isolation isolation, boolean readOnly)
          throws UnknownTransactionStatusException, CrudException, ExecutionException,
              CommitException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    manager.mutate(
        Arrays.asList(
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 2))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build(),
            Insert.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 3))
                .intValue(BALANCE, INITIAL_BALANCE)
                .build()));

    // Create a committed record with before image to simulate an old committed record that has both
    // after and before images
    Optional<Result> result =
        originalStorage.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .build());
    Put put =
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, 100)
            .build();
    String transactionId = UUID.randomUUID().toString();
    PrepareMutationComposer prepareMutationComposer =
        new PrepareMutationComposer(
            transactionId,
            System.currentTimeMillis() - (RecoveryHandler.TRANSACTION_LIFETIME_MILLIS + 1),
            new TransactionTableMetadataManager(admin, 0));
    prepareMutationComposer.add(put, result.map(TransactionResult::new).orElse(null));
    originalStorage.mutate(prepareMutationComposer.get());
    originalStorage.put(
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(Attribute.STATE, TransactionState.COMMITTED.get())
            .bigIntValue(Attribute.COMMITTED_AT, System.currentTimeMillis())
            .build());

    // Act Assert
    DistributedTransaction transaction = begin(manager, readOnly);
    List<Result> results;
    try (TransactionCrudOperable.Scanner scanner =
        transaction.getScanner(
            Scan.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .where(column(BALANCE).isEqualToInt(INITIAL_BALANCE))
                .limit(2)
                .build())) {
      results = scanner.all();
    }
    transaction.commit();

    assertThat(results).hasSize(2);
    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(results.get(0).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(results.get(1).getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(results.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(2);
    assertThat(results.get(1).getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void
      commit_ConflictingExternalUpdate_DifferentGetButSameRecordReturned_ShouldThrowShouldBehaveCorrectly(
          Isolation isolation)
          throws UnknownTransactionStatusException, CrudException, RollbackException,
              CommitException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    manager.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());

    // Act Assert
    DistributedTransaction transaction = manager.begin();

    // Retrieve the record
    Optional<Result> result =
        transaction.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .build());

    assertThat(result).isPresent();
    assertThat(result.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result.get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);

    // Update the balance of the record
    transaction.update(
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .condition(updateIf(column(BALANCE).isEqualToInt(INITIAL_BALANCE)).build())
            .intValue(BALANCE, 100)
            .build());

    // Update the balance of the record by another transaction
    manager.update(
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, 200)
            .build());

    // Retrieve the record again, but use a different Get object (with a where clause)
    result =
        transaction.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .where(column(BALANCE).isEqualToInt(200))
                .build());

    assertThat(result).isNotPresent();

    // CommitConflictException should be thrown because the record was updated by another
    // transaction
    assertThatThrownBy(transaction::commit).isInstanceOf(CommitConflictException.class);
    transaction.rollback();

    result =
        manager.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .build());

    // The record should still exist with the updated balance by the other transaction
    assertThat(result).isPresent();
    assertThat(result.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result.get().getInt(BALANCE)).isEqualTo(200);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void manager_get_GetGivenForCommittedRecord_ShouldReturnRecord(Isolation isolation)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecord(manager, namespace1, TABLE_1);
    Get get = prepareGet(0, 0, namespace1, TABLE_1);

    // Act
    Optional<Result> result = manager.get(get);

    // Assert
    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void manager_scan_ScanGivenForCommittedRecord_ShouldReturnRecords(Isolation isolation)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecords(manager, namespace1, TABLE_1);
    Scan scan = prepareScan(1, 0, 2, namespace1, TABLE_1);

    // Act
    List<Result> results = manager.scan(scan);

    // Assert
    assertThat(results.size()).isEqualTo(3);
    assertThat(results.get(0).getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(results.get(0).getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(getBalance(results.get(0))).isEqualTo(INITIAL_BALANCE);

    assertThat(results.get(1).getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(results.get(1).getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(getBalance(results.get(1))).isEqualTo(INITIAL_BALANCE);

    assertThat(results.get(2).getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(results.get(2).getInt(ACCOUNT_TYPE)).isEqualTo(2);
    assertThat(getBalance(results.get(2))).isEqualTo(INITIAL_BALANCE);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void manager_getScanner_ScanGivenForCommittedRecord_ShouldReturnRecords(Isolation isolation)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecords(manager, namespace1, TABLE_1);
    Scan scan = prepareScan(1, 0, 2, namespace1, TABLE_1);

    // Act Assert
    TransactionManagerCrudOperable.Scanner scanner = manager.getScanner(scan);

    Optional<Result> result1 = scanner.one();
    assertThat(result1).isPresent();
    assertThat(result1.get().getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(result1.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(getBalance(result1.get())).isEqualTo(INITIAL_BALANCE);

    Optional<Result> result2 = scanner.one();
    assertThat(result2).isPresent();
    assertThat(result2.get().getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(result2.get().getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(getBalance(result2.get())).isEqualTo(INITIAL_BALANCE);

    Optional<Result> result3 = scanner.one();
    assertThat(result3).isPresent();
    assertThat(result3.get().getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(result3.get().getInt(ACCOUNT_TYPE)).isEqualTo(2);
    assertThat(getBalance(result3.get())).isEqualTo(INITIAL_BALANCE);

    assertThat(scanner.one()).isNotPresent();

    scanner.close();
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void manager_put_PutGivenForNonExisting_ShouldCreateRecord(Isolation isolation)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    int expected = INITIAL_BALANCE;
    Put put =
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, expected)
            .build();

    // Act
    manager.put(put);

    // Assert
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    Optional<Result> result = manager.get(get);

    assertThat(result.isPresent()).isTrue();
    assertThat(getBalance(result.get())).isEqualTo(expected);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void manager_put_PutGivenForExisting_ShouldUpdateRecord(Isolation isolation)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecord(manager, namespace1, TABLE_1);

    // Act
    int expected = INITIAL_BALANCE + 100;
    Put put =
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, expected)
            .enableImplicitPreRead()
            .build();
    manager.put(put);

    // Assert
    Optional<Result> actual = manager.get(prepareGet(0, 0, namespace1, TABLE_1));

    assertThat(actual.isPresent()).isTrue();
    assertThat(getBalance(actual.get())).isEqualTo(expected);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void manager_insert_InsertGivenForNonExisting_ShouldCreateRecord(Isolation isolation)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    int expected = INITIAL_BALANCE;
    Insert insert =
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, expected)
            .build();

    // Act
    manager.insert(insert);

    // Assert
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    Optional<Result> result = manager.get(get);

    assertThat(result.isPresent()).isTrue();
    assertThat(getBalance(result.get())).isEqualTo(expected);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void manager_insert_InsertGivenForExisting_ShouldThrowCrudConflictException(Isolation isolation)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecord(manager, namespace1, TABLE_1);

    // Act Assert
    int expected = INITIAL_BALANCE + 100;
    Insert insert =
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, expected)
            .build();

    assertThatThrownBy(() -> manager.insert(insert)).isInstanceOf(CrudConflictException.class);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void manager_upsert_UpsertGivenForNonExisting_ShouldCreateRecord(Isolation isolation)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    int expected = INITIAL_BALANCE;
    Upsert upsert =
        Upsert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, expected)
            .build();

    // Act
    manager.upsert(upsert);

    // Assert
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    Optional<Result> result = manager.get(get);

    assertThat(result.isPresent()).isTrue();
    assertThat(getBalance(result.get())).isEqualTo(expected);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void manager_upsert_UpsertGivenForExisting_ShouldUpdateRecord(Isolation isolation)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecord(manager, namespace1, TABLE_1);

    // Act
    int expected = INITIAL_BALANCE + 100;
    Upsert upsert =
        Upsert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, expected)
            .build();
    manager.upsert(upsert);

    // Assert
    Optional<Result> actual = manager.get(prepareGet(0, 0, namespace1, TABLE_1));

    assertThat(actual.isPresent()).isTrue();
    assertThat(getBalance(actual.get())).isEqualTo(expected);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void manager_update_UpdateGivenForNonExisting_ShouldDoNothing(Isolation isolation)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    Update update =
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build();

    // Act
    assertThatCode(() -> manager.update(update)).doesNotThrowAnyException();

    // Assert
    Optional<Result> actual = manager.get(prepareGet(0, 0, namespace1, TABLE_1));

    assertThat(actual).isEmpty();
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void manager_update_UpdateGivenForExisting_ShouldUpdateRecord(Isolation isolation)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecord(manager, namespace1, TABLE_1);

    // Act
    int expected = INITIAL_BALANCE + 100;
    Update update =
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, expected)
            .build();
    manager.update(update);

    // Assert
    Optional<Result> actual = manager.get(prepareGet(0, 0, namespace1, TABLE_1));

    assertThat(actual.isPresent()).isTrue();
    assertThat(getBalance(actual.get())).isEqualTo(expected);
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void manager_delete_DeleteGivenForExisting_ShouldDeleteRecord(Isolation isolation)
      throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    populateRecord(manager, namespace1, TABLE_1);
    Delete delete = prepareDelete(0, 0, namespace1, TABLE_1);

    // Act
    manager.delete(delete);

    // Assert
    Optional<Result> result = manager.get(prepareGet(0, 0, namespace1, TABLE_1));

    assertThat(result.isPresent()).isFalse();
  }

  @ParameterizedTest
  @EnumSource(Isolation.class)
  void manager_mutate_ShouldMutateRecords(Isolation isolation) throws TransactionException {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation);
    manager.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());
    manager.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 1))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());

    Update update =
        Update.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, 1)
            .build();
    Delete delete = prepareDelete(1, 0, namespace1, TABLE_1);

    // Act
    manager.mutate(Arrays.asList(update, delete));

    // Assert
    Optional<Result> result1 = manager.get(prepareGet(0, 0, namespace1, TABLE_1));
    Optional<Result> result2 = manager.get(prepareGet(1, 0, namespace1, TABLE_1));

    assertThat(result1.isPresent()).isTrue();
    assertThat(getBalance(result1.get())).isEqualTo(1);

    assertThat(result2.isPresent()).isFalse();
  }

  @ParameterizedTest
  @MethodSource("isolationAndOnePhaseCommitEnabled")
  public void
      insertAndCommit_SinglePartitionMutationsGiven_ShouldBehaveCorrectlyBasedOnStorageMutationAtomicityUnit(
          Isolation isolation, boolean onePhaseCommitEnabled)
          throws TransactionException, ExecutionException, CoordinatorException {
    if (isGroupCommitEnabled() && onePhaseCommitEnabled) {
      // Enabling both one-phase commit and group commit is not supported
      return;
    }

    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation, onePhaseCommitEnabled);
    DistributedTransaction transaction = manager.begin();

    // Act
    transaction.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, 100)
            .build());
    transaction.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
            .intValue(BALANCE, 200)
            .build());
    transaction.commit();

    // Assert
    StorageInfo storageInfo = admin.getStorageInfo(namespace1);
    switch (storageInfo.getMutationAtomicityUnit()) {
      case RECORD:
        // twice for prepare, twice for commit
        verify(storage, times(4)).mutate(anyList());

        // commit-state should occur
        if (isGroupCommitEnabled()) {
          verify(coordinator)
              .putStateForGroupCommit(
                  anyString(), anyList(), any(TransactionState.class), anyLong());
          return;
        }
        verify(coordinator).putState(any(Coordinator.State.class));
        break;
      case PARTITION:
      case TABLE:
      case NAMESPACE:
      case STORAGE:
        if (onePhaseCommitEnabled && isolation != Isolation.SERIALIZABLE) {
          // one-phase commit, so only one mutation call
          verify(storage).mutate(anyList());

          // no commit-state should occur
          verify(coordinator, never()).putState(any(Coordinator.State.class));
        } else {
          // one for prepare, one for commit
          verify(storage, times(2)).mutate(anyList());

          // commit-state should occur
          if (isGroupCommitEnabled()) {
            verify(coordinator)
                .putStateForGroupCommit(
                    anyString(), anyList(), any(TransactionState.class), anyLong());
          } else {
            verify(coordinator).putState(any(Coordinator.State.class));
          }
        }
        break;
      default:
        throw new AssertionError();
    }

    Optional<Result> result1 =
        manager.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .build());
    assertThat(result1.isPresent()).isTrue();
    assertThat(result1.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result1.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result1.get().getInt(BALANCE)).isEqualTo(100);

    Optional<Result> result2 =
        manager.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 1))
                .build());
    assertThat(result2.isPresent()).isTrue();
    assertThat(result2.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result2.get().getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(result2.get().getInt(BALANCE)).isEqualTo(200);
  }

  @ParameterizedTest
  @MethodSource("isolationAndOnePhaseCommitEnabled")
  public void
      insertAndCommit_TwoPartitionsMutationsGiven_ShouldBehaveCorrectlyBasedOnStorageMutationAtomicityUnit(
          Isolation isolation, boolean onePhaseCommitEnabled)
          throws TransactionException, ExecutionException, CoordinatorException {
    if (isGroupCommitEnabled() && onePhaseCommitEnabled) {
      // Enabling both one-phase commit and group commit is not supported
      return;
    }

    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation, onePhaseCommitEnabled);
    DistributedTransaction transaction = manager.begin();

    // Act
    transaction.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, 100)
            .build());
    transaction.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 1))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, 200)
            .build());
    transaction.commit();

    // Assert
    StorageInfo storageInfo = admin.getStorageInfo(namespace1);
    switch (storageInfo.getMutationAtomicityUnit()) {
      case RECORD:
      case PARTITION:
        // twice for prepare, twice for commit
        verify(storage, times(4)).mutate(anyList());

        // commit-state should occur
        if (isGroupCommitEnabled()) {
          verify(coordinator)
              .putStateForGroupCommit(
                  anyString(), anyList(), any(TransactionState.class), anyLong());
        } else {
          verify(coordinator).putState(any(Coordinator.State.class));
        }
        break;
      case TABLE:
      case NAMESPACE:
      case STORAGE:
        if (onePhaseCommitEnabled && isolation != Isolation.SERIALIZABLE) {
          // one-phase commit, so only one mutation call
          verify(storage).mutate(anyList());

          // no commit-state should occur
          verify(coordinator, never()).putState(any(Coordinator.State.class));
        } else {
          // one for prepare, one for commit
          verify(storage, times(2)).mutate(anyList());

          // commit-state should occur
          if (isGroupCommitEnabled()) {
            verify(coordinator)
                .putStateForGroupCommit(
                    anyString(), anyList(), any(TransactionState.class), anyLong());
          } else {
            verify(coordinator).putState(any(Coordinator.State.class));
          }
        }
        break;
      default:
        throw new AssertionError();
    }

    Optional<Result> result1 =
        manager.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .build());
    assertThat(result1.isPresent()).isTrue();
    assertThat(result1.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result1.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result1.get().getInt(BALANCE)).isEqualTo(100);

    Optional<Result> result2 =
        manager.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 1))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .build());
    assertThat(result2.isPresent()).isTrue();
    assertThat(result2.get().getInt(ACCOUNT_ID)).isEqualTo(1);
    assertThat(result2.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result2.get().getInt(BALANCE)).isEqualTo(200);
  }

  @ParameterizedTest
  @MethodSource("isolationAndOnePhaseCommitEnabled")
  public void
      insertAndCommit_TwoNamespacesMutationsGiven_ShouldBehaveCorrectlyBasedOnStorageMutationAtomicityUnit(
          Isolation isolation, boolean onePhaseCommitEnabled)
          throws TransactionException, ExecutionException, CoordinatorException {
    if (isGroupCommitEnabled() && onePhaseCommitEnabled) {
      // Enabling both one-phase commit and group commit is not supported
      return;
    }

    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(isolation, onePhaseCommitEnabled);
    DistributedTransaction transaction = manager.begin();

    // Act
    transaction.insert(
        Insert.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, 100)
            .build());
    transaction.insert(
        Insert.newBuilder()
            .namespace(namespace2)
            .table(TABLE_2)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, 200)
            .build());
    transaction.commit();

    // Assert
    StorageInfo storageInfo1 = admin.getStorageInfo(namespace1);
    StorageInfo storageInfo2 = admin.getStorageInfo(namespace2);
    if (!storageInfo1.getStorageName().equals(storageInfo2.getStorageName())) {
      // different storages

      // twice for prepare, twice for commit
      verify(storage, times(4)).mutate(anyList());

      // commit-state should occur
      if (isGroupCommitEnabled()) {
        verify(coordinator)
            .putStateForGroupCommit(anyString(), anyList(), any(TransactionState.class), anyLong());
      } else {
        verify(coordinator).putState(any(Coordinator.State.class));
      }
    } else {
      // same storage
      switch (storageInfo1.getMutationAtomicityUnit()) {
        case RECORD:
        case PARTITION:
        case TABLE:
        case NAMESPACE:
          // twice for prepare, twice for commit
          verify(storage, times(4)).mutate(anyList());

          // commit-state should occur
          if (isGroupCommitEnabled()) {
            verify(coordinator)
                .putStateForGroupCommit(
                    anyString(), anyList(), any(TransactionState.class), anyLong());
          } else {
            verify(coordinator).putState(any(Coordinator.State.class));
          }
          break;
        case STORAGE:
          if (onePhaseCommitEnabled && isolation != Isolation.SERIALIZABLE) {
            // one-phase commit, so only one mutation call
            verify(storage).mutate(anyList());

            // no commit-state should occur
            verify(coordinator, never()).putState(any(Coordinator.State.class));
          } else {
            // one for prepare, one for commit
            verify(storage, times(2)).mutate(anyList());

            // commit-state should occur
            if (isGroupCommitEnabled()) {
              verify(coordinator)
                  .putStateForGroupCommit(
                      anyString(), anyList(), any(TransactionState.class), anyLong());
            } else {
              verify(coordinator).putState(any(Coordinator.State.class));
            }
          }
          break;
        default:
          throw new AssertionError();
      }
    }

    Optional<Result> result1 =
        manager.get(
            Get.newBuilder()
                .namespace(namespace1)
                .table(TABLE_1)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .build());
    assertThat(result1.isPresent()).isTrue();
    assertThat(result1.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result1.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result1.get().getInt(BALANCE)).isEqualTo(100);

    Optional<Result> result2 =
        manager.get(
            Get.newBuilder()
                .namespace(namespace2)
                .table(TABLE_2)
                .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
                .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
                .build());
    assertThat(result2.isPresent()).isTrue();
    assertThat(result2.get().getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result2.get().getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result2.get().getInt(BALANCE)).isEqualTo(200);
  }

  @Test
  @EnabledIf("isGroupCommitEnabled")
  void put_WhenTheOtherTransactionsIsDelayed_ShouldBeCommittedWithoutBlocked() throws Exception {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SNAPSHOT);

    // Act
    DistributedTransaction slowTxn = manager.begin();
    DistributedTransaction fastTxn = manager.begin();
    fastTxn.put(preparePut(0, 0, namespace1, TABLE_1));

    assertTimeout(Duration.ofSeconds(10), fastTxn::commit);

    slowTxn.put(preparePut(1, 0, namespace1, TABLE_1));
    slowTxn.commit();

    // Assert
    DistributedTransaction validationTxn = manager.beginReadOnly();
    assertThat(validationTxn.get(prepareGet(0, 0, namespace1, TABLE_1))).isPresent();
    assertThat(validationTxn.get(prepareGet(1, 0, namespace1, TABLE_1))).isPresent();
    validationTxn.commit();

    assertThat(coordinator.getState(slowTxn.getId()).get().getState())
        .isEqualTo(TransactionState.COMMITTED);
    assertThat(coordinator.getState(fastTxn.getId()).get().getState())
        .isEqualTo(TransactionState.COMMITTED);
  }

  @Test
  @EnabledIf("isGroupCommitEnabled")
  void put_WhenTheOtherTransactionsFails_ShouldBeCommittedWithoutBlocked() throws Exception {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SNAPSHOT);
    doThrow(PreparationConflictException.class).when(commit).prepareRecords(any());

    // Act
    DistributedTransaction failingTxn = manager.begin();
    DistributedTransaction successTxn = manager.begin();
    failingTxn.put(preparePut(0, 0, namespace1, TABLE_1));
    successTxn.put(preparePut(1, 0, namespace1, TABLE_1));

    // This transaction will be committed after the other transaction in the same group is removed.
    assertTimeout(
        Duration.ofSeconds(10),
        () -> {
          try {
            failingTxn.commit();
            fail();
          } catch (CommitConflictException e) {
            // Expected
          } finally {
            reset(commit);
          }
        });
    assertTimeout(Duration.ofSeconds(10), successTxn::commit);

    // Assert
    DistributedTransaction validationTxn = manager.beginReadOnly();
    assertThat(validationTxn.get(prepareGet(0, 0, namespace1, TABLE_1))).isEmpty();
    assertThat(validationTxn.get(prepareGet(1, 0, namespace1, TABLE_1))).isPresent();
    validationTxn.commit();

    assertThat(coordinator.getState(failingTxn.getId()).get().getState())
        .isEqualTo(TransactionState.ABORTED);
    assertThat(coordinator.getState(successTxn.getId()).get().getState())
        .isEqualTo(TransactionState.COMMITTED);
  }

  @Test
  @EnabledIf("isGroupCommitEnabled")
  void put_WhenTransactionFailsDueToConflict_ShouldBeAbortedWithoutBlocked() throws Exception {
    // Arrange
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);

    // Act
    DistributedTransaction failingTxn = manager.begin();
    DistributedTransaction successTxn = manager.begin();
    failingTxn.get(prepareGet(1, 0, namespace1, TABLE_1));
    failingTxn.put(preparePut(0, 0, namespace1, TABLE_1));
    successTxn.put(preparePut(1, 0, namespace1, TABLE_1));

    // This transaction will be committed after the other transaction in the same group
    // is moved to a delayed group.
    assertTimeout(Duration.ofSeconds(10), successTxn::commit);
    assertTimeout(
        Duration.ofSeconds(10),
        () -> {
          try {
            failingTxn.commit();
            fail();
          } catch (CommitConflictException e) {
            // Expected
          }
        });

    // Assert
    DistributedTransaction validationTxn = manager.beginReadOnly();
    assertThat(validationTxn.get(prepareGet(0, 0, namespace1, TABLE_1))).isEmpty();
    assertThat(validationTxn.get(prepareGet(1, 0, namespace1, TABLE_1))).isPresent();
    validationTxn.commit();

    assertThat(coordinator.getState(failingTxn.getId()).get().getState())
        .isEqualTo(TransactionState.ABORTED);
    assertThat(coordinator.getState(successTxn.getId()).get().getState())
        .isEqualTo(TransactionState.COMMITTED);
  }

  @Test
  @EnabledIf("isGroupCommitEnabled")
  void put_WhenAllTransactionsAbort_ShouldBeAbortedProperly() throws Exception {
    // Act
    ConsensusCommitManager manager = createConsensusCommitManager(Isolation.SERIALIZABLE);
    DistributedTransaction failingTxn1 = manager.begin();
    DistributedTransaction failingTxn2 = manager.begin();

    doThrow(PreparationConflictException.class).when(commit).prepareRecords(any());

    failingTxn1.put(preparePut(0, 0, namespace1, TABLE_1));
    failingTxn2.put(preparePut(1, 0, namespace1, TABLE_1));

    try {
      assertThat(catchThrowable(failingTxn1::commit)).isInstanceOf(CommitConflictException.class);
      assertThat(catchThrowable(failingTxn2::commit)).isInstanceOf(CommitConflictException.class);
    } finally {
      reset(commit);
    }

    // Assert
    DistributedTransaction validationTxn = manager.beginReadOnly();
    assertThat(validationTxn.get(prepareGet(0, 0, namespace1, TABLE_1))).isEmpty();
    assertThat(validationTxn.get(prepareGet(1, 0, namespace1, TABLE_1))).isEmpty();
    validationTxn.commit();

    assertThat(coordinator.getState(failingTxn1.getId()).get().getState())
        .isEqualTo(TransactionState.ABORTED);
    assertThat(coordinator.getState(failingTxn2.getId()).get().getState())
        .isEqualTo(TransactionState.ABORTED);
  }

  private DistributedTransaction prepareTransfer(
      ConsensusCommitManager manager,
      int fromId,
      String fromNamespace,
      String fromTable,
      int toId,
      String toNamespace,
      String toTable,
      int amount)
      throws TransactionException {
    boolean differentTables = toNamespace.equals(fromNamespace) || !toTable.equals(fromTable);

    DistributedTransaction transaction = manager.begin();

    List<Get> fromGets = prepareGets(fromNamespace, fromTable);
    List<Get> toGets = differentTables ? prepareGets(toNamespace, toTable) : fromGets;

    Optional<Result> fromResult = transaction.get(fromGets.get(fromId));
    assertThat(fromResult).isPresent();

    Optional<Result> toResult = transaction.get(toGets.get(toId));
    assertThat(toResult).isPresent();

    List<Put> fromPuts = preparePuts(fromNamespace, fromTable);
    List<Put> toPuts = differentTables ? preparePuts(toNamespace, toTable) : fromPuts;

    transaction.put(
        Put.newBuilder(fromPuts.get(fromId))
            .intValue(BALANCE, getBalance(fromResult.get()) - amount)
            .build());
    transaction.put(
        Put.newBuilder(toPuts.get(toId))
            .intValue(BALANCE, getBalance(toResult.get()) + amount)
            .build());

    return transaction;
  }

  private DistributedTransaction prepareDeletes(
      ConsensusCommitManager manager,
      int one,
      String namespace,
      String table,
      int another,
      String anotherNamespace,
      String anotherTable)
      throws TransactionException {
    boolean differentTables = !table.equals(anotherTable);

    DistributedTransaction transaction = manager.begin();

    List<Get> gets = prepareGets(namespace, table);
    List<Get> anotherGets = differentTables ? prepareGets(anotherNamespace, anotherTable) : gets;
    transaction.get(gets.get(one));
    transaction.get(anotherGets.get(another));

    List<Delete> deletes = prepareDeletes(namespace, table);
    List<Delete> anotherDeletes =
        differentTables ? prepareDeletes(anotherNamespace, anotherTable) : deletes;
    transaction.delete(deletes.get(one));
    transaction.delete(anotherDeletes.get(another));

    return transaction;
  }

  private void populateRecord(ConsensusCommitManager manager, String namespace, String table)
      throws TransactionException {
    manager.insert(
        Insert.newBuilder()
            .namespace(namespace)
            .table(table)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());
  }

  private void populateRecords(ConsensusCommitManager manager, String namespace, String table)
      throws TransactionException {
    DistributedTransaction transaction = manager.begin();
    for (int i = 0; i < NUM_ACCOUNTS; i++) {
      for (int j = 0; j < NUM_TYPES; j++) {
        Key partitionKey = Key.ofInt(ACCOUNT_ID, i);
        Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, j);
        Insert insert =
            Insert.newBuilder()
                .namespace(namespace)
                .table(table)
                .partitionKey(partitionKey)
                .clusteringKey(clusteringKey)
                .intValue(BALANCE, INITIAL_BALANCE)
                .build();
        transaction.insert(insert);
      }
    }
    transaction.commit();
  }

  private String populatePreparedRecordAndCoordinatorStateRecord(
      DistributedStorage storage,
      String namespace,
      String table,
      TransactionState recordState,
      long preparedAt,
      TransactionState coordinatorState,
      CommitType commitType)
      throws ExecutionException, CoordinatorException {
    Key partitionKey = Key.ofInt(ACCOUNT_ID, 0);
    Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, 0);

    String ongoingTxId;
    CoordinatorGroupCommitKeyManipulator keyManipulator =
        new CoordinatorGroupCommitKeyManipulator();
    if (commitType == CommitType.NORMAL_COMMIT) {
      ongoingTxId = ANY_ID_2;
    } else {
      ongoingTxId = keyManipulator.fullKey(keyManipulator.generateParentKey(), ANY_ID_2);
    }

    Put put =
        Put.newBuilder()
            .namespace(namespace)
            .table(table)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .intValue(BALANCE, NEW_BALANCE)
            .textValue(Attribute.ID, ongoingTxId)
            .intValue(Attribute.STATE, recordState.get())
            .intValue(Attribute.VERSION, 2)
            .bigIntValue(Attribute.PREPARED_AT, preparedAt)
            .intValue(Attribute.BEFORE_PREFIX + BALANCE, INITIAL_BALANCE)
            .textValue(Attribute.BEFORE_ID, ANY_ID_1)
            .intValue(Attribute.BEFORE_STATE, TransactionState.COMMITTED.get())
            .intValue(Attribute.BEFORE_VERSION, 1)
            .bigIntValue(Attribute.BEFORE_PREPARED_AT, 1)
            .bigIntValue(Attribute.BEFORE_COMMITTED_AT, 1)
            .build();
    storage.put(put);

    if (coordinatorState == null) {
      return ongoingTxId;
    }

    switch (commitType) {
      case NORMAL_COMMIT:
        Coordinator.State state = new Coordinator.State(ANY_ID_2, coordinatorState);
        coordinator.putState(state);
        break;
      case GROUP_COMMIT:
        Keys<String, String, String> keys = keyManipulator.keysFromFullKey(ongoingTxId);
        coordinator.putStateForGroupCommit(
            keys.parentKey,
            Collections.singletonList(keys.fullKey),
            coordinatorState,
            System.currentTimeMillis());
        break;
      case DELAYED_GROUP_COMMIT:
        coordinator.putState(new Coordinator.State(ongoingTxId, coordinatorState));
        break;
    }

    return ongoingTxId;
  }

  private Get prepareGet(int id, int type, String namespace, String table) {
    Key partitionKey = Key.ofInt(ACCOUNT_ID, id);
    Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, type);
    return Get.newBuilder()
        .namespace(namespace)
        .table(table)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  private List<Get> prepareGets(String namespace, String table) {
    List<Get> gets = new ArrayList<>();
    IntStream.range(0, NUM_ACCOUNTS)
        .forEach(
            i ->
                IntStream.range(0, NUM_TYPES)
                    .forEach(j -> gets.add(prepareGet(i, j, namespace, table))));
    return gets;
  }

  private Scan prepareScan(int id, int fromType, int toType, String namespace, String table) {
    Key partitionKey = Key.ofInt(ACCOUNT_ID, id);
    return Scan.newBuilder()
        .namespace(namespace)
        .table(table)
        .partitionKey(partitionKey)
        .consistency(Consistency.LINEARIZABLE)
        .start(Key.ofInt(ACCOUNT_TYPE, fromType))
        .end(Key.ofInt(ACCOUNT_TYPE, toType))
        .build();
  }

  private Scan prepareScan(int id, String namespace, String table) {
    Key partitionKey = Key.ofInt(ACCOUNT_ID, id);
    return Scan.newBuilder()
        .namespace(namespace)
        .table(table)
        .partitionKey(partitionKey)
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  private Scan prepareScanWithIndex(String namespace, String table, int balance) {
    Key indexKey = Key.ofInt(BALANCE, balance);
    return Scan.newBuilder()
        .namespace(namespace)
        .table(table)
        .indexKey(indexKey)
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  private Scan prepareScanAll(String namespace, String table) {
    return Scan.newBuilder()
        .namespace(namespace)
        .table(table)
        .all()
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  private Put preparePut(int id, int type, String namespace, String table) {
    Key partitionKey = Key.ofInt(ACCOUNT_ID, id);
    Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, type);
    return Put.newBuilder()
        .namespace(namespace)
        .table(table)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  private List<Put> preparePuts(String namespace, String table) {
    List<Put> puts = new ArrayList<>();
    IntStream.range(0, NUM_ACCOUNTS)
        .forEach(
            i ->
                IntStream.range(0, NUM_TYPES)
                    .forEach(j -> puts.add(preparePut(i, j, namespace, table))));
    return puts;
  }

  private Delete prepareDelete(int id, int type, String namespace, String table) {
    Key partitionKey = Key.ofInt(ACCOUNT_ID, id);
    Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, type);
    return Delete.newBuilder()
        .namespace(namespace)
        .table(table)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  private List<Delete> prepareDeletes(String namespace, String table) {
    List<Delete> deletes = new ArrayList<>();
    IntStream.range(0, NUM_ACCOUNTS)
        .forEach(
            i ->
                IntStream.range(0, NUM_TYPES)
                    .forEach(j -> deletes.add(prepareDelete(i, j, namespace, table))));
    return deletes;
  }

  private int getBalance(Result result) {
    assertThat(result.contains(BALANCE)).isTrue();
    return result.getInt(BALANCE);
  }

  private ConsensusCommitManager createConsensusCommitManager(Isolation isolation) {
    return createConsensusCommitManager(isolation, false);
  }

  private ConsensusCommitManager createConsensusCommitManager(
      Isolation isolation, boolean onePhaseCommitEnabled) {
    storage = spy(originalStorage);
    coordinator = spy(new Coordinator(storage, consensusCommitConfig));
    TransactionTableMetadataManager tableMetadataManager =
        new TransactionTableMetadataManager(admin, -1);
    recovery = spy(new RecoveryHandler(storage, coordinator, tableMetadataManager));
    recoveryExecutor = new RecoveryExecutor(coordinator, recovery, tableMetadataManager);
    groupCommitter = CoordinatorGroupCommitter.from(consensusCommitConfig).orElse(null);
    CrudHandler crud =
        new CrudHandler(
            storage,
            recoveryExecutor,
            tableMetadataManager,
            consensusCommitConfig.isIncludeMetadataEnabled(),
            parallelExecutor);
    commit = spy(createCommitHandler(tableMetadataManager, groupCommitter, onePhaseCommitEnabled));
    return new ConsensusCommitManager(
        storage,
        admin,
        databaseConfig,
        coordinator,
        parallelExecutor,
        recoveryExecutor,
        crud,
        commit,
        isolation,
        groupCommitter);
  }

  private CommitHandler createCommitHandler(
      TransactionTableMetadataManager tableMetadataManager,
      @Nullable CoordinatorGroupCommitter groupCommitter,
      boolean onePhaseCommitEnabled) {
    MutationsGrouper mutationsGrouper = new MutationsGrouper(new StorageInfoProvider(admin));
    if (groupCommitter != null) {
      return new CommitHandlerWithGroupCommit(
          storage,
          coordinator,
          tableMetadataManager,
          parallelExecutor,
          mutationsGrouper,
          true,
          false,
          groupCommitter);
    } else {
      return new CommitHandler(
          storage,
          coordinator,
          tableMetadataManager,
          parallelExecutor,
          mutationsGrouper,
          true,
          onePhaseCommitEnabled);
    }
  }

  private DistributedTransaction begin(ConsensusCommitManager manager, boolean readOnly) {
    if (readOnly) {
      return manager.beginReadOnly();
    } else {
      return manager.begin();
    }
  }

  private void waitForRecoveryCompletion(DistributedTransaction transaction) throws CrudException {
    if (transaction instanceof DecoratedDistributedTransaction) {
      transaction = ((DecoratedDistributedTransaction) transaction).getOriginalTransaction();
    }
    assert transaction instanceof ConsensusCommit;

    ((ConsensusCommit) transaction).waitForRecoveryCompletion();
  }

  private boolean isGroupCommitEnabled() {
    return consensusCommitConfig.isCoordinatorGroupCommitEnabled();
  }

  static Stream<Arguments> isolationAndReadOnlyMode() {
    return Arrays.stream(Isolation.values())
        .flatMap(
            isolation -> Stream.of(false, true).map(readOnly -> Arguments.of(isolation, readOnly)));
  }

  static Stream<Arguments> isolationAndCommitType() {
    return Arrays.stream(Isolation.values())
        .flatMap(
            isolation ->
                Arrays.stream(CommitType.values())
                    .map(commitType -> Arguments.of(isolation, commitType)));
  }

  static Stream<Arguments> isolationAndReadOnlyModeAndCommitType() {
    return Arrays.stream(Isolation.values())
        .flatMap(
            isolation ->
                Stream.of(false, true)
                    .flatMap(
                        readOnly ->
                            Arrays.stream(CommitType.values())
                                .map(commitType -> Arguments.of(isolation, readOnly, commitType))));
  }

  static Stream<Arguments> isolationAndOnePhaseCommitEnabled() {
    return Arrays.stream(Isolation.values())
        .flatMap(
            isolation ->
                Stream.of(false, true)
                    .map(onePhaseCommitEnabled -> Arguments.of(isolation, onePhaseCommitEnabled)));
  }

  enum CommitType {
    NORMAL_COMMIT,
    GROUP_COMMIT,
    DELAYED_GROUP_COMMIT
  }
}
