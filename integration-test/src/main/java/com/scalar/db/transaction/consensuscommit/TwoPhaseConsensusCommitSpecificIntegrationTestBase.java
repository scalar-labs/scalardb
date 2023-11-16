package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanAll;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.common.DecoratedTwoPhaseCommitTransaction;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.Coordinator.State;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TwoPhaseConsensusCommitSpecificIntegrationTestBase {

  private static final String TEST_NAME = "2pcc";
  private static final String NAMESPACE_1 = "int_test_" + TEST_NAME + "1";
  private static final String NAMESPACE_2 = "int_test_" + TEST_NAME + "2";
  private static final String TABLE_1 = "tx_test_table1";
  private static final String TABLE_2 = "tx_test_table2";
  private static final String ACCOUNT_ID = "account_id";
  private static final String ACCOUNT_TYPE = "account_type";
  private static final String BALANCE = "balance";
  private static final int INITIAL_BALANCE = 1000;
  private static final int NUM_ACCOUNTS = 4;
  private static final int NUM_TYPES = 4;
  private static final String ANY_ID_1 = "id1";
  private static final String ANY_ID_2 = "id2";

  private TwoPhaseConsensusCommitManager manager1;
  private TwoPhaseConsensusCommitManager manager2;
  private DistributedStorage storage1;
  private DistributedStorage storage2;
  private ConsensusCommitAdmin consensusCommitAdmin1;
  private ConsensusCommitAdmin consensusCommitAdmin2;
  private Coordinator coordinatorForStorage1;
  private String namespace1;
  private String namespace2;

  @BeforeAll
  public void beforeAll() throws Exception {
    initialize();
    Properties properties1 = modifyProperties(getProperties1(TEST_NAME));
    Properties properties2 = modifyProperties(getProperties2(TEST_NAME));

    namespace1 = getNamespace1();
    namespace2 = getNamespace2();
    StorageFactory factory1 = StorageFactory.create(properties1);
    StorageFactory factory2 = StorageFactory.create(properties2);
    DistributedStorageAdmin admin1 = factory1.getStorageAdmin();
    DistributedStorageAdmin admin2 = factory2.getStorageAdmin();
    DatabaseConfig databaseConfig1 = new DatabaseConfig(properties1);
    DatabaseConfig databaseConfig2 = new DatabaseConfig(properties2);
    ConsensusCommitConfig consensusCommitConfig1 = new ConsensusCommitConfig(databaseConfig1);
    ConsensusCommitConfig consensusCommitConfig2 = new ConsensusCommitConfig(databaseConfig2);
    consensusCommitAdmin1 = new ConsensusCommitAdmin(admin1, consensusCommitConfig1, false);
    consensusCommitAdmin2 = new ConsensusCommitAdmin(admin2, consensusCommitConfig2, false);
    createTables();
    storage1 = factory1.getStorage();
    storage2 = factory2.getStorage();
    manager1 = new TwoPhaseConsensusCommitManager(storage1, admin1, databaseConfig1);
    manager2 = new TwoPhaseConsensusCommitManager(storage2, admin2, databaseConfig2);
    coordinatorForStorage1 = new Coordinator(storage1, consensusCommitConfig1);
  }

  private Properties modifyProperties(Properties properties) {
    // Add testName as a coordinator namespace suffix
    String coordinatorNamespace =
        properties.getProperty(ConsensusCommitConfig.COORDINATOR_NAMESPACE, Coordinator.NAMESPACE);
    properties.setProperty(
        ConsensusCommitConfig.COORDINATOR_NAMESPACE, coordinatorNamespace + "_" + TEST_NAME);

    return properties;
  }

  protected void initialize() throws Exception {}

  protected abstract Properties getProperties1(String testName);

  protected Properties getProperties2(String testName) {
    return getProperties1(testName);
  }

  protected String getNamespace1() {
    return NAMESPACE_1;
  }

  protected String getNamespace2() {
    return NAMESPACE_2;
  }

  private void createTables() throws ExecutionException {
    Map<String, String> options = getCreationOptions();
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(ACCOUNT_ID, DataType.INT)
            .addColumn(ACCOUNT_TYPE, DataType.INT)
            .addColumn(BALANCE, DataType.INT)
            .addPartitionKey(ACCOUNT_ID)
            .addClusteringKey(ACCOUNT_TYPE)
            .build();
    consensusCommitAdmin1.createNamespace(namespace1, true, options);
    consensusCommitAdmin1.createTable(namespace1, TABLE_1, tableMetadata, true, options);
    consensusCommitAdmin1.createCoordinatorTables(true, options);
    consensusCommitAdmin2.createNamespace(namespace2, true, options);
    consensusCommitAdmin2.createTable(namespace2, TABLE_2, tableMetadata, true, options);
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  @BeforeEach
  public void setUp() throws Exception {
    truncateTables();
  }

  private void truncateTables() throws ExecutionException {
    consensusCommitAdmin1.truncateTable(namespace1, TABLE_1);
    consensusCommitAdmin1.truncateCoordinatorTables();
    consensusCommitAdmin2.truncateTable(namespace2, TABLE_2);
  }

  @AfterAll
  public void afterAll() throws Exception {
    dropTables();
    consensusCommitAdmin1.close();
    consensusCommitAdmin2.close();
    storage1.close();
    storage2.close();
    manager1.close();
    manager2.close();
  }

  private void dropTables() throws ExecutionException {
    consensusCommitAdmin1.dropTable(namespace1, TABLE_1);
    consensusCommitAdmin1.dropNamespace(namespace1);
    consensusCommitAdmin1.dropCoordinatorTables();
    consensusCommitAdmin2.dropTable(namespace2, TABLE_2);
    consensusCommitAdmin2.dropNamespace(namespace2);
  }

  @Test
  public void get_GetGivenForCommittedRecord_ShouldReturnRecord() throws TransactionException {
    // Arrange
    populate(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act
    Optional<Result> result = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction.prepare();
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isTrue();
    assertThat(getAccountId(result.get())).isEqualTo(0);
    assertThat(getAccountType(result.get())).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE);
  }

  @Test
  public void scan_ScanGivenForCommittedRecord_ShouldReturnRecord() throws TransactionException {
    // Arrange
    populate(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act
    List<Result> results = transaction.scan(prepareScan(0, 0, 0, namespace1, TABLE_1));
    transaction.prepare();
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(1);
    assertThat(getAccountId(results.get(0))).isEqualTo(0);
    assertThat(getAccountType(results.get(0))).isEqualTo(0);
    assertThat(getBalance(results.get(0))).isEqualTo(INITIAL_BALANCE);
  }

  @Test
  public void
      get_CalledTwiceAndAnotherTransactionCommitsInBetween_ShouldReturnFromSnapshotInSecondTime()
          throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act
    Optional<Result> result1 = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    populate(manager1, namespace1, TABLE_1);
    Optional<Result> result2 = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction.prepare();
    transaction.commit();

    // Assert
    assertThat(result1).isEqualTo(result2);
  }

  @Test
  public void get_GetGivenForNonExisting_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    populate(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act
    Optional<Result> result = transaction.get(prepareGet(0, 4, namespace1, TABLE_1));

    // Assert
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void scan_ScanGivenForNonExisting_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    populate(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act
    List<Result> results = transaction.scan(prepareScan(0, 4, 4, namespace1, TABLE_1));
    transaction.prepare();
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(0);
  }

  private void selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward(
      SelectionType selectionType)
      throws ExecutionException, TransactionException, CoordinatorException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecordForStorage1(
        TransactionState.PREPARED, current, TransactionState.COMMITTED);

    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act Assert
    assertThatThrownBy(
            () -> {
              if (selectionType == SelectionType.GET) {
                transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
              } else if (selectionType == SelectionType.SCAN) {
                transaction.scan(prepareScan(0, 0, 0, namespace1, TABLE_1));
              } else {
                transaction.scan(prepareScanAll(namespace1, TABLE_1));
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    Optional<Result> result;
    if (selectionType == SelectionType.GET) {
      result = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    } else {
      Scan scan =
          selectionType == SelectionType.SCAN
              ? prepareScan(0, 0, 0, namespace1, TABLE_1)
              : prepareScanAll(namespace1, TABLE_1);
      List<Result> results = transaction.scan(scan);
      assertThat(results.size()).isEqualTo(1);
      result = Optional.of(results.get(0));
    }
    transaction.prepare();
    transaction.commit();

    assertThat(result.isPresent()).isTrue();
    assertThat(getAccountId(result.get())).isEqualTo(0);
    assertThat(getAccountType(result.get())).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE); // a rolled forward value
  }

  @Test
  public void get_GetGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward(
        SelectionType.GET);
  }

  @Test
  public void scan_ScanGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward(
        SelectionType.SCAN);
  }

  private void selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback(
      SelectionType selectionType)
      throws ExecutionException, TransactionException, CoordinatorException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecordForStorage1(
        TransactionState.PREPARED, current, TransactionState.ABORTED);

    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act Assert
    assertThatThrownBy(
            () -> {
              if (selectionType == SelectionType.GET) {
                transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
              } else if (selectionType == SelectionType.SCAN) {
                transaction.scan(prepareScan(0, 0, 0, namespace1, TABLE_1));
              } else {
                transaction.scan(prepareScanAll(namespace1, TABLE_1));
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    Optional<Result> result;
    if (selectionType == SelectionType.GET) {
      result = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    } else {
      Scan scan =
          selectionType == SelectionType.SCAN
              ? prepareScan(0, 0, 0, namespace1, TABLE_1)
              : prepareScanAll(namespace1, TABLE_1);
      List<Result> results = transaction.scan(scan);
      assertThat(results.size()).isEqualTo(1);
      result = Optional.of(results.get(0));
    }
    transaction.prepare();
    transaction.commit();

    assertThat(result.isPresent()).isTrue();
    assertThat(getAccountId(result.get())).isEqualTo(0);
    assertThat(getAccountType(result.get())).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(0); // a rolled back value
  }

  @Test
  public void get_GetGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback()
      throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback(
        SelectionType.GET);
  }

  @Test
  public void scan_ScanGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback()
      throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback(
        SelectionType.SCAN);
  }

  private void
      selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
          SelectionType selectionType)
          throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    long prepared_at = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecordForStorage1(
        TransactionState.PREPARED, prepared_at, null);

    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act Assert
    assertThatThrownBy(
            () -> {
              if (selectionType == SelectionType.GET) {
                transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
              } else if (selectionType == SelectionType.SCAN) {
                transaction.scan(prepareScan(0, 0, 0, namespace1, TABLE_1));
              } else {
                transaction.scan(prepareScanAll(namespace1, TABLE_1));
              }
            })
        .isInstanceOf(UncommittedRecordException.class);
    transaction.prepare();
    transaction.commit();

    // Assert
    assertThat(coordinatorForStorage1.getState(ANY_ID_2).isPresent()).isFalse();
  }

  @Test
  public void
      get_GetGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws ExecutionException, CoordinatorException, TransactionException {
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        SelectionType.GET);
  }

  @Test
  public void
      scan_ScanGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws ExecutionException, CoordinatorException, TransactionException {
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        SelectionType.SCAN);
  }

  private void
      selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
          SelectionType selectionType)
          throws ExecutionException, TransactionException, CoordinatorException {
    // Arrange
    long prepared_at = System.currentTimeMillis() - RecoveryHandler.TRANSACTION_LIFETIME_MILLIS;
    populatePreparedRecordAndCoordinatorStateRecordForStorage1(
        TransactionState.PREPARED, prepared_at, null);

    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act Assert
    assertThatThrownBy(
            () -> {
              if (selectionType == SelectionType.GET) {
                transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
              } else if (selectionType == SelectionType.SCAN) {
                transaction.scan(prepareScan(0, 0, 0, namespace1, TABLE_1));
              } else {
                transaction.scan(prepareScanAll(namespace1, TABLE_1));
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    Optional<Result> result;
    if (selectionType == SelectionType.GET) {
      result = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    } else {
      Scan scan =
          selectionType == SelectionType.SCAN
              ? prepareScan(0, 0, 0, namespace1, TABLE_1)
              : prepareScanAll(namespace1, TABLE_1);
      List<Result> results = transaction.scan(scan);
      assertThat(results.size()).isEqualTo(1);
      result = Optional.of(results.get(0));
    }
    transaction.prepare();
    transaction.commit();

    assertThat(result.isPresent()).isTrue();
    assertThat(getAccountId(result.get())).isEqualTo(0);
    assertThat(getAccountType(result.get())).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(0); // a rolled back value

    assertThat(coordinatorForStorage1.getState(ANY_ID_2).isPresent()).isTrue();
    assertThat(coordinatorForStorage1.getState(ANY_ID_2).get().getState())
        .isEqualTo(TransactionState.ABORTED);
  }

  @Test
  public void get_GetGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
      throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        SelectionType.GET);
  }

  @Test
  public void
      scan_ScanGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
          throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        SelectionType.SCAN);
  }

  private void
      selection_SelectionGivenForPreparedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly(
          SelectionType selectionType)
          throws ExecutionException, TransactionException, CoordinatorException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecordForStorage1(
        TransactionState.PREPARED, current, TransactionState.COMMITTED);

    TwoPhaseConsensusCommit transaction =
        (TwoPhaseConsensusCommit)
            ((DecoratedTwoPhaseCommitTransaction) manager1.begin()).getOriginalTransaction();

    transaction.setBeforeRecoveryHook(
        () ->
            assertThatThrownBy(
                    () -> {
                      TwoPhaseCommitTransaction another = manager1.begin();
                      if (selectionType == SelectionType.GET) {
                        another.get(prepareGet(0, 0, namespace1, TABLE_1));
                      } else if (selectionType == SelectionType.SCAN) {
                        another.scan(prepareScan(0, 0, 0, namespace1, TABLE_1));
                      } else {
                        another.scan(prepareScanAll(namespace1, TABLE_1));
                      }
                    })
                .isInstanceOf(UncommittedRecordException.class));

    // Act Assert
    assertThatThrownBy(
            () -> {
              if (selectionType == SelectionType.GET) {
                transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
              } else if (selectionType == SelectionType.SCAN) {
                transaction.scan(prepareScan(0, 0, 0, namespace1, TABLE_1));
              } else {
                transaction.scan(prepareScanAll(namespace1, TABLE_1));
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    Optional<Result> result;
    if (selectionType == SelectionType.GET) {
      result = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    } else {
      Scan scan =
          selectionType == SelectionType.SCAN
              ? prepareScan(0, 0, 0, namespace1, TABLE_1)
              : prepareScanAll(namespace1, TABLE_1);
      List<Result> results = transaction.scan(scan);
      assertThat(results.size()).isEqualTo(1);
      result = Optional.of(results.get(0));
    }
    transaction.prepare();
    transaction.commit();

    assertThat(result.isPresent()).isTrue();
    assertThat(getAccountId(result.get())).isEqualTo(0);
    assertThat(getAccountType(result.get())).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE); // a rolled forward value
  }

  @Test
  public void
      get_GetGivenForPreparedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly()
          throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly(
        SelectionType.GET);
  }

  @Test
  public void
      scan_ScanGivenForPreparedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly()
          throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly(
        SelectionType.SCAN);
  }

  private void
      selection_SelectionGivenForPreparedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly(
          SelectionType selectionType)
          throws ExecutionException, TransactionException, CoordinatorException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecordForStorage1(
        TransactionState.PREPARED, current, TransactionState.ABORTED);

    TwoPhaseConsensusCommit transaction =
        (TwoPhaseConsensusCommit)
            ((DecoratedTwoPhaseCommitTransaction) manager1.begin()).getOriginalTransaction();

    transaction.setBeforeRecoveryHook(
        () ->
            assertThatThrownBy(
                    () -> {
                      TwoPhaseCommitTransaction another = manager1.begin();
                      if (selectionType == SelectionType.GET) {
                        another.get(prepareGet(0, 0, namespace1, TABLE_1));
                      } else if (selectionType == SelectionType.SCAN) {
                        another.scan(prepareScan(0, 0, 0, namespace1, TABLE_1));
                      } else {
                        another.scan(prepareScanAll(namespace1, TABLE_1));
                      }
                    })
                .isInstanceOf(UncommittedRecordException.class));

    // Act Assert
    assertThatThrownBy(
            () -> {
              if (selectionType == SelectionType.GET) {
                transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
              } else if (selectionType == SelectionType.SCAN) {
                transaction.scan(prepareScan(0, 0, 0, namespace1, TABLE_1));
              } else {
                transaction.scan(prepareScanAll(namespace1, TABLE_1));
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    Optional<Result> result;
    if (selectionType == SelectionType.GET) {
      result = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    } else {
      Scan scan =
          selectionType == SelectionType.SCAN
              ? prepareScan(0, 0, 0, namespace1, TABLE_1)
              : prepareScanAll(namespace1, TABLE_1);
      List<Result> results = transaction.scan(scan);
      assertThat(results.size()).isEqualTo(1);
      result = Optional.of(results.get(0));
    }
    transaction.prepare();
    transaction.commit();

    assertThat(result.isPresent()).isTrue();
    assertThat(getAccountId(result.get())).isEqualTo(0);
    assertThat(getAccountType(result.get())).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(0); // a rolled back value
  }

  @Test
  public void
      get_GetGivenForPreparedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly()
          throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForPreparedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly(
        SelectionType.GET);
  }

  @Test
  public void
      scan_ScanGivenForPreparedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly()
          throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForPreparedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly(
        SelectionType.SCAN);
  }

  private void selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward(
      SelectionType selectionType)
      throws ExecutionException, TransactionException, CoordinatorException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecordForStorage1(
        TransactionState.DELETED, current, TransactionState.COMMITTED);

    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act Assert
    assertThatThrownBy(
            () -> {
              if (selectionType == SelectionType.GET) {
                transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
              } else if (selectionType == SelectionType.SCAN) {
                transaction.scan(prepareScan(0, 0, 0, namespace1, TABLE_1));
              } else {
                transaction.scan(prepareScanAll(namespace1, TABLE_1));
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    if (selectionType == SelectionType.GET) {
      Optional<Result> result = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
      assertThat(result.isPresent()).isFalse(); // deleted
    } else {
      Scan scan =
          selectionType == SelectionType.SCAN
              ? prepareScan(0, 0, 0, namespace1, TABLE_1)
              : prepareScanAll(namespace1, TABLE_1);
      List<Result> results = transaction.scan(scan);
      assertThat(results.size()).isEqualTo(0); // deleted
    }
    transaction.prepare();
    transaction.commit();
  }

  @Test
  public void get_GetGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward(
        SelectionType.GET);
  }

  @Test
  public void scan_ScanGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward(
        SelectionType.SCAN);
  }

  private void selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback(
      SelectionType selectionType)
      throws ExecutionException, TransactionException, CoordinatorException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecordForStorage1(
        TransactionState.DELETED, current, TransactionState.ABORTED);

    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act Assert
    assertThatThrownBy(
            () -> {
              if (selectionType == SelectionType.GET) {
                transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
              } else if (selectionType == SelectionType.SCAN) {
                transaction.scan(prepareScan(0, 0, 0, namespace1, TABLE_1));
              } else {
                transaction.scan(prepareScanAll(namespace1, TABLE_1));
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    Optional<Result> result;
    if (selectionType == SelectionType.GET) {
      result = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    } else {
      Scan scan =
          selectionType == SelectionType.SCAN
              ? prepareScan(0, 0, 0, namespace1, TABLE_1)
              : prepareScanAll(namespace1, TABLE_1);
      List<Result> results = transaction.scan(scan);
      assertThat(results.size()).isEqualTo(1);
      result = Optional.of(results.get(0));
    }
    transaction.prepare();
    transaction.commit();

    assertThat(result.isPresent()).isTrue();
    assertThat(getAccountId(result.get())).isEqualTo(0);
    assertThat(getAccountType(result.get())).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(0); // a rolled back value
  }

  @Test
  public void get_GetGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback()
      throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback(SelectionType.GET);
  }

  @Test
  public void scan_ScanGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback()
      throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback(
        SelectionType.SCAN);
  }

  private void
      selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
          SelectionType selectionType)
          throws ExecutionException, CoordinatorException, TransactionException {
    // Arrange
    long prepared_at = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecordForStorage1(
        TransactionState.DELETED, prepared_at, null);

    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act Assert
    assertThatThrownBy(
            () -> {
              if (selectionType == SelectionType.GET) {
                transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
              } else if (selectionType == SelectionType.SCAN) {
                transaction.scan(prepareScan(0, 0, 0, namespace1, TABLE_1));
              } else {
                transaction.scan(prepareScanAll(namespace1, TABLE_1));
              }
            })
        .isInstanceOf(UncommittedRecordException.class);
    transaction.prepare();
    transaction.commit();

    // Assert
    assertThat(coordinatorForStorage1.getState(ANY_ID_2).isPresent()).isFalse();
  }

  @Test
  public void
      get_GetGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws ExecutionException, CoordinatorException, TransactionException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        SelectionType.GET);
  }

  @Test
  public void
      scan_ScanGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws ExecutionException, CoordinatorException, TransactionException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        SelectionType.SCAN);
  }

  private void
      selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
          SelectionType selectionType)
          throws ExecutionException, TransactionException, CoordinatorException {
    // Arrange
    long prepared_at = System.currentTimeMillis() - RecoveryHandler.TRANSACTION_LIFETIME_MILLIS;
    populatePreparedRecordAndCoordinatorStateRecordForStorage1(
        TransactionState.DELETED, prepared_at, null);

    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act Assert
    assertThatThrownBy(
            () -> {
              if (selectionType == SelectionType.GET) {
                transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
              } else if (selectionType == SelectionType.SCAN) {
                transaction.scan(prepareScan(0, 0, 0, namespace1, TABLE_1));
              } else {
                transaction.scan(prepareScanAll(namespace1, TABLE_1));
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    Optional<Result> result;
    if (selectionType == SelectionType.GET) {
      result = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    } else {
      Scan scan =
          selectionType == SelectionType.SCAN
              ? prepareScan(0, 0, 0, namespace1, TABLE_1)
              : prepareScanAll(namespace1, TABLE_1);
      List<Result> results = transaction.scan(scan);
      assertThat(results.size()).isEqualTo(1);
      result = Optional.of(results.get(0));
    }
    transaction.prepare();
    transaction.commit();

    assertThat(result.isPresent()).isTrue();
    assertThat(getAccountId(result.get())).isEqualTo(0);
    assertThat(getAccountType(result.get())).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(0); // a rolled back value

    assertThat(coordinatorForStorage1.getState(ANY_ID_2).isPresent()).isTrue();
    assertThat(coordinatorForStorage1.getState(ANY_ID_2).get().getState())
        .isEqualTo(TransactionState.ABORTED);
  }

  @Test
  public void get_GetGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
      throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        SelectionType.GET);
  }

  @Test
  public void
      scan_ScanGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
          throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        SelectionType.SCAN);
  }

  private void
      selection_SelectionGivenForDeletedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly(
          SelectionType selectionType)
          throws ExecutionException, TransactionException, CoordinatorException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecordForStorage1(
        TransactionState.DELETED, current, TransactionState.COMMITTED);

    TwoPhaseConsensusCommit transaction =
        (TwoPhaseConsensusCommit)
            ((DecoratedTwoPhaseCommitTransaction) manager1.begin()).getOriginalTransaction();

    transaction.setBeforeRecoveryHook(
        () ->
            assertThatThrownBy(
                    () -> {
                      TwoPhaseCommitTransaction another = manager1.begin();
                      if (selectionType == SelectionType.GET) {
                        another.get(prepareGet(0, 0, namespace1, TABLE_1));
                      } else if (selectionType == SelectionType.SCAN) {
                        another.scan(prepareScan(0, 0, 0, namespace1, TABLE_1));
                      } else {
                        another.scan(prepareScanAll(namespace1, TABLE_1));
                      }
                    })
                .isInstanceOf(UncommittedRecordException.class));

    // Act Assert
    assertThatThrownBy(
            () -> {
              if (selectionType == SelectionType.GET) {
                transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
              } else if (selectionType == SelectionType.SCAN) {
                transaction.scan(prepareScan(0, 0, 0, namespace1, TABLE_1));
              } else {
                transaction.scan(prepareScanAll(namespace1, TABLE_1));
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    if (selectionType == SelectionType.GET) {
      Optional<Result> result = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
      assertThat(result.isPresent()).isFalse(); // deleted
    } else {
      Scan scan =
          selectionType == SelectionType.SCAN
              ? prepareScan(0, 0, 0, namespace1, TABLE_1)
              : prepareScanAll(namespace1, TABLE_1);
      List<Result> results = transaction.scan(scan);
      assertThat(results.size()).isEqualTo(0); // deleted
    }
    transaction.prepare();
    transaction.commit();
  }

  @Test
  public void
      get_GetGivenForDeletedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly()
          throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly(
        SelectionType.GET);
  }

  @Test
  public void
      scan_ScanGivenForDeletedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly()
          throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly(
        SelectionType.SCAN);
  }

  private void
      selection_SelectionGivenForDeletedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly(
          SelectionType selectionType)
          throws ExecutionException, TransactionException, CoordinatorException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecordForStorage1(
        TransactionState.DELETED, current, TransactionState.ABORTED);

    TwoPhaseConsensusCommit transaction =
        (TwoPhaseConsensusCommit)
            ((DecoratedTwoPhaseCommitTransaction) manager1.begin()).getOriginalTransaction();

    transaction.setBeforeRecoveryHook(
        () ->
            assertThatThrownBy(
                    () -> {
                      TwoPhaseCommitTransaction another = manager1.begin();
                      if (selectionType == SelectionType.GET) {
                        another.get(prepareGet(0, 0, namespace1, TABLE_1));
                      } else if (selectionType == SelectionType.SCAN) {
                        another.scan(prepareScan(0, 0, 0, namespace1, TABLE_1));
                      } else {
                        another.scan(prepareScanAll(namespace1, TABLE_1));
                      }
                    })
                .isInstanceOf(UncommittedRecordException.class));

    // Act Assert
    assertThatThrownBy(
            () -> {
              if (selectionType == SelectionType.GET) {
                transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
              } else if (selectionType == SelectionType.SCAN) {
                transaction.scan(prepareScan(0, 0, 0, namespace1, TABLE_1));
              } else {
                transaction.scan(prepareScanAll(namespace1, TABLE_1));
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    Optional<Result> result;
    if (selectionType == SelectionType.GET) {
      result = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    } else {
      Scan scan =
          selectionType == SelectionType.SCAN
              ? prepareScan(0, 0, 0, namespace1, TABLE_1)
              : prepareScanAll(namespace1, TABLE_1);
      List<Result> results = transaction.scan(scan);
      assertThat(results.size()).isEqualTo(1);
      result = Optional.of(results.get(0));
    }
    transaction.prepare();
    transaction.commit();

    assertThat(result.isPresent()).isTrue();
    assertThat(getAccountId(result.get())).isEqualTo(0);
    assertThat(getAccountType(result.get())).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(0); // a rolled back value
  }

  @Test
  public void
      get_GetGivenForDeletedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly()
          throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly(
        SelectionType.GET);
  }

  @Test
  public void
      scan_ScanGivenForDeletedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly()
          throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly(
        SelectionType.SCAN);
  }

  @Test
  public void getAndScan_CommitHappenedInBetween_ShouldReadRepeatably()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.commit();

    TwoPhaseCommitTransaction transaction1 = manager1.begin();
    Optional<Result> result1 = transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));

    TwoPhaseCommitTransaction transaction2 = manager1.begin();
    transaction2.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction2.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 2));
    transaction2.prepare();
    transaction2.commit();

    // Act
    Result result2 = transaction1.scan(prepareScan(0, 0, 0, namespace1, TABLE_1)).get(0);
    Optional<Result> result3 = transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction1.prepare();
    transaction1.commit();

    // Assert
    assertThat(result1).isPresent();
    assertThat(result1.get()).isEqualTo(result2);
    assertThat(result1).isEqualTo(result3);
  }

  @Test
  public void putAndCommit_PutGivenForNonExisting_ShouldCreateRecord() throws TransactionException {
    // Arrange
    int expected = INITIAL_BALANCE;
    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, expected));
    transaction.prepare();
    transaction.commit();

    // Assert
    TwoPhaseCommitTransaction another = manager1.begin();
    Optional<Result> result = another.get(prepareGet(0, 0, namespace1, TABLE_1));
    another.prepare();
    another.commit();

    assertThat(result).isPresent();
    assertThat(getAccountId(result.get())).isEqualTo(0);
    assertThat(getAccountType(result.get())).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(expected);
  }

  @Test
  public void putAndCommit_PutGivenForExistingAfterRead_ShouldUpdateRecord()
      throws TransactionException {
    // Arrange
    populate(manager1, namespace1, TABLE_1);

    Get get = prepareGet(0, 0, namespace1, TABLE_1);

    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act
    Optional<Result> result = transaction.get(get);
    assertThat(result.isPresent()).isTrue();

    int afterBalance = getBalance(result.get()) + 100;
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, afterBalance));
    transaction.prepare();
    transaction.commit();

    // Assert
    TwoPhaseCommitTransaction another = manager1.begin();
    result = another.get(get);
    another.prepare();
    another.commit();

    assertThat(result).isPresent();
    assertThat(getAccountId(result.get())).isEqualTo(0);
    assertThat(getAccountType(result.get())).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(afterBalance);
  }

  @Test
  public void putAndCommit_PutGivenForExistingAndNeverRead_ShouldUpdateRecord()
      throws TransactionException {
    // Arrange
    populate(manager1, namespace1, TABLE_1);

    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act
    int afterBalance = INITIAL_BALANCE + 100;
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, afterBalance));
    transaction.prepare();
    transaction.commit();

    // Assert
    TwoPhaseCommitTransaction another = manager1.begin();
    Optional<Result> result = another.get(prepareGet(0, 0, namespace1, TABLE_1));
    another.prepare();
    another.commit();

    assertThat(result).isPresent();
    assertThat(getAccountId(result.get())).isEqualTo(0);
    assertThat(getAccountType(result.get())).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(afterBalance);
  }

  @Test
  public void putAndCommit_GetsAndPutsGiven_ShouldCommitProperly() throws TransactionException {
    // Arrange
    populate(manager1, namespace1, TABLE_1);
    populate(manager2, namespace2, TABLE_2);

    int amount = 100;
    int fromBalance = INITIAL_BALANCE - amount;
    int toBalance = INITIAL_BALANCE + amount;
    int fromId = 0;
    int fromType = 0;
    int toId = 1;
    int toType = 0;

    TwoPhaseCommitTransaction fromTx = manager1.begin();
    TwoPhaseCommitTransaction toTx = manager2.join(fromTx.getId());
    transfer(
        fromId,
        fromType,
        namespace1,
        TABLE_1,
        fromTx,
        toId,
        toType,
        namespace2,
        TABLE_2,
        toTx,
        amount);

    // Act Assert
    assertThatCode(
            () -> {
              fromTx.prepare();
              toTx.prepare();
              fromTx.commit();
              toTx.commit();
            })
        .doesNotThrowAnyException();

    // Assert
    TwoPhaseCommitTransaction another1 = manager1.begin();
    TwoPhaseCommitTransaction another2 = manager2.join(another1.getId());

    Optional<Result> result = another1.get(prepareGet(fromId, fromType, namespace1, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(fromBalance);

    result = another2.get(prepareGet(toId, toType, namespace2, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(toBalance);

    another1.prepare();
    another2.prepare();
    another1.commit();
    another2.commit();
  }

  @Test
  public void commit_ConflictingPutsGivenForNonExisting_ShouldCommitOneAndAbortTheOther()
      throws TransactionException {
    // Arrange
    int expected = INITIAL_BALANCE;
    int fromId = 0;
    int fromType = 0;
    int toId = 1;
    int toType = 0;
    int anotherFromId = toId;
    int anotherFromType = toType;
    int anotherToId = 2;
    int anotherToType = 0;

    TwoPhaseCommitTransaction fromTx = manager1.begin();
    TwoPhaseCommitTransaction toTx = manager2.join(fromTx.getId());
    fromTx.get(prepareGet(fromId, fromType, namespace1, TABLE_1));
    toTx.get(prepareGet(toId, toType, namespace2, TABLE_2));
    fromTx.put(preparePut(fromId, fromType, namespace1, TABLE_1).withValue(BALANCE, expected));
    toTx.put(preparePut(toId, toType, namespace2, TABLE_2).withValue(BALANCE, expected));

    // Act Assert
    assertThatCode(
            () -> {
              TwoPhaseCommitTransaction anotherFromTx = manager2.begin();
              TwoPhaseCommitTransaction anotherToTx = manager1.join(anotherFromTx.getId());
              anotherFromTx.put(
                  preparePut(anotherFromId, anotherFromType, namespace2, TABLE_2)
                      .withValue(BALANCE, expected));
              anotherToTx.put(
                  preparePut(anotherToId, anotherToType, namespace1, TABLE_1)
                      .withValue(BALANCE, expected));
              anotherFromTx.prepare();
              anotherToTx.prepare();
              anotherFromTx.commit();
              anotherToTx.commit();
            })
        .doesNotThrowAnyException();

    assertThatThrownBy(
            () -> {
              fromTx.prepare();
              toTx.prepare();
            })
        .isInstanceOf(PreparationException.class);
    fromTx.rollback();
    toTx.rollback();

    // Assert
    TwoPhaseCommitTransaction another1 = manager1.begin();
    TwoPhaseCommitTransaction another2 = manager2.join(another1.getId());

    Optional<Result> result = another1.get(prepareGet(fromId, fromType, namespace1, TABLE_1));
    assertThat(result).isNotPresent();

    result = another2.get(prepareGet(toId, toType, namespace2, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(expected);

    result = another1.get(prepareGet(anotherToId, anotherToType, namespace1, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(expected);

    another1.prepare();
    another2.prepare();
    another1.commit();
    another2.commit();
  }

  @Test
  public void commit_ConflictingPutAndDeleteGivenForExisting_ShouldCommitPutAndAbortDelete()
      throws TransactionException {
    // Arrange
    int amount = 200;
    int fromId = 0;
    int fromType = 0;
    int toId = 1;
    int toType = 0;
    int anotherFromId = toId;
    int anotherFromType = toType;
    int anotherToId = 2;
    int anotherToType = 0;

    populate(manager1, namespace1, TABLE_1);
    populate(manager2, namespace2, TABLE_2);

    TwoPhaseCommitTransaction fromTx = manager1.begin();
    TwoPhaseCommitTransaction toTx = manager2.join(fromTx.getId());
    fromTx.get(prepareGet(fromId, fromType, namespace1, TABLE_1));
    fromTx.delete(prepareDelete(fromId, fromType, namespace1, TABLE_1));
    toTx.get(prepareGet(toId, toType, namespace2, TABLE_2));
    toTx.delete(prepareDelete(toId, toType, namespace2, TABLE_2));

    // Act Assert
    assertThatCode(
            () -> {
              TwoPhaseCommitTransaction anotherFromTx = manager2.begin();
              TwoPhaseCommitTransaction anotherToTx = manager1.join(anotherFromTx.getId());
              transfer(
                  anotherFromId,
                  anotherFromType,
                  namespace2,
                  TABLE_2,
                  anotherFromTx,
                  anotherToId,
                  anotherToType,
                  namespace1,
                  TABLE_1,
                  anotherToTx,
                  amount);

              anotherFromTx.prepare();
              anotherToTx.prepare();
              anotherFromTx.commit();
              anotherToTx.commit();
            })
        .doesNotThrowAnyException();

    assertThatThrownBy(
            () -> {
              fromTx.prepare();
              toTx.prepare();
            })
        .isInstanceOf(PreparationException.class);
    fromTx.rollback();
    toTx.rollback();

    // Assert
    TwoPhaseCommitTransaction another1 = manager1.begin();
    TwoPhaseCommitTransaction another2 = manager2.join(another1.getId());

    Optional<Result> result = another1.get(prepareGet(fromId, fromType, namespace1, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE);

    result = another2.get(prepareGet(anotherFromId, anotherFromType, namespace2, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE - amount);

    result = another1.get(prepareGet(anotherToId, anotherToType, namespace1, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE + amount);

    another1.prepare();
    another2.prepare();
    another1.commit();
    another2.commit();
  }

  @Test
  public void commit_ConflictingPutsGivenForExisting_ShouldCommitOneAndAbortTheOther()
      throws TransactionException {
    // Arrange
    int amount1 = 100;
    int amount2 = 200;
    int fromId = 0;
    int fromType = 0;
    int toId = 1;
    int toType = 0;
    int anotherFromId = toId;
    int anotherFromType = toType;
    int anotherToId = 2;
    int anotherToType = 0;

    populate(manager1, namespace1, TABLE_1);
    populate(manager2, namespace2, TABLE_2);

    TwoPhaseCommitTransaction fromTx = manager1.begin();
    TwoPhaseCommitTransaction toTx = manager2.join(fromTx.getId());
    transfer(
        fromId,
        fromType,
        namespace1,
        TABLE_1,
        fromTx,
        toId,
        toType,
        namespace2,
        TABLE_2,
        toTx,
        amount1);

    // Act Assert
    assertThatCode(
            () -> {
              TwoPhaseCommitTransaction anotherFromTx = manager2.begin();
              TwoPhaseCommitTransaction anotherToTx = manager1.join(anotherFromTx.getId());
              transfer(
                  anotherFromId,
                  anotherFromType,
                  namespace2,
                  TABLE_2,
                  anotherFromTx,
                  anotherToId,
                  anotherToType,
                  namespace1,
                  TABLE_1,
                  anotherToTx,
                  amount2);

              anotherFromTx.prepare();
              anotherToTx.prepare();
              anotherFromTx.commit();
              anotherToTx.commit();
            })
        .doesNotThrowAnyException();

    assertThatThrownBy(
            () -> {
              fromTx.prepare();
              toTx.prepare();
            })
        .isInstanceOf(PreparationException.class);
    fromTx.rollback();
    toTx.rollback();

    // Assert
    TwoPhaseCommitTransaction another1 = manager1.begin();
    TwoPhaseCommitTransaction another2 = manager2.join(another1.getId());

    Optional<Result> result = another1.get(prepareGet(fromId, fromType, namespace1, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE);

    result = another2.get(prepareGet(anotherFromId, anotherFromType, namespace2, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE - amount2);

    result = another1.get(prepareGet(anotherToId, anotherToType, namespace1, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE + amount2);

    another1.prepare();
    another2.prepare();
    another1.commit();
    another2.commit();
  }

  @Test
  public void commit_NonConflictingPutsGivenForExisting_ShouldCommitBoth()
      throws TransactionException {
    // Arrange
    int amount1 = 100;
    int amount2 = 200;
    int fromId = 0;
    int fromType = 0;
    int toId = 1;
    int toType = 0;
    int anotherFromId = 2;
    int anotherFromType = 0;
    int anotherToId = 3;
    int anotherToType = 0;

    populate(manager1, namespace1, TABLE_1);
    populate(manager2, namespace2, TABLE_2);

    TwoPhaseCommitTransaction fromTx = manager1.begin();
    TwoPhaseCommitTransaction toTx = manager2.join(fromTx.getId());
    transfer(
        fromId,
        fromType,
        namespace1,
        TABLE_1,
        fromTx,
        toId,
        toType,
        namespace2,
        TABLE_2,
        toTx,
        amount1);

    // Act Assert
    assertThatCode(
            () -> {
              TwoPhaseCommitTransaction anotherFromTx = manager2.begin();
              TwoPhaseCommitTransaction anotherToTx = manager1.join(anotherFromTx.getId());
              transfer(
                  anotherFromId,
                  anotherFromType,
                  namespace2,
                  TABLE_2,
                  anotherFromTx,
                  anotherToId,
                  anotherToType,
                  namespace1,
                  TABLE_1,
                  anotherToTx,
                  amount2);

              anotherFromTx.prepare();
              anotherToTx.prepare();
              anotherFromTx.commit();
              anotherToTx.commit();
            })
        .doesNotThrowAnyException();

    assertThatCode(
            () -> {
              fromTx.prepare();
              toTx.prepare();
              fromTx.commit();
              toTx.commit();
            })
        .doesNotThrowAnyException();

    // Assert
    TwoPhaseCommitTransaction another1 = manager1.begin();
    TwoPhaseCommitTransaction another2 = manager2.join(another1.getId());

    Optional<Result> result = another1.get(prepareGet(fromId, fromType, namespace1, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE - amount1);

    result = another2.get(prepareGet(toId, toType, namespace2, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE + amount1);

    result = another2.get(prepareGet(anotherFromId, anotherFromType, namespace2, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE - amount2);

    result = another1.get(prepareGet(anotherToId, anotherToType, namespace1, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE + amount2);

    another1.prepare();
    another2.prepare();
    another1.commit();
    another2.commit();
  }

  @Test
  public void putAndCommit_GetsAndPutsForSameKeyButDifferentTablesGiven_ShouldCommitBoth()
      throws TransactionException {
    // Arrange
    int expected = INITIAL_BALANCE;
    int fromId = 0;
    int fromType = 0;
    int toId = 1;
    int toType = 0;
    int anotherFromId = fromId;
    int anotherFromType = fromType;
    int anotherToId = toId;
    int anotherToType = toType;

    TwoPhaseCommitTransaction fromTx = manager1.begin();
    TwoPhaseCommitTransaction toTx = manager2.join(fromTx.getId());
    fromTx.put(preparePut(fromId, fromType, namespace1, TABLE_1).withValue(BALANCE, expected));
    toTx.put(preparePut(toId, toType, namespace2, TABLE_2).withValue(BALANCE, expected));

    // Act Assert
    assertThatCode(
            () -> {
              TwoPhaseCommitTransaction anotherFromTx = manager2.begin();
              TwoPhaseCommitTransaction anotherToTx = manager1.join(anotherFromTx.getId());
              anotherFromTx.put(
                  preparePut(anotherFromId, anotherFromType, namespace2, TABLE_2)
                      .withValue(BALANCE, expected));
              anotherToTx.put(
                  preparePut(anotherToId, anotherToType, namespace1, TABLE_1)
                      .withValue(BALANCE, expected));

              anotherFromTx.prepare();
              anotherToTx.prepare();
              anotherFromTx.commit();
              anotherToTx.commit();
            })
        .doesNotThrowAnyException();

    assertThatCode(
            () -> {
              fromTx.prepare();
              toTx.prepare();
              fromTx.commit();
              toTx.commit();
            })
        .doesNotThrowAnyException();

    // Assert
    TwoPhaseCommitTransaction another1 = manager1.begin();
    TwoPhaseCommitTransaction another2 = manager2.join(another1.getId());

    Optional<Result> result = another1.get(prepareGet(fromId, fromType, namespace1, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(expected);

    result = another2.get(prepareGet(toId, toType, namespace2, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(expected);

    result = another2.get(prepareGet(anotherFromId, anotherFromType, namespace2, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(expected);

    result = another1.get(prepareGet(anotherToId, anotherToType, namespace1, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(expected);

    another1.prepare();
    another2.prepare();
    another1.commit();
    another2.commit();
  }

  @Test
  public void prepare_DeleteGivenWithoutRead_ShouldNotThrowAnyExceptions()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();
    transaction.delete(prepareDelete(0, 0, namespace1, TABLE_1));

    // Act Assert
    assertThatCode(
            () -> {
              transaction.prepare();
              transaction.commit();
            })
        .doesNotThrowAnyException();
  }

  @Test
  public void prepare_DeleteGivenForNonExisting_ShouldNotThrowAnyExceptions()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();
    transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction.delete(prepareDelete(0, 0, namespace1, TABLE_1));

    // Act Assert
    assertThatCode(
            () -> {
              transaction.prepare();
              transaction.commit();
            })
        .doesNotThrowAnyException();
  }

  @Test
  public void commit_DeleteGivenForExistingAfterRead_ShouldDeleteRecord()
      throws TransactionException {
    // Arrange
    populate(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act
    Optional<Result> result = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    transaction.prepare();
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isTrue();

    TwoPhaseCommitTransaction another = manager1.begin();
    result = another.get(prepareGet(0, 0, namespace1, TABLE_1));
    another.prepare();
    another.commit();

    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void commit_ConflictingDeletesGivenForExisting_ShouldCommitOneAndAbortTheOther()
      throws TransactionException {
    // Arrange
    int account1Id = 0;
    int account1Type = 0;
    int account2Id = 1;
    int account2Type = 0;
    int account3Id = 2;
    int account3Type = 0;

    populate(manager1, namespace1, TABLE_1);
    populate(manager2, namespace2, TABLE_2);

    TwoPhaseCommitTransaction tx1 = manager1.begin();
    TwoPhaseCommitTransaction tx2 = manager2.join(tx1.getId());
    deletes(
        account1Id,
        account1Type,
        namespace1,
        TABLE_1,
        tx1,
        account2Id,
        account2Type,
        namespace2,
        TABLE_2,
        tx2);

    // Act Assert
    assertThatCode(
            () -> {
              TwoPhaseCommitTransaction tx3 = manager2.begin();
              TwoPhaseCommitTransaction tx4 = manager1.join(tx3.getId());
              deletes(
                  account2Id,
                  account2Type,
                  namespace2,
                  TABLE_2,
                  tx3,
                  account3Id,
                  account3Type,
                  namespace1,
                  TABLE_1,
                  tx4);

              tx3.prepare();
              tx4.prepare();
              tx3.commit();
              tx4.commit();
            })
        .doesNotThrowAnyException();

    assertThatThrownBy(
            () -> {
              tx1.prepare();
              tx2.prepare();
            })
        .isInstanceOf(PreparationException.class);
    tx1.rollback();
    tx2.rollback();

    // Assert
    TwoPhaseCommitTransaction another1 = manager1.begin();
    TwoPhaseCommitTransaction another2 = manager2.join(another1.getId());

    Optional<Result> result =
        another1.get(prepareGet(account1Id, account1Type, namespace1, TABLE_1));
    assertThat(result).isPresent();

    result = another2.get(prepareGet(account2Id, account2Type, namespace2, TABLE_2));
    assertThat(result).isNotPresent();

    result = another1.get(prepareGet(account3Id, account3Type, namespace1, TABLE_1));
    assertThat(result).isNotPresent();

    another1.prepare();
    another2.prepare();
    another1.commit();
    another2.commit();
  }

  @Test
  public void commit_NonConflictingDeletesGivenForExisting_ShouldCommitBoth()
      throws TransactionException {
    // Arrange
    int account1Id = 0;
    int account1Type = 0;
    int account2Id = 1;
    int account2Type = 0;
    int account3Id = 2;
    int account3Type = 0;
    int account4Id = 3;
    int account4Type = 0;

    populate(manager1, namespace1, TABLE_1);
    populate(manager2, namespace2, TABLE_2);

    TwoPhaseCommitTransaction tx1 = manager1.begin();
    TwoPhaseCommitTransaction tx2 = manager2.join(tx1.getId());
    deletes(
        account1Id,
        account1Type,
        namespace1,
        TABLE_1,
        tx1,
        account2Id,
        account2Type,
        namespace2,
        TABLE_2,
        tx2);

    // Act Assert
    assertThatCode(
            () -> {
              TwoPhaseCommitTransaction tx3 = manager2.begin();
              TwoPhaseCommitTransaction tx4 = manager1.join(tx3.getId());
              deletes(
                  account3Id,
                  account3Type,
                  namespace2,
                  TABLE_2,
                  tx3,
                  account4Id,
                  account4Type,
                  namespace1,
                  TABLE_1,
                  tx4);

              tx3.prepare();
              tx4.prepare();
              tx3.commit();
              tx4.commit();
            })
        .doesNotThrowAnyException();

    assertThatCode(
            () -> {
              tx1.prepare();
              tx2.prepare();
              tx1.commit();
              tx2.commit();
            })
        .doesNotThrowAnyException();

    // Assert
    TwoPhaseCommitTransaction another1 = manager1.begin();
    TwoPhaseCommitTransaction another2 = manager2.join(another1.getId());

    Optional<Result> result =
        another1.get(prepareGet(account1Id, account1Type, namespace1, TABLE_1));
    assertThat(result).isNotPresent();

    result = another2.get(prepareGet(account2Id, account2Type, namespace2, TABLE_2));
    assertThat(result).isNotPresent();

    result = another2.get(prepareGet(account3Id, account3Type, namespace2, TABLE_2));
    assertThat(result).isNotPresent();

    result = another1.get(prepareGet(account4Id, account4Type, namespace1, TABLE_1));
    assertThat(result).isNotPresent();

    another1.prepare();
    another2.prepare();
    another1.commit();
    another2.commit();
  }

  @Test
  public void commit_WriteSkewOnExistingRecordsWithSnapshot_ShouldProduceNonSerializableResult()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction1 = manager1.begin();
    TwoPhaseCommitTransaction transaction2 = manager2.join(transaction1.getId());
    transaction1.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction2.put(preparePut(1, 0, namespace2, TABLE_2).withValue(BALANCE, 1));
    transaction1.prepare();
    transaction2.prepare();
    transaction1.commit();
    transaction2.commit();

    // Act
    TwoPhaseCommitTransaction tx1Sub1 = manager1.begin();
    TwoPhaseCommitTransaction tx1Sub2 = manager2.join(tx1Sub1.getId());
    Optional<Result> result = tx1Sub2.get(prepareGet(1, 0, namespace2, TABLE_2));
    assertThat(result).isPresent();
    int current1 = getBalance(result.get());
    tx1Sub1.get(prepareGet(0, 0, namespace1, TABLE_1));
    tx1Sub1.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, current1 + 1));

    TwoPhaseCommitTransaction tx2Sub1 = manager1.begin();
    TwoPhaseCommitTransaction tx2Sub2 = manager2.join(tx2Sub1.getId());
    result = tx2Sub1.get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result).isPresent();
    int current2 = getBalance(result.get());
    tx2Sub2.get(prepareGet(1, 0, namespace2, TABLE_2));
    tx2Sub2.put(preparePut(1, 0, namespace2, TABLE_2).withValue(BALANCE, current2 + 1));

    tx1Sub1.prepare();
    tx1Sub2.prepare();
    tx1Sub1.commit();
    tx1Sub2.commit();

    tx2Sub1.prepare();
    tx2Sub2.prepare();
    tx2Sub1.commit();
    tx2Sub2.commit();

    // Assert
    transaction1 = manager1.begin();
    transaction2 = manager2.join(transaction1.getId());

    // the results can not be produced by executing the transactions serially
    result = transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(current1 + 1);
    result = transaction2.get(prepareGet(1, 0, namespace2, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(current2 + 1);

    transaction1.prepare();
    transaction2.prepare();
    transaction1.commit();
    transaction2.commit();
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowPreparationException()
          throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction1 = manager1.begin();
    TwoPhaseCommitTransaction transaction2 = manager2.join(transaction1.getId());
    transaction1.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction2.put(preparePut(1, 0, namespace2, TABLE_2).withValue(BALANCE, 1));
    transaction1.prepare();
    transaction2.prepare();
    transaction1.commit();
    transaction2.commit();

    Isolation isolation = Isolation.SERIALIZABLE;
    SerializableStrategy strategy = SerializableStrategy.EXTRA_WRITE;

    // Act Assert
    TwoPhaseCommitTransaction tx1Sub1 = manager1.begin(isolation, strategy);
    TwoPhaseCommitTransaction tx1Sub2 = manager2.join(tx1Sub1.getId(), isolation, strategy);
    Optional<Result> result = tx1Sub2.get(prepareGet(1, 0, namespace2, TABLE_2));
    assertThat(result).isPresent();
    int current1 = getBalance(result.get());
    tx1Sub1.get(prepareGet(0, 0, namespace1, TABLE_1));
    tx1Sub1.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, current1 + 1));

    TwoPhaseCommitTransaction tx2Sub1 = manager1.begin(isolation, strategy);
    TwoPhaseCommitTransaction tx2Sub2 = manager2.join(tx2Sub1.getId(), isolation, strategy);
    result = tx2Sub1.get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result).isPresent();
    int current2 = getBalance(result.get());
    tx2Sub2.get(prepareGet(1, 0, namespace2, TABLE_2));
    tx2Sub2.put(preparePut(1, 0, namespace2, TABLE_2).withValue(BALANCE, current2 + 1));

    tx1Sub1.prepare();
    tx1Sub2.prepare();
    tx1Sub1.commit();
    tx1Sub2.commit();

    assertThatThrownBy(
            () -> {
              tx2Sub1.prepare();
              tx2Sub2.prepare();
            })
        .isInstanceOf(PreparationException.class);
    tx2Sub1.rollback();
    tx2Sub2.rollback();

    // Assert
    transaction1 = manager1.begin();
    transaction2 = manager2.join(transaction1.getId());

    result = transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(current1 + 1);
    result = transaction2.get(prepareGet(1, 0, namespace2, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(current2);

    transaction1.prepare();
    transaction2.prepare();
    transaction1.commit();
    transaction2.commit();
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowValidationException()
          throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction1 = manager1.begin();
    TwoPhaseCommitTransaction transaction2 = manager2.join(transaction1.getId());
    transaction1.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction2.put(preparePut(1, 0, namespace2, TABLE_2).withValue(BALANCE, 1));
    transaction1.prepare();
    transaction2.prepare();
    transaction1.commit();
    transaction2.commit();

    Isolation isolation = Isolation.SERIALIZABLE;
    SerializableStrategy strategy = SerializableStrategy.EXTRA_READ;

    // Act Assert
    TwoPhaseCommitTransaction tx1Sub1 = manager1.begin(isolation, strategy);
    TwoPhaseCommitTransaction tx1Sub2 = manager2.join(tx1Sub1.getId(), isolation, strategy);
    Optional<Result> result = tx1Sub2.get(prepareGet(1, 0, namespace2, TABLE_2));
    assertThat(result).isPresent();
    int current1 = getBalance(result.get());
    tx1Sub1.get(prepareGet(0, 0, namespace1, TABLE_1));
    tx1Sub1.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, current1 + 1));

    TwoPhaseCommitTransaction tx2Sub1 = manager1.begin(isolation, strategy);
    TwoPhaseCommitTransaction tx2Sub2 = manager2.join(tx2Sub1.getId(), isolation, strategy);
    result = tx2Sub1.get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result).isPresent();
    int current2 = getBalance(result.get());
    tx2Sub2.get(prepareGet(1, 0, namespace2, TABLE_2));
    tx2Sub2.put(preparePut(1, 0, namespace2, TABLE_2).withValue(BALANCE, current2 + 1));

    tx1Sub1.prepare();
    tx1Sub2.prepare();
    tx1Sub1.validate();
    tx1Sub2.validate();
    tx1Sub1.commit();
    tx1Sub2.commit();

    tx2Sub1.prepare();
    tx2Sub2.prepare();
    assertThatThrownBy(
            () -> {
              tx2Sub1.validate();
              tx2Sub2.validate();
            })
        .isInstanceOf(ValidationException.class);
    tx2Sub1.rollback();
    tx2Sub2.rollback();

    // Assert
    transaction1 = manager1.begin();
    transaction2 = manager2.join(transaction1.getId());

    result = transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(current1 + 1);
    result = transaction2.get(prepareGet(1, 0, namespace2, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(current2);

    transaction1.prepare();
    transaction2.prepare();
    transaction1.commit();
    transaction2.commit();
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowPreparationException()
          throws TransactionException {
    // Arrange
    Isolation isolation = Isolation.SERIALIZABLE;
    SerializableStrategy strategy = SerializableStrategy.EXTRA_WRITE;

    // Act Assert
    TwoPhaseCommitTransaction tx1Sub1 = manager1.begin(isolation, strategy);
    TwoPhaseCommitTransaction tx1Sub2 = manager2.join(tx1Sub1.getId(), isolation, strategy);
    Optional<Result> result = tx1Sub2.get(prepareGet(1, 0, namespace2, TABLE_2));
    assertThat(result).isNotPresent();
    int current1 = 0;
    tx1Sub1.get(prepareGet(0, 0, namespace1, TABLE_1));
    tx1Sub1.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, current1 + 1));

    TwoPhaseCommitTransaction tx2Sub1 = manager1.begin(isolation, strategy);
    TwoPhaseCommitTransaction tx2Sub2 = manager2.join(tx2Sub1.getId(), isolation, strategy);
    result = tx2Sub1.get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result).isNotPresent();
    int current2 = 0;
    tx2Sub2.get(prepareGet(1, 0, namespace2, TABLE_2));
    tx2Sub2.put(preparePut(1, 0, namespace2, TABLE_2).withValue(BALANCE, current2 + 1));

    tx1Sub1.prepare();
    tx1Sub2.prepare();
    tx1Sub1.commit();
    tx1Sub2.commit();

    assertThatThrownBy(
            () -> {
              tx2Sub1.prepare();
              tx2Sub2.prepare();
            })
        .isInstanceOf(PreparationException.class);
    tx2Sub1.rollback();
    tx2Sub2.rollback();

    // Assert
    TwoPhaseCommitTransaction transaction1 = manager1.begin();
    TwoPhaseCommitTransaction transaction2 = manager2.join(transaction1.getId());

    result = transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(current1 + 1);
    result = transaction2.get(prepareGet(1, 0, namespace2, TABLE_2));
    assertThat(result).isNotPresent();

    transaction1.prepare();
    transaction2.prepare();
    transaction1.commit();
    transaction2.commit();
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWriteAndCommitStatusFailed_ShouldRollbackProperly()
          throws TransactionException, CoordinatorException {
    // Arrange
    State state = new State(ANY_ID_1, TransactionState.ABORTED);
    coordinatorForStorage1.putState(state);

    Isolation isolation = Isolation.SERIALIZABLE;
    SerializableStrategy strategy = SerializableStrategy.EXTRA_WRITE;

    // Act
    TwoPhaseCommitTransaction txSub1 = manager1.begin(ANY_ID_1, isolation, strategy);
    TwoPhaseCommitTransaction txSub2 = manager2.join(txSub1.getId(), isolation, strategy);

    Optional<Result> result = txSub2.get(prepareGet(1, 0, namespace2, TABLE_2));
    assertThat(result).isNotPresent();
    result = txSub1.get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result).isNotPresent();
    int current1 = 0;
    txSub1.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, current1 + 1));

    assertThatThrownBy(
            () -> {
              txSub1.prepare();
              txSub2.prepare();
              txSub1.commit();
              txSub2.commit();
            })
        .isInstanceOf(CommitException.class);
    txSub1.rollback();
    txSub2.rollback();

    // Assert
    TwoPhaseCommitTransaction transaction1 = manager1.begin();
    TwoPhaseCommitTransaction transaction2 = manager2.join(transaction1.getId());

    result = transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result).isNotPresent();
    result = transaction2.get(prepareGet(1, 0, namespace2, TABLE_2));
    assertThat(result).isNotPresent();

    transaction1.prepare();
    transaction2.prepare();
    transaction1.commit();
    transaction2.commit();
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowValidationException()
          throws TransactionException {
    // Arrange
    Isolation isolation = Isolation.SERIALIZABLE;
    SerializableStrategy strategy = SerializableStrategy.EXTRA_READ;

    // Act
    TwoPhaseCommitTransaction tx1Sub1 = manager1.begin(isolation, strategy);
    TwoPhaseCommitTransaction tx1Sub2 = manager2.join(tx1Sub1.getId(), isolation, strategy);
    Optional<Result> result = tx1Sub2.get(prepareGet(1, 0, namespace2, TABLE_2));
    assertThat(result).isNotPresent();
    int current1 = 0;
    tx1Sub1.get(prepareGet(0, 0, namespace1, TABLE_1));
    tx1Sub1.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, current1 + 1));

    TwoPhaseCommitTransaction tx2Sub1 = manager1.begin(isolation, strategy);
    TwoPhaseCommitTransaction tx2Sub2 = manager2.join(tx2Sub1.getId(), isolation, strategy);
    result = tx2Sub1.get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result).isNotPresent();
    int current2 = 0;
    tx2Sub2.get(prepareGet(1, 0, namespace2, TABLE_2));
    tx2Sub2.put(preparePut(1, 0, namespace2, TABLE_2).withValue(BALANCE, current2 + 1));

    tx1Sub1.prepare();
    tx1Sub2.prepare();
    tx1Sub1.validate();
    tx1Sub2.validate();
    tx1Sub1.commit();
    tx1Sub2.commit();

    tx2Sub1.prepare();
    tx2Sub2.prepare();
    assertThatThrownBy(
            () -> {
              tx2Sub1.validate();
              tx2Sub2.validate();
            })
        .isInstanceOf(ValidationException.class);
    tx2Sub1.rollback();
    tx2Sub2.rollback();

    // Assert
    TwoPhaseCommitTransaction transaction1 = manager1.begin();
    TwoPhaseCommitTransaction transaction2 = manager2.join(transaction1.getId());

    result = transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(current1 + 1);
    result = transaction2.get(prepareGet(1, 0, namespace2, TABLE_2));
    assertThat(result).isNotPresent();

    transaction1.prepare();
    transaction2.prepare();
    transaction1.commit();
    transaction2.commit();
  }

  @Test
  public void
      commit_WriteSkewWithScanOnNonExistingRecordsWithSerializableWithExtraWrite_ShouldThrowPreparationException()
          throws TransactionException {
    // Arrange
    Isolation isolation = Isolation.SERIALIZABLE;
    SerializableStrategy strategy = SerializableStrategy.EXTRA_WRITE;

    // Act Assert
    TwoPhaseCommitTransaction transaction1 = manager1.begin(isolation, strategy);
    List<Result> results = transaction1.scan(prepareScan(0, 0, 1, namespace1, TABLE_1));
    assertThat(results).isEmpty();
    int count1 = 0;
    transaction1.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, count1 + 1));

    TwoPhaseCommitTransaction transaction2 = manager1.begin(isolation, strategy);
    results = transaction2.scan(prepareScan(0, 0, 1, namespace1, TABLE_1));
    assertThat(results).isEmpty();
    int count2 = 0;
    transaction2.put(preparePut(0, 1, namespace1, TABLE_1).withValue(BALANCE, count2 + 1));

    assertThatThrownBy(transaction1::prepare).isInstanceOf(PreparationException.class);
    transaction1.rollback();

    assertThatThrownBy(transaction2::prepare).isInstanceOf(PreparationException.class);
    transaction2.rollback();

    // Assert
    TwoPhaseCommitTransaction transaction = manager1.begin();
    Optional<Result> result = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result).isNotPresent();
    result = transaction.get(prepareGet(0, 1, namespace1, TABLE_1));
    assertThat(result).isNotPresent();

    transaction.prepare();
    transaction.commit();
  }

  @Test
  public void
      commit_WriteSkewWithScanOnNonExistingRecordsWithSerializableWithExtraRead_ShouldThrowValidationException()
          throws TransactionException {
    // Arrange
    Isolation isolation = Isolation.SERIALIZABLE;
    SerializableStrategy strategy = SerializableStrategy.EXTRA_READ;

    // Act Assert
    TwoPhaseCommitTransaction transaction1 = manager1.begin(isolation, strategy);
    List<Result> results = transaction1.scan(prepareScan(0, 0, 1, namespace1, TABLE_1));
    assertThat(results).isEmpty();
    int count1 = 0;
    transaction1.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, count1 + 1));

    TwoPhaseCommitTransaction transaction2 = manager1.begin(isolation, strategy);
    results = transaction2.scan(prepareScan(0, 0, 1, namespace1, TABLE_1));
    assertThat(results).isEmpty();
    int count2 = 0;
    transaction2.put(preparePut(0, 1, namespace1, TABLE_1).withValue(BALANCE, count2 + 1));

    assertThatCode(
            () -> {
              transaction1.prepare();
              transaction1.validate();
              transaction1.commit();
            })
        .doesNotThrowAnyException();

    transaction2.prepare();
    assertThatThrownBy(transaction2::validate).isInstanceOf(ValidationException.class);
    transaction2.rollback();

    // Assert
    TwoPhaseCommitTransaction transaction = manager1.begin();
    Optional<Result> result = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(count1 + 1);
    result = transaction.get(prepareGet(0, 1, namespace1, TABLE_1));
    assertThat(result).isNotPresent();

    transaction.prepare();
    transaction.commit();
  }

  @Test
  public void
      commit_WriteSkewWithScanOnExistingRecordsWithSerializableWithExtraRead_ShouldThrowValidationException()
          throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction.put(preparePut(0, 1, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.commit();

    Isolation isolation = Isolation.SERIALIZABLE;
    SerializableStrategy strategy = SerializableStrategy.EXTRA_READ;

    // Act Assert
    TwoPhaseCommitTransaction transaction1 = manager1.begin(isolation, strategy);
    List<Result> results = transaction1.scan(prepareScan(0, 0, 1, namespace1, TABLE_1));
    assertThat(results.size()).isEqualTo(2);
    int count1 = results.size();
    transaction1.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, count1 + 1));

    TwoPhaseCommitTransaction transaction2 = manager1.begin(isolation, strategy);
    results = transaction2.scan(prepareScan(0, 0, 1, namespace1, TABLE_1));
    assertThat(results.size()).isEqualTo(2);
    int count2 = results.size();
    transaction2.put(preparePut(0, 1, namespace1, TABLE_1).withValue(BALANCE, count2 + 1));

    assertThatCode(
            () -> {
              transaction1.prepare();
              transaction1.validate();
              transaction1.commit();
            })
        .doesNotThrowAnyException();

    transaction2.prepare();
    assertThatThrownBy(transaction2::validate).isInstanceOf(ValidationException.class);
    transaction2.rollback();

    // Assert
    transaction = manager1.begin();
    Optional<Result> result = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(count1 + 1);
    result = transaction.get(prepareGet(0, 1, namespace1, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(1);

    transaction.prepare();
    transaction.commit();
  }

  @Test
  public void scanAndCommit_MultipleScansGivenInTransactionWithExtraRead_ShouldCommitProperly()
      throws TransactionException {
    // Arrange
    populate(manager1, namespace1, TABLE_1);

    // Act Assert
    TwoPhaseCommitTransaction transaction =
        manager1.begin(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    transaction.scan(prepareScan(0, namespace1, TABLE_1));
    transaction.scan(prepareScan(1, namespace1, TABLE_1));
    assertThatCode(
            () -> {
              transaction.prepare();
              transaction.validate();
              transaction.commit();
            })
        .doesNotThrowAnyException();
  }

  @Test
  public void putAndCommit_DeleteGivenInBetweenTransactions_ShouldProduceSerializableResults()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 2));
    transaction.prepare();
    transaction.commit();

    // Act
    TwoPhaseCommitTransaction transaction1 = manager1.begin();
    Optional<Result> result = transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result).isPresent();
    int balance1 = getBalance(result.get());
    transaction1.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, balance1 + 1));

    TwoPhaseCommitTransaction transaction2 = manager1.begin();
    transaction2.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction2.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    transaction2.prepare();
    transaction2.commit();

    // the same transaction processing as transaction1
    TwoPhaseCommitTransaction transaction3 = manager1.begin();
    result = transaction3.get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result).isNotPresent();
    int balance3 = 0;
    transaction3.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, balance3 + 1));
    transaction3.prepare();
    transaction3.commit();

    assertThatThrownBy(transaction1::prepare).isInstanceOf(PreparationException.class);
    transaction1.rollback();

    // Assert
    transaction = manager1.begin();
    result = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(1);

    transaction.prepare();
    transaction.commit();
  }

  @Test
  public void deleteAndCommit_DeleteGivenInBetweenTransactions_ShouldProduceSerializableResults()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 2));
    transaction.prepare();
    transaction.commit();

    // Act
    TwoPhaseCommitTransaction transaction1 = manager1.begin();
    transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction1.delete(prepareDelete(0, 0, namespace1, TABLE_1));

    TwoPhaseCommitTransaction transaction2 = manager1.begin();
    transaction2.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction2.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    transaction2.prepare();
    transaction2.commit();

    TwoPhaseCommitTransaction transaction3 = manager1.begin();
    Optional<Result> result = transaction3.get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result).isNotPresent();
    int balance3 = 0;
    transaction3.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, balance3 + 1));
    transaction3.prepare();
    transaction3.commit();

    assertThatThrownBy(transaction1::prepare).isInstanceOf(PreparationException.class);
    transaction1.rollback();

    // Assert
    transaction = manager1.begin();
    result = transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(1);

    transaction.prepare();
    transaction.commit();
  }

  @Test
  public void get_PutCalledBefore_ShouldGet() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();

    // Act
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    Optional<Result> result = transaction.get(get);
    assertThatCode(
            () -> {
              transaction.prepare();
              transaction.commit();
            })
        .doesNotThrowAnyException();

    // Assert
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(1);
  }

  @Test
  public void get_DeleteCalledBefore_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.commit();

    // Act
    TwoPhaseCommitTransaction transaction1 = manager1.begin();
    Optional<Result> resultBefore = transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction1.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    Optional<Result> resultAfter = transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));

    assertThatCode(
            () -> {
              transaction1.prepare();
              transaction1.commit();
            })
        .doesNotThrowAnyException();

    // Assert
    assertThat(resultBefore.isPresent()).isTrue();
    assertThat(resultAfter.isPresent()).isFalse();
  }

  @Test
  public void scan_DeleteCalledBefore_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.commit();

    // Act Assert
    TwoPhaseCommitTransaction transaction1 = manager1.begin();
    List<Result> resultBefore = transaction1.scan(prepareScan(0, 0, 0, namespace1, TABLE_1));
    transaction1.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    List<Result> resultAfter = transaction1.scan(prepareScan(0, 0, 0, namespace1, TABLE_1));

    assertThatCode(
            () -> {
              transaction1.prepare();
              transaction1.commit();
            })
        .doesNotThrowAnyException();

    // Assert
    assertThat(resultBefore.size()).isEqualTo(1);
    assertThat(resultAfter.size()).isEqualTo(0);
  }

  @Test
  public void delete_PutCalledBefore_ShouldDelete() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.commit();

    // Act Assert
    TwoPhaseCommitTransaction transaction1 = manager1.begin();
    Optional<Result> resultBefore = transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction1.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 2));
    transaction1.delete(prepareDelete(0, 0, namespace1, TABLE_1));

    assertThatCode(
            () -> {
              transaction1.prepare();
              transaction1.commit();
            })
        .doesNotThrowAnyException();

    // Assert
    TwoPhaseCommitTransaction transaction2 = manager1.begin();
    Optional<Result> resultAfter = transaction2.get(prepareGet(0, 0, namespace1, TABLE_1));

    assertThat(resultBefore.isPresent()).isTrue();
    assertThat(resultAfter.isPresent()).isFalse();

    transaction2.prepare();
    transaction2.commit();
  }

  @Test
  public void put_DeleteCalledBefore_ShouldThrowIllegalArgumentException()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.commit();

    // Act
    TwoPhaseCommitTransaction transaction1 = manager1.begin();
    Get get = prepareGet(0, 0, namespace1, TABLE_1);
    transaction1.get(get);
    transaction1.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    Throwable thrown =
        catchThrowable(
            () -> transaction1.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 2)));
    transaction1.rollback();

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void scan_OverlappingPutGivenBefore_ShouldIllegalArgumentException()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));

    // Act Assert
    assertThatThrownBy(() -> transaction.scan(prepareScan(0, 0, 0, namespace1, TABLE_1)))
        .isInstanceOf(IllegalArgumentException.class);

    transaction.rollback();
  }

  @Test
  public void scan_NonOverlappingPutGivenBefore_ShouldScan() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));

    // Act Assert
    assertThatCode(() -> transaction.scan(prepareScan(0, 1, 1, namespace1, TABLE_1)))
        .doesNotThrowAnyException();

    transaction.prepare();
    transaction.commit();
  }

  @Test
  public void scan_DeleteGivenBefore_ShouldScan() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction.put(preparePut(0, 1, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.commit();

    // Act
    TwoPhaseCommitTransaction transaction1 = manager1.begin();
    transaction1.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    Scan scan = prepareScan(0, 0, 1, namespace1, TABLE_1);
    List<Result> results = transaction1.scan(scan);
    assertThatCode(
            () -> {
              transaction1.prepare();
              transaction1.commit();
            })
        .doesNotThrowAnyException();

    // Assert
    assertThat(results.size()).isEqualTo(1);
  }

  @Test
  public void start_CorrectTransactionIdGiven_ShouldNotThrowAnyExceptions() {
    // Arrange
    String transactionId = ANY_ID_1;

    // Act Assert
    assertThatCode(
            () -> {
              TwoPhaseCommitTransaction transaction = manager1.begin(transactionId);
              transaction.prepare();
              transaction.commit();
            })
        .doesNotThrowAnyException();
  }

  @Test
  public void start_EmptyTransactionIdGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    String transactionId = "";

    // Act Assert
    assertThatThrownBy(() -> manager1.begin(transactionId))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void getState_forSuccessfulTransaction_ShouldReturnCommittedState()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();
    transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.commit();

    // Act
    TransactionState state = manager1.getState(transaction.getId());

    // Assert
    assertThat(state).isEqualTo(TransactionState.COMMITTED);
  }

  @Test
  public void getState_forFailedTransaction_ShouldReturnAbortedState() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction1 = manager1.begin();
    transaction1.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction1.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));

    TwoPhaseCommitTransaction transaction2 = manager1.begin();
    transaction2.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction2.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));
    transaction2.prepare();
    transaction2.commit();

    assertThatCode(transaction1::prepare).isInstanceOf(PreparationException.class);
    transaction1.rollback();

    // Act
    TransactionState state = manager1.getState(transaction1.getId());

    // Assert
    assertThat(state).isEqualTo(TransactionState.ABORTED);
  }

  @Test
  public void abort_forOngoingTransaction_ShouldAbortCorrectly() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();
    transaction.get(prepareGet(0, 0, namespace1, TABLE_1));
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withValue(BALANCE, 1));

    // Act
    manager1.abort(transaction.getId());

    transaction.prepare();
    assertThatCode(transaction::commit).isInstanceOf(CommitException.class);
    transaction.rollback();

    // Assert
    TransactionState state = manager1.getState(transaction.getId());
    assertThat(state).isEqualTo(TransactionState.ABORTED);
  }

  @Test
  public void scanAll_DeleteCalledBefore_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withIntValue(BALANCE, 1));
    transaction.prepare();
    transaction.commit();

    // Act
    TwoPhaseCommitTransaction transaction1 = manager1.begin();
    ScanAll scanAll = prepareScanAll(namespace1, TABLE_1);
    List<Result> resultBefore = transaction1.scan(scanAll);
    transaction1.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    List<Result> resultAfter = transaction1.scan(scanAll);
    assertThatCode(
            () -> {
              transaction1.prepare();
              transaction1.commit();
            })
        .doesNotThrowAnyException();

    // Assert
    assertThat(resultBefore.size()).isEqualTo(1);
    assertThat(resultAfter.size()).isEqualTo(0);
  }

  @Test
  public void scanAll_DeleteGivenBefore_ShouldScanAll() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withIntValue(BALANCE, 1));
    transaction.put(preparePut(0, 1, namespace1, TABLE_1).withIntValue(BALANCE, 1));
    transaction.prepare();
    transaction.commit();

    // Act
    TwoPhaseCommitTransaction transaction1 = manager1.begin();
    transaction1.delete(prepareDelete(0, 0, namespace1, TABLE_1));
    ScanAll scanAll = prepareScanAll(namespace1, TABLE_1);
    List<Result> results = transaction1.scan(scanAll);
    assertThatCode(
            () -> {
              transaction1.prepare();
              transaction1.commit();
            })
        .doesNotThrowAnyException();

    // Assert
    assertThat(results.size()).isEqualTo(1);
  }

  @Test
  public void scanAll_NonOverlappingPutGivenBefore_ShouldScanAll() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction1 = manager1.begin();
    TwoPhaseCommitTransaction transaction2 = manager2.join(transaction1.getId());

    transaction1.put(preparePut(0, 0, namespace1, TABLE_1).withIntValue(BALANCE, 1));

    // Act
    assertThatCode(() -> transaction2.scan(prepareScanAll(namespace2, TABLE_2)))
        .doesNotThrowAnyException();

    transaction1.rollback();
    transaction2.rollback();
  }

  @Test
  public void scanAll_OverlappingPutGivenBefore_ShouldThrowIllegalArgumentException()
      throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction transaction = manager1.begin();
    transaction.put(preparePut(0, 0, namespace1, TABLE_1).withIntValue(BALANCE, 1));

    // Act
    assertThatThrownBy(() -> transaction.scan(prepareScanAll(namespace1, TABLE_1)))
        .isInstanceOf(IllegalArgumentException.class);

    transaction.rollback();
  }

  @Test
  public void scanAll_ScanAllGivenForCommittedRecord_ShouldReturnRecord()
      throws TransactionException {
    // Arrange
    populate(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.begin();
    ScanAll scanAll = prepareScanAll(namespace1, TABLE_1).withLimit(1);

    // Act
    List<Result> results = transaction.scan(scanAll);
    transaction.prepare();
    transaction.rollback();

    // Assert
    assertThat(results.size()).isEqualTo(1);
    Assertions.assertThat(
            ((TransactionResult) ((FilteredResult) results.get(0)).getOriginalResult()).getState())
        .isEqualTo(TransactionState.COMMITTED);
  }

  @Test
  public void scanAll_ScanAllGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback()
      throws TransactionException, CoordinatorException, ExecutionException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback(
        SelectionType.SCAN_ALL);
  }

  @Test
  public void
      scanAll_ScanAllGivenForDeletedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly()
          throws ExecutionException, CoordinatorException, TransactionException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly(
        SelectionType.SCAN_ALL);
  }

  @Test
  public void scanAll_ScanAllGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, CoordinatorException, TransactionException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward(
        SelectionType.SCAN_ALL);
  }

  @Test
  public void
      scanAll_ScanAllGivenForDeletedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly()
          throws ExecutionException, CoordinatorException, TransactionException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly(
        SelectionType.SCAN_ALL);
  }

  @Test
  public void
      scanAll_ScanAllGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
          throws ExecutionException, CoordinatorException, TransactionException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        SelectionType.SCAN_ALL);
  }

  @Test
  public void
      scanAll_ScanAllGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws ExecutionException, CoordinatorException, TransactionException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        SelectionType.SCAN_ALL);
  }

  @Test
  public void scanAll_ScanAllGivenForNonExisting_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    TwoPhaseCommitTransaction putTransaction = manager1.begin();
    putTransaction.put(preparePut(0, 0, namespace1, TABLE_1));
    putTransaction.prepare();
    putTransaction.commit();

    TwoPhaseCommitTransaction transaction = manager2.begin();
    ScanAll scanAll =
        new ScanAll()
            .forNamespace(namespace2)
            .forTable(TABLE_2)
            .withConsistency(Consistency.LINEARIZABLE);

    // Act
    List<Result> results = transaction.scan(scanAll);
    transaction.prepare();
    transaction.commit();

    // Assert
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  public void scanAll_ScanAllGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback()
      throws TransactionException, CoordinatorException, ExecutionException {
    selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback(
        SelectionType.SCAN_ALL);
  }

  @Test
  public void
      scanAll_ScanAllGivenForPreparedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly()
          throws ExecutionException, CoordinatorException, TransactionException {
    selection_SelectionGivenForPreparedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly(
        SelectionType.SCAN_ALL);
  }

  @Test
  public void scanAll_ScanAllGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, CoordinatorException, TransactionException {
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward(
        SelectionType.SCAN_ALL);
  }

  @Test
  public void
      scanAll_ScanAllGivenForPreparedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly()
          throws ExecutionException, CoordinatorException, TransactionException {
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly(
        SelectionType.SCAN_ALL);
  }

  @Test
  public void
      scanAll_ScanAllGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
          throws ExecutionException, CoordinatorException, TransactionException {
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        SelectionType.SCAN_ALL);
  }

  @Test
  public void
      scanAll_ScanAllGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws ExecutionException, CoordinatorException, TransactionException {
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        SelectionType.SCAN_ALL);
  }

  private void populate(TwoPhaseConsensusCommitManager manager, String namespace, String table)
      throws TransactionException {
    TwoPhaseCommitTransaction transaction = manager.begin();
    for (int i = 0; i < NUM_ACCOUNTS; i++) {
      for (int j = 0; j < NUM_TYPES; j++) {
        Key partitionKey = Key.ofInt(ACCOUNT_ID, i);
        Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, j);
        transaction.put(
            Put.newBuilder()
                .namespace(namespace)
                .table(table)
                .partitionKey(partitionKey)
                .clusteringKey(clusteringKey)
                .intValue(BALANCE, INITIAL_BALANCE)
                .disableImplicitPreRead()
                .build());
      }
    }
    transaction.prepare();
    transaction.commit();
  }

  private ScanAll prepareScanAll(String namespace, String table) {
    return new ScanAll()
        .forNamespace(namespace)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  private void populatePreparedRecordAndCoordinatorStateRecordForStorage1(
      TransactionState recordState, long preparedAt, TransactionState coordinatorState)
      throws ExecutionException, CoordinatorException {
    Key partitionKey = new Key(new IntValue(ACCOUNT_ID, 0));
    Key clusteringKey = new Key(new IntValue(ACCOUNT_TYPE, 0));
    Put put =
        new Put(partitionKey, clusteringKey)
            .forNamespace(namespace1)
            .forTable(TABLE_1)
            .withValue(new IntValue(BALANCE, INITIAL_BALANCE))
            .withValue(Attribute.toIdValue(ANY_ID_2))
            .withValue(Attribute.toStateValue(recordState))
            .withValue(Attribute.toVersionValue(2))
            .withValue(Attribute.toPreparedAtValue(preparedAt))
            .withValue(Attribute.toBeforeIdValue(ANY_ID_1))
            .withValue(Attribute.toBeforeStateValue(TransactionState.COMMITTED))
            .withValue(Attribute.toBeforeVersionValue(1))
            .withValue(Attribute.toBeforePreparedAtValue(1))
            .withValue(Attribute.toBeforeCommittedAtValue(1));
    storage1.put(put);

    if (coordinatorState == null) {
      return;
    }
    State state = new State(ANY_ID_2, coordinatorState);
    coordinatorForStorage1.putState(state);
  }

  private void transfer(
      int fromId,
      int fromType,
      String fromNamespace,
      String fromTable,
      TwoPhaseCommitTransaction fromTx,
      int toId,
      int toType,
      String toNamespace,
      String toTable,
      TwoPhaseCommitTransaction toTx,
      int amount)
      throws TransactionException {
    int fromBalance =
        fromTx
            .get(prepareGet(fromId, fromType, fromNamespace, fromTable))
            .get()
            .getValue(BALANCE)
            .get()
            .getAsInt();
    int toBalance =
        toTx.get(prepareGet(toId, toType, toNamespace, toTable))
            .get()
            .getValue(BALANCE)
            .get()
            .getAsInt();
    fromTx.put(
        preparePut(fromId, fromType, fromNamespace, fromTable)
            .withValue(BALANCE, fromBalance - amount));
    toTx.put(preparePut(toId, toType, toNamespace, toTable).withValue(BALANCE, toBalance + amount));
  }

  private void deletes(
      int id,
      int type,
      String namespace,
      String table,
      TwoPhaseCommitTransaction tx,
      int anotherId,
      int anotherType,
      String anotherNamespace,
      String anotherTable,
      TwoPhaseCommitTransaction anotherTx)
      throws TransactionException {
    tx.get(prepareGet(id, type, namespace, table));
    anotherTx.get(prepareGet(anotherId, anotherType, anotherNamespace, anotherTable));
    tx.delete(prepareDelete(id, type, namespace, table));
    anotherTx.delete(prepareDelete(anotherId, anotherType, anotherNamespace, anotherTable));
  }

  private Get prepareGet(int id, int type, String namespace, String table) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    Key clusteringKey = new Key(ACCOUNT_TYPE, type);
    return new Get(partitionKey, clusteringKey)
        .forNamespace(namespace)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  private Scan prepareScan(int id, int fromType, int toType, String namespace, String table) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    return new Scan(partitionKey)
        .forNamespace(namespace)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE)
        .withStart(new Key(ACCOUNT_TYPE, fromType))
        .withEnd(new Key(ACCOUNT_TYPE, toType));
  }

  private Scan prepareScan(int id, String namespace, String table) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    return new Scan(partitionKey)
        .forNamespace(namespace)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  private Put preparePut(int id, int type, String namespace, String table) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    Key clusteringKey = new Key(ACCOUNT_TYPE, type);
    return new Put(partitionKey, clusteringKey)
        .forNamespace(namespace)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  private Delete prepareDelete(int id, int type, String namespace, String table) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    Key clusteringKey = new Key(ACCOUNT_TYPE, type);
    return new Delete(partitionKey, clusteringKey)
        .forNamespace(namespace)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  private int getAccountId(Result result) {
    Optional<Value<?>> id = result.getValue(ACCOUNT_ID);
    assertThat(id).isPresent();
    return id.get().getAsInt();
  }

  private int getAccountType(Result result) {
    Optional<Value<?>> type = result.getValue(ACCOUNT_TYPE);
    assertThat(type).isPresent();
    return type.get().getAsInt();
  }

  private int getBalance(Result result) {
    Optional<Value<?>> balance = result.getValue(BALANCE);
    assertThat(balance).isPresent();
    return balance.get().getAsInt();
  }

  private enum SelectionType {
    GET,
    SCAN,
    SCAN_ALL
  }
}
