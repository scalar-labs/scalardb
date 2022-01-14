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
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UncommittedRecordException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.Coordinator.State;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TwoPhaseConsensusCommitIntegrationTest {

  private static final String PROP_CONTACT_POINTS = "scalardb.contact_points";
  private static final String PROP_CONTACT_PORT = "scalardb.contact_port";
  private static final String PROP_USERNAME = "scalardb.username";
  private static final String PROP_PASSWORD = "scalardb.password";
  private static final String PROP_STORAGE = "scalardb.storage";

  private static final String DEFAULT_CONTACT_POINTS = "jdbc:mysql://localhost:3306/";
  private static final String DEFAULT_USERNAME = "root";
  private static final String DEFAULT_PASSWORD = "mysql";
  private static final String DEFAULT_STORAGE = "jdbc";

  private static final String NAMESPACE = "integration_testing";
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

  private static TwoPhaseConsensusCommitManager manager;
  private static DistributedStorage storage;
  private static DistributedStorageAdmin admin;
  private static ConsensusCommitAdmin consensusCommitAdmin;
  private static Coordinator coordinator;

  private static DatabaseConfig getDatabaseConfig() {
    String contactPoints = System.getProperty(PROP_CONTACT_POINTS, DEFAULT_CONTACT_POINTS);
    String contactPort = System.getProperty(PROP_CONTACT_PORT);
    String username = System.getProperty(PROP_USERNAME, DEFAULT_USERNAME);
    String password = System.getProperty(PROP_PASSWORD, DEFAULT_PASSWORD);
    String storage = System.getProperty(PROP_STORAGE, DEFAULT_STORAGE);

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, contactPoints);
    if (contactPort != null) {
      properties.setProperty(DatabaseConfig.CONTACT_PORT, contactPort);
    }
    properties.setProperty(DatabaseConfig.USERNAME, username);
    properties.setProperty(DatabaseConfig.PASSWORD, password);
    properties.setProperty(DatabaseConfig.STORAGE, storage);
    return new DatabaseConfig(properties);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws ExecutionException {
    DatabaseConfig config = getDatabaseConfig();
    initStorageAndAdmin(config);
    createTables();
    initManagerAndCoordinator(config);
  }

  private static void initStorageAndAdmin(DatabaseConfig config) {
    StorageFactory factory = new StorageFactory(config);
    storage = factory.getStorage();
    admin = factory.getAdmin();
    consensusCommitAdmin =
        new ConsensusCommitAdmin(admin, new ConsensusCommitConfig(config.getProperties()));
  }

  private static void createTables() throws ExecutionException {
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(ACCOUNT_ID, DataType.INT)
            .addColumn(ACCOUNT_TYPE, DataType.INT)
            .addColumn(BALANCE, DataType.INT)
            .addPartitionKey(ACCOUNT_ID)
            .addClusteringKey(ACCOUNT_TYPE)
            .build();
    admin.createNamespace(NAMESPACE, true);
    consensusCommitAdmin.createTransactionalTable(NAMESPACE, TABLE_1, tableMetadata, true);
    consensusCommitAdmin.createTransactionalTable(NAMESPACE, TABLE_2, tableMetadata, true);
    consensusCommitAdmin.createCoordinatorTable();
  }

  private static void initManagerAndCoordinator(DatabaseConfig config) {
    ConsensusCommitConfig consensusCommitConfig = new ConsensusCommitConfig(config.getProperties());
    manager = new TwoPhaseConsensusCommitManager(storage, admin, consensusCommitConfig);
    coordinator = new Coordinator(storage, consensusCommitConfig);
  }

  @Before
  public void setUp() throws ExecutionException {
    truncateTables();
  }

  private void truncateTables() throws ExecutionException {
    admin.truncateTable(NAMESPACE, TABLE_1);
    admin.truncateTable(NAMESPACE, TABLE_2);
    consensusCommitAdmin.truncateCoordinatorTable();
  }

  @AfterClass
  public static void tearDownAfterClass() throws ExecutionException {
    deleteTables();
    admin.close();
    storage.close();
    manager.close();
  }

  private static void deleteTables() throws ExecutionException {
    admin.dropTable(NAMESPACE, TABLE_1);
    admin.dropTable(NAMESPACE, TABLE_2);
    admin.dropNamespace(NAMESPACE);
    consensusCommitAdmin.dropCoordinatorTable();
  }

  @Test
  public void get_GetGivenForCommittedRecord_ShouldReturnRecord() throws TransactionException {
    // Arrange
    populate(TABLE_1);
    TwoPhaseConsensusCommit transaction = manager.start();

    // Act
    Optional<Result> result = transaction.get(prepareGet(0, 0, TABLE_1));

    // Assert
    assertThat(result.isPresent()).isTrue();
    assertThat(getAccountId(result.get())).isEqualTo(0);
    assertThat(getAccountType(result.get())).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE);
  }

  @Test
  public void scan_ScanGivenForCommittedRecord_ShouldReturnRecord() throws TransactionException {
    // Arrange
    populate(TABLE_1);
    TwoPhaseConsensusCommit transaction = manager.start();

    // Act
    List<Result> results = transaction.scan(prepareScan(0, 0, 0, TABLE_1));

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
    TwoPhaseConsensusCommit transaction = manager.start();

    // Act
    Optional<Result> result1 = transaction.get(prepareGet(0, 0, TABLE_1));
    populate(TABLE_1);
    Optional<Result> result2 = transaction.get(prepareGet(0, 0, TABLE_1));

    // Assert
    assertThat(result1).isEqualTo(result2);
  }

  @Test
  public void get_GetGivenForNonExisting_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    populate(TABLE_1);
    TwoPhaseConsensusCommit transaction = manager.start();

    // Act
    Optional<Result> result = transaction.get(prepareGet(0, 4, TABLE_1));

    // Assert
    assertThat(result.isPresent()).isFalse();
  }

  @Test
  public void scan_ScanGivenForNonExisting_ShouldReturnEmpty() throws TransactionException {
    // Arrange
    populate(TABLE_1);
    TwoPhaseConsensusCommit transaction = manager.start();

    // Act
    List<Result> results = transaction.scan(prepareScan(0, 4, 4, TABLE_1));

    // Assert
    assertThat(results.size()).isEqualTo(0);
  }

  private void selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward(
      boolean isGet) throws ExecutionException, TransactionException, CoordinatorException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        TABLE_1, TransactionState.PREPARED, current, TransactionState.COMMITTED);

    TwoPhaseConsensusCommit transaction = manager.start();

    // Act Assert
    assertThatThrownBy(
            () -> {
              if (isGet) {
                transaction.get(prepareGet(0, 0, TABLE_1));
              } else {
                transaction.scan(prepareScan(0, 0, 0, TABLE_1));
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    Optional<Result> result;
    if (isGet) {
      result = transaction.get(prepareGet(0, 0, TABLE_1));
    } else {
      List<Result> results = transaction.scan(prepareScan(0, 0, 0, TABLE_1));
      assertThat(results.size()).isEqualTo(1);
      result = Optional.of(results.get(0));
    }

    assertThat(result.isPresent()).isTrue();
    assertThat(getAccountId(result.get())).isEqualTo(0);
    assertThat(getAccountType(result.get())).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE); // a rolled forward value
  }

  @Test
  public void get_GetGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward(true);
  }

  @Test
  public void scan_ScanGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommitted_ShouldRollforward(false);
  }

  private void selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback(
      boolean isGet) throws ExecutionException, TransactionException, CoordinatorException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        TABLE_1, TransactionState.PREPARED, current, TransactionState.ABORTED);

    TwoPhaseConsensusCommit transaction = manager.start();

    // Act Assert
    assertThatThrownBy(
            () -> {
              if (isGet) {
                transaction.get(prepareGet(0, 0, TABLE_1));
              } else {
                transaction.scan(prepareScan(0, 0, 0, TABLE_1));
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    Optional<Result> result;
    if (isGet) {
      result = transaction.get(prepareGet(0, 0, TABLE_1));
    } else {
      List<Result> results = transaction.scan(prepareScan(0, 0, 0, TABLE_1));
      assertThat(results.size()).isEqualTo(1);
      result = Optional.of(results.get(0));
    }

    assertThat(result.isPresent()).isTrue();
    assertThat(getAccountId(result.get())).isEqualTo(0);
    assertThat(getAccountType(result.get())).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(0); // a rolled back value
  }

  @Test
  public void get_GetGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback()
      throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback(true);
  }

  @Test
  public void scan_ScanGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback()
      throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForPreparedWhenCoordinatorStateAborted_ShouldRollback(false);
  }

  private void
      selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
          boolean isGet) throws ExecutionException, CoordinatorException {
    // Arrange
    long prepared_at = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        TABLE_1, TransactionState.PREPARED, prepared_at, null);

    TwoPhaseConsensusCommit transaction = manager.start();

    // Act Assert
    assertThatThrownBy(
            () -> {
              if (isGet) {
                transaction.get(prepareGet(0, 0, TABLE_1));
              } else {
                transaction.scan(prepareScan(0, 0, 0, TABLE_1));
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    assertThat(coordinator.getState(ANY_ID_2).isPresent()).isFalse();
  }

  @Test
  public void
      get_GetGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws ExecutionException, CoordinatorException {
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        true);
  }

  @Test
  public void
      scan_ScanGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws ExecutionException, CoordinatorException {
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        false);
  }

  private void
      selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
          boolean isGet) throws ExecutionException, TransactionException, CoordinatorException {
    // Arrange
    long prepared_at = System.currentTimeMillis() - RecoveryHandler.TRANSACTION_LIFETIME_MILLIS;
    populatePreparedRecordAndCoordinatorStateRecord(
        TABLE_1, TransactionState.PREPARED, prepared_at, null);

    TwoPhaseConsensusCommit transaction = manager.start();

    // Act Assert
    assertThatThrownBy(
            () -> {
              if (isGet) {
                transaction.get(prepareGet(0, 0, TABLE_1));
              } else {
                transaction.scan(prepareScan(0, 0, 0, TABLE_1));
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    Optional<Result> result;
    if (isGet) {
      result = transaction.get(prepareGet(0, 0, TABLE_1));
    } else {
      List<Result> results = transaction.scan(prepareScan(0, 0, 0, TABLE_1));
      assertThat(results.size()).isEqualTo(1);
      result = Optional.of(results.get(0));
    }

    assertThat(result.isPresent()).isTrue();
    assertThat(getAccountId(result.get())).isEqualTo(0);
    assertThat(getAccountType(result.get())).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(0); // a rolled back value

    assertThat(coordinator.getState(ANY_ID_2).isPresent()).isTrue();
    assertThat(coordinator.getState(ANY_ID_2).get().getState()).isEqualTo(TransactionState.ABORTED);
  }

  @Test
  public void get_GetGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
      throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        true);
  }

  @Test
  public void
      scan_ScanGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
          throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForPreparedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        false);
  }

  private void
      selection_SelectionGivenForPreparedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly(
          boolean isGet) throws ExecutionException, TransactionException, CoordinatorException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        TABLE_1, TransactionState.PREPARED, current, TransactionState.COMMITTED);

    TwoPhaseConsensusCommit transaction = manager.start();

    transaction.setBeforeRecoveryHook(
        () -> {
          TwoPhaseConsensusCommit another = manager.start();
          assertThatThrownBy(
                  () -> {
                    if (isGet) {
                      another.get(prepareGet(0, 0, TABLE_1));
                    } else {
                      another.scan(prepareScan(0, 0, 0, TABLE_1));
                    }
                  })
              .isInstanceOf(UncommittedRecordException.class);
        });

    // Act Assert
    assertThatThrownBy(
            () -> {
              if (isGet) {
                transaction.get(prepareGet(0, 0, TABLE_1));
              } else {
                transaction.scan(prepareScan(0, 0, 0, TABLE_1));
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    Optional<Result> result;
    if (isGet) {
      result = transaction.get(prepareGet(0, 0, TABLE_1));
    } else {
      List<Result> results = transaction.scan(prepareScan(0, 0, 0, TABLE_1));
      assertThat(results.size()).isEqualTo(1);
      result = Optional.of(results.get(0));
    }

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
        true);
  }

  @Test
  public void
      scan_ScanGivenForPreparedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly()
          throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForPreparedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly(
        false);
  }

  private void
      selection_SelectionGivenForPreparedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly(
          boolean isGet) throws ExecutionException, TransactionException, CoordinatorException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        TABLE_1, TransactionState.PREPARED, current, TransactionState.ABORTED);

    TwoPhaseConsensusCommit transaction = manager.start();

    transaction.setBeforeRecoveryHook(
        () -> {
          TwoPhaseConsensusCommit another = manager.start();
          assertThatThrownBy(
                  () -> {
                    if (isGet) {
                      another.get(prepareGet(0, 0, TABLE_1));
                    } else {
                      another.scan(prepareScan(0, 0, 0, TABLE_1));
                    }
                  })
              .isInstanceOf(UncommittedRecordException.class);
        });

    // Act Assert
    assertThatThrownBy(
            () -> {
              if (isGet) {
                transaction.get(prepareGet(0, 0, TABLE_1));
              } else {
                transaction.scan(prepareScan(0, 0, 0, TABLE_1));
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    Optional<Result> result;
    if (isGet) {
      result = transaction.get(prepareGet(0, 0, TABLE_1));
    } else {
      List<Result> results = transaction.scan(prepareScan(0, 0, 0, TABLE_1));
      assertThat(results.size()).isEqualTo(1);
      result = Optional.of(results.get(0));
    }

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
        true);
  }

  @Test
  public void
      scan_ScanGivenForPreparedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly()
          throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForPreparedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly(
        false);
  }

  private void selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward(
      boolean isGet) throws ExecutionException, TransactionException, CoordinatorException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        TABLE_1, TransactionState.DELETED, current, TransactionState.COMMITTED);

    TwoPhaseConsensusCommit transaction = manager.start();

    // Act Assert
    assertThatThrownBy(
            () -> {
              if (isGet) {
                transaction.get(prepareGet(0, 0, TABLE_1));
              } else {
                transaction.scan(prepareScan(0, 0, 0, TABLE_1));
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    if (isGet) {
      Optional<Result> result = transaction.get(prepareGet(0, 0, TABLE_1));
      assertThat(result.isPresent()).isFalse(); // deleted
    } else {
      List<Result> results = transaction.scan(prepareScan(0, 0, 0, TABLE_1));
      assertThat(results.size()).isEqualTo(0); // deleted
    }
  }

  @Test
  public void get_GetGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward(true);
  }

  @Test
  public void scan_ScanGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward()
      throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommitted_ShouldRollforward(false);
  }

  private void selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback(
      boolean isGet) throws ExecutionException, TransactionException, CoordinatorException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        TABLE_1, TransactionState.DELETED, current, TransactionState.ABORTED);

    TwoPhaseConsensusCommit transaction = manager.start();

    // Act Assert
    assertThatThrownBy(
            () -> {
              if (isGet) {
                transaction.get(prepareGet(0, 0, TABLE_1));
              } else {
                transaction.scan(prepareScan(0, 0, 0, TABLE_1));
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    Optional<Result> result;
    if (isGet) {
      result = transaction.get(prepareGet(0, 0, TABLE_1));
    } else {
      List<Result> results = transaction.scan(prepareScan(0, 0, 0, TABLE_1));
      assertThat(results.size()).isEqualTo(1);
      result = Optional.of(results.get(0));
    }

    assertThat(result.isPresent()).isTrue();
    assertThat(getAccountId(result.get())).isEqualTo(0);
    assertThat(getAccountType(result.get())).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(0); // a rolled back value
  }

  @Test
  public void get_GetGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback()
      throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback(true);
  }

  @Test
  public void scan_ScanGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback()
      throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateAborted_ShouldRollback(false);
  }

  private void
      selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
          boolean isGet) throws ExecutionException, CoordinatorException {
    // Arrange
    long prepared_at = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        TABLE_1, TransactionState.DELETED, prepared_at, null);

    TwoPhaseConsensusCommit transaction = manager.start();

    // Act Assert
    assertThatThrownBy(
            () -> {
              if (isGet) {
                transaction.get(prepareGet(0, 0, TABLE_1));
              } else {
                transaction.scan(prepareScan(0, 0, 0, TABLE_1));
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    assertThat(coordinator.getState(ANY_ID_2).isPresent()).isFalse();
  }

  @Test
  public void
      get_GetGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws ExecutionException, CoordinatorException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        true);
  }

  @Test
  public void
      scan_ScanGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction()
          throws ExecutionException, CoordinatorException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndNotExpired_ShouldNotAbortTransaction(
        false);
  }

  private void
      selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
          boolean isGet) throws ExecutionException, TransactionException, CoordinatorException {
    // Arrange
    long prepared_at = System.currentTimeMillis() - RecoveryHandler.TRANSACTION_LIFETIME_MILLIS;
    populatePreparedRecordAndCoordinatorStateRecord(
        TABLE_1, TransactionState.DELETED, prepared_at, null);

    TwoPhaseConsensusCommit transaction = manager.start();

    // Act Assert
    assertThatThrownBy(
            () -> {
              if (isGet) {
                transaction.get(prepareGet(0, 0, TABLE_1));
              } else {
                transaction.scan(prepareScan(0, 0, 0, TABLE_1));
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    Optional<Result> result;
    if (isGet) {
      result = transaction.get(prepareGet(0, 0, TABLE_1));
    } else {
      List<Result> results = transaction.scan(prepareScan(0, 0, 0, TABLE_1));
      assertThat(results.size()).isEqualTo(1);
      result = Optional.of(results.get(0));
    }

    assertThat(result.isPresent()).isTrue();
    assertThat(getAccountId(result.get())).isEqualTo(0);
    assertThat(getAccountType(result.get())).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(0); // a rolled back value

    assertThat(coordinator.getState(ANY_ID_2).isPresent()).isTrue();
    assertThat(coordinator.getState(ANY_ID_2).get().getState()).isEqualTo(TransactionState.ABORTED);
  }

  @Test
  public void get_GetGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
      throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        true);
  }

  @Test
  public void
      scan_ScanGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction()
          throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateNotExistAndExpired_ShouldAbortTransaction(
        false);
  }

  private void
      selection_SelectionGivenForDeletedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly(
          boolean isGet) throws ExecutionException, TransactionException, CoordinatorException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        TABLE_1, TransactionState.DELETED, current, TransactionState.COMMITTED);

    TwoPhaseConsensusCommit transaction = manager.start();

    transaction.setBeforeRecoveryHook(
        () -> {
          TwoPhaseConsensusCommit another = manager.start();
          assertThatThrownBy(
                  () -> {
                    if (isGet) {
                      another.get(prepareGet(0, 0, TABLE_1));
                    } else {
                      another.scan(prepareScan(0, 0, 0, TABLE_1));
                    }
                  })
              .isInstanceOf(UncommittedRecordException.class);
        });

    // Act Assert
    assertThatThrownBy(
            () -> {
              if (isGet) {
                transaction.get(prepareGet(0, 0, TABLE_1));
              } else {
                transaction.scan(prepareScan(0, 0, 0, TABLE_1));
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    if (isGet) {
      Optional<Result> result = transaction.get(prepareGet(0, 0, TABLE_1));
      assertThat(result.isPresent()).isFalse(); // deleted
    } else {
      List<Result> results = transaction.scan(prepareScan(0, 0, 0, TABLE_1));
      assertThat(results.size()).isEqualTo(0); // deleted
    }
  }

  @Test
  public void
      get_GetGivenForDeletedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly()
          throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly(
        true);
  }

  @Test
  public void
      scan_ScanGivenForDeletedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly()
          throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateCommittedAndRolledForwardByAnother_ShouldRollforwardProperly(
        false);
  }

  private void
      selection_SelectionGivenForDeletedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly(
          boolean isGet) throws ExecutionException, TransactionException, CoordinatorException {
    // Arrange
    long current = System.currentTimeMillis();
    populatePreparedRecordAndCoordinatorStateRecord(
        TABLE_1, TransactionState.DELETED, current, TransactionState.ABORTED);

    TwoPhaseConsensusCommit transaction = manager.start();

    transaction.setBeforeRecoveryHook(
        () -> {
          TwoPhaseConsensusCommit another = manager.start();
          assertThatThrownBy(
                  () -> {
                    if (isGet) {
                      another.get(prepareGet(0, 0, TABLE_1));
                    } else {
                      another.scan(prepareScan(0, 0, 0, TABLE_1));
                    }
                  })
              .isInstanceOf(UncommittedRecordException.class);
        });

    // Act Assert
    assertThatThrownBy(
            () -> {
              if (isGet) {
                transaction.get(prepareGet(0, 0, TABLE_1));
              } else {
                transaction.scan(prepareScan(0, 0, 0, TABLE_1));
              }
            })
        .isInstanceOf(UncommittedRecordException.class);

    // Assert
    Optional<Result> result;
    if (isGet) {
      result = transaction.get(prepareGet(0, 0, TABLE_1));
    } else {
      List<Result> results = transaction.scan(prepareScan(0, 0, 0, TABLE_1));
      assertThat(results.size()).isEqualTo(1);
      result = Optional.of(results.get(0));
    }

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
        true);
  }

  @Test
  public void
      scan_ScanGivenForDeletedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly()
          throws ExecutionException, TransactionException, CoordinatorException {
    selection_SelectionGivenForDeletedWhenCoordinatorStateAbortedAndRolledBackByAnother_ShouldRollbackProperly(
        false);
  }

  @Test
  public void getAndScan_CommitHappenedInBetween_ShouldReadRepeatably()
      throws TransactionException {
    // Arrange
    TwoPhaseConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.commit();

    TwoPhaseConsensusCommit transaction1 = manager.start();
    Optional<Result> result1 = transaction1.get(prepareGet(0, 0, TABLE_1));

    TwoPhaseConsensusCommit transaction2 = manager.start();
    transaction2.get(prepareGet(0, 0, TABLE_1));
    transaction2.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 2));
    transaction2.prepare();
    transaction2.commit();

    // Act
    Result result2 = transaction1.scan(prepareScan(0, 0, 0, TABLE_1)).get(0);
    Optional<Result> result3 = transaction1.get(prepareGet(0, 0, TABLE_1));

    // Assert
    assertThat(result1).isPresent();
    assertThat(result1.get()).isEqualTo(result2);
    assertThat(result1).isEqualTo(result3);
  }

  @Test
  public void putAndCommit_PutGivenForNonExisting_ShouldCreateRecord() throws TransactionException {
    // Arrange
    int expected = INITIAL_BALANCE;
    TwoPhaseConsensusCommit transaction = manager.start();

    // Act
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, expected));
    transaction.prepare();
    transaction.commit();

    // Assert
    TwoPhaseConsensusCommit another = manager.start();
    Optional<Result> result = another.get(prepareGet(0, 0, TABLE_1));

    assertThat(result).isPresent();
    assertThat(getAccountId(result.get())).isEqualTo(0);
    assertThat(getAccountType(result.get())).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(expected);
  }

  @Test
  public void putAndCommit_PutGivenForExistingAfterRead_ShouldUpdateRecord()
      throws TransactionException {
    // Arrange
    populate(TABLE_1);

    TwoPhaseConsensusCommit transaction = manager.start();

    // Act
    Optional<Result> result = transaction.get(prepareGet(0, 0, TABLE_1));
    assertThat(result.isPresent()).isTrue();

    int afterBalance = getBalance(result.get()) + 100;
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, afterBalance));
    transaction.prepare();
    transaction.commit();

    // Assert
    TwoPhaseConsensusCommit another = manager.start();
    result = another.get(prepareGet(0, 0, TABLE_1));

    assertThat(result).isPresent();
    assertThat(getAccountId(result.get())).isEqualTo(0);
    assertThat(getAccountType(result.get())).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(afterBalance);
  }

  @Test
  public void putAndCommit_PutGivenForExistingAndNeverRead_ShouldThrowPreparationException()
      throws TransactionException {
    // Arrange
    populate(TABLE_1);

    TwoPhaseConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1100));

    // Act Assert
    assertThatThrownBy(transaction::prepare).isInstanceOf(PreparationException.class);
    transaction.rollback();

    // Assert
    TwoPhaseConsensusCommit another = manager.start();
    Optional<Result> result = another.get(prepareGet(0, 0, TABLE_1));

    assertThat(result).isPresent();
    assertThat(getAccountId(result.get())).isEqualTo(0);
    assertThat(getAccountType(result.get())).isEqualTo(0);
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE); // a rolled back value
  }

  @Test
  public void putAndCommit_GetsAndPutsGiven_ShouldCommitProperly() throws TransactionException {
    // Arrange
    populate(TABLE_1);
    populate(TABLE_2);

    int amount = 100;
    int fromBalance = INITIAL_BALANCE - amount;
    int toBalance = INITIAL_BALANCE + amount;
    int fromId = 0;
    int fromType = 0;
    int toId = 1;
    int toType = 0;

    TwoPhaseConsensusCommit fromTx = manager.start();
    TwoPhaseConsensusCommit toTx = manager.join(fromTx.getId());
    transfer(fromId, fromType, TABLE_1, fromTx, toId, toType, TABLE_2, toTx, amount);

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
    TwoPhaseConsensusCommit another = manager.start();
    Optional<Result> result = another.get(prepareGet(fromId, fromType, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(fromBalance);

    result = another.get(prepareGet(toId, toType, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(toBalance);
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

    TwoPhaseConsensusCommit fromTx = manager.start();
    TwoPhaseConsensusCommit toTx = manager.join(fromTx.getId());
    fromTx.put(preparePut(fromId, fromType, TABLE_1).withValue(BALANCE, expected));
    toTx.put(preparePut(toId, toType, TABLE_2).withValue(BALANCE, expected));

    fromTx.setBeforePrepareHook(
        () ->
            assertThatCode(
                    () -> {
                      TwoPhaseConsensusCommit anotherFromTx = manager.start();
                      TwoPhaseConsensusCommit anotherToTx = manager.join(anotherFromTx.getId());
                      anotherFromTx.put(
                          preparePut(anotherFromId, anotherFromType, TABLE_2)
                              .withValue(BALANCE, expected));
                      anotherToTx.put(
                          preparePut(anotherToId, anotherToType, TABLE_1)
                              .withValue(BALANCE, expected));
                      anotherFromTx.prepare();
                      anotherToTx.prepare();
                      anotherFromTx.commit();
                      anotherToTx.commit();
                    })
                .doesNotThrowAnyException());

    // Act Assert
    assertThatThrownBy(
            () -> {
              fromTx.prepare();
              toTx.prepare();
            })
        .isInstanceOf(PreparationException.class);
    fromTx.rollback();
    toTx.rollback();

    // Assert
    TwoPhaseConsensusCommit another = manager.start();
    Optional<Result> result = another.get(prepareGet(fromId, fromType, TABLE_1));
    assertThat(result).isNotPresent();

    result = another.get(prepareGet(toId, toType, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(expected);

    result = another.get(prepareGet(anotherToId, anotherToType, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(expected);
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

    populate(TABLE_1);
    populate(TABLE_2);

    TwoPhaseConsensusCommit fromTx = manager.start();
    TwoPhaseConsensusCommit toTx = manager.join(fromTx.getId());
    fromTx.get(prepareGet(fromId, fromType, TABLE_1));
    fromTx.delete(prepareDelete(fromId, fromType, TABLE_1));
    toTx.get(prepareGet(toId, toType, TABLE_2));
    toTx.delete(prepareDelete(toId, toType, TABLE_2));

    fromTx.setBeforePrepareHook(
        () ->
            assertThatCode(
                    () -> {
                      TwoPhaseConsensusCommit anotherFromTx = manager.start();
                      TwoPhaseConsensusCommit anotherToTx = manager.join(anotherFromTx.getId());
                      transfer(
                          anotherFromId,
                          anotherFromType,
                          TABLE_2,
                          anotherFromTx,
                          anotherToId,
                          anotherToType,
                          TABLE_1,
                          anotherToTx,
                          amount);

                      anotherFromTx.prepare();
                      anotherToTx.prepare();
                      anotherFromTx.commit();
                      anotherToTx.commit();
                    })
                .doesNotThrowAnyException());

    // Act Assert
    assertThatThrownBy(
            () -> {
              fromTx.prepare();
              toTx.prepare();
            })
        .isInstanceOf(PreparationException.class);
    fromTx.rollback();
    toTx.rollback();

    // Assert
    TwoPhaseConsensusCommit another = manager.start();
    Optional<Result> result = another.get(prepareGet(fromId, fromType, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE);

    result = another.get(prepareGet(anotherFromId, anotherFromType, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE - amount);

    result = another.get(prepareGet(anotherToId, anotherToType, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE + amount);
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

    populate(TABLE_1);
    populate(TABLE_2);

    TwoPhaseConsensusCommit fromTx = manager.start();
    TwoPhaseConsensusCommit toTx = manager.join(fromTx.getId());
    transfer(fromId, fromType, TABLE_1, fromTx, toId, toType, TABLE_2, toTx, amount1);

    fromTx.setBeforePrepareHook(
        () ->
            assertThatCode(
                    () -> {
                      TwoPhaseConsensusCommit anotherFromTx = manager.start();
                      TwoPhaseConsensusCommit anotherToTx = manager.join(anotherFromTx.getId());
                      transfer(
                          anotherFromId,
                          anotherFromType,
                          TABLE_2,
                          anotherFromTx,
                          anotherToId,
                          anotherToType,
                          TABLE_1,
                          anotherToTx,
                          amount2);

                      anotherFromTx.prepare();
                      anotherToTx.prepare();
                      anotherFromTx.commit();
                      anotherToTx.commit();
                    })
                .doesNotThrowAnyException());

    // Act Assert
    assertThatThrownBy(
            () -> {
              fromTx.prepare();
              toTx.prepare();
            })
        .isInstanceOf(PreparationException.class);
    fromTx.rollback();
    toTx.rollback();

    // Assert
    TwoPhaseConsensusCommit another = manager.start();
    Optional<Result> result = another.get(prepareGet(fromId, fromType, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE);

    result = another.get(prepareGet(anotherFromId, anotherFromType, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE - amount2);

    result = another.get(prepareGet(anotherToId, anotherToType, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE + amount2);
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

    populate(TABLE_1);
    populate(TABLE_2);

    TwoPhaseConsensusCommit fromTx = manager.start();
    TwoPhaseConsensusCommit toTx = manager.join(fromTx.getId());
    transfer(fromId, fromType, TABLE_1, fromTx, toId, toType, TABLE_2, toTx, amount1);

    fromTx.setBeforePrepareHook(
        () ->
            assertThatCode(
                    () -> {
                      TwoPhaseConsensusCommit anotherFromTx = manager.start();
                      TwoPhaseConsensusCommit anotherToTx = manager.join(anotherFromTx.getId());
                      transfer(
                          anotherFromId,
                          anotherFromType,
                          TABLE_2,
                          anotherFromTx,
                          anotherToId,
                          anotherToType,
                          TABLE_1,
                          anotherToTx,
                          amount2);

                      anotherFromTx.prepare();
                      anotherToTx.prepare();
                      anotherFromTx.commit();
                      anotherToTx.commit();
                    })
                .doesNotThrowAnyException());

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
    TwoPhaseConsensusCommit another = manager.start();
    Optional<Result> result = another.get(prepareGet(fromId, fromType, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE - amount1);

    result = another.get(prepareGet(toId, toType, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE + amount1);

    result = another.get(prepareGet(anotherFromId, anotherFromType, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE - amount2);

    result = another.get(prepareGet(anotherToId, anotherToType, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(INITIAL_BALANCE + amount2);
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

    TwoPhaseConsensusCommit fromTx = manager.start();
    TwoPhaseConsensusCommit toTx = manager.join(fromTx.getId());
    fromTx.put(preparePut(fromId, fromType, TABLE_1).withValue(BALANCE, expected));
    toTx.put(preparePut(toId, toType, TABLE_2).withValue(BALANCE, expected));

    fromTx.setBeforePrepareHook(
        () ->
            assertThatCode(
                    () -> {
                      TwoPhaseConsensusCommit anotherFromTx = manager.start();
                      TwoPhaseConsensusCommit anotherToTx = manager.join(anotherFromTx.getId());
                      anotherFromTx.put(
                          preparePut(anotherFromId, anotherFromType, TABLE_2)
                              .withValue(BALANCE, expected));
                      anotherToTx.put(
                          preparePut(anotherToId, anotherToType, TABLE_1)
                              .withValue(BALANCE, expected));

                      anotherFromTx.prepare();
                      anotherToTx.prepare();
                      anotherFromTx.commit();
                      anotherToTx.commit();
                    })
                .doesNotThrowAnyException());

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
    TwoPhaseConsensusCommit another = manager.start();
    Optional<Result> result = another.get(prepareGet(fromId, fromType, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(expected);

    result = another.get(prepareGet(toId, toType, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(expected);

    result = another.get(prepareGet(anotherFromId, anotherFromType, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(expected);

    result = another.get(prepareGet(anotherToId, anotherToType, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(expected);
  }

  @Test
  public void prepare_DeleteGivenWithoutRead_ShouldThrowIllegalArgumentException()
      throws CrudException {
    // Arrange
    TwoPhaseConsensusCommit transaction = manager.start();
    transaction.delete(prepareDelete(0, 0, TABLE_1));

    // Act
    Throwable throwable = catchThrowable(transaction::prepare);

    // Assert
    assertThat(throwable).isInstanceOf(PreparationException.class);
    assertThat(throwable.getCause()).isInstanceOf(CommitException.class);
    assertThat(throwable.getCause().getCause()).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void prepare_DeleteGivenForNonExisting_ShouldThrowIllegalArgumentException()
      throws TransactionException {
    // Arrange
    TwoPhaseConsensusCommit transaction = manager.start();
    transaction.get(prepareGet(0, 0, TABLE_1));
    transaction.delete(prepareDelete(0, 0, TABLE_1));

    // Act
    Throwable throwable = catchThrowable(transaction::prepare);

    // Assert
    assertThat(throwable).isInstanceOf(PreparationException.class);
    assertThat(throwable.getCause()).isInstanceOf(CommitException.class);
    assertThat(throwable.getCause().getCause()).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void commit_DeleteGivenForExistingAfterRead_ShouldDeleteRecord()
      throws TransactionException {
    // Arrange
    populate(TABLE_1);
    TwoPhaseConsensusCommit transaction = manager.start();

    // Act
    Optional<Result> result = transaction.get(prepareGet(0, 0, TABLE_1));
    transaction.delete(prepareDelete(0, 0, TABLE_1));
    transaction.prepare();
    transaction.commit();

    // Assert
    assertThat(result.isPresent()).isTrue();

    TwoPhaseConsensusCommit another = manager.start();
    result = another.get(prepareGet(0, 0, TABLE_1));
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

    populate(TABLE_1);
    populate(TABLE_2);

    TwoPhaseConsensusCommit tx1 = manager.start();
    TwoPhaseConsensusCommit tx2 = manager.join(tx1.getId());
    deletes(account1Id, account1Type, TABLE_1, tx1, account2Id, account2Type, TABLE_2, tx2);

    tx1.setBeforePrepareHook(
        () ->
            assertThatCode(
                    () -> {
                      TwoPhaseConsensusCommit tx3 = manager.start();
                      TwoPhaseConsensusCommit tx4 = manager.join(tx3.getId());
                      deletes(
                          account2Id,
                          account2Type,
                          TABLE_2,
                          tx3,
                          account3Id,
                          account3Type,
                          TABLE_1,
                          tx4);

                      tx3.prepare();
                      tx4.prepare();
                      tx3.commit();
                      tx4.commit();
                    })
                .doesNotThrowAnyException());

    // Act Assert
    assertThatThrownBy(
            () -> {
              tx1.prepare();
              tx2.prepare();
            })
        .isInstanceOf(PreparationException.class);
    tx1.rollback();
    tx2.rollback();

    // Assert
    TwoPhaseConsensusCommit another = manager.start();
    Optional<Result> result = another.get(prepareGet(account1Id, account1Type, TABLE_1));
    assertThat(result).isPresent();

    result = another.get(prepareGet(account2Id, account2Type, TABLE_2));
    assertThat(result).isNotPresent();

    result = another.get(prepareGet(account3Id, account3Type, TABLE_1));
    assertThat(result).isNotPresent();
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

    populate(TABLE_1);
    populate(TABLE_2);

    TwoPhaseConsensusCommit tx1 = manager.start();
    TwoPhaseConsensusCommit tx2 = manager.join(tx1.getId());
    deletes(account1Id, account1Type, TABLE_1, tx1, account2Id, account2Type, TABLE_2, tx2);

    tx1.setBeforePrepareHook(
        () ->
            assertThatCode(
                    () -> {
                      TwoPhaseConsensusCommit tx3 = manager.start();
                      TwoPhaseConsensusCommit tx4 = manager.join(tx3.getId());
                      deletes(
                          account3Id,
                          account3Type,
                          TABLE_2,
                          tx3,
                          account4Id,
                          account4Type,
                          TABLE_1,
                          tx4);

                      tx3.prepare();
                      tx4.prepare();
                      tx3.commit();
                      tx4.commit();
                    })
                .doesNotThrowAnyException());

    // Act Assert
    assertThatCode(
            () -> {
              tx1.prepare();
              tx2.prepare();
              tx1.commit();
              tx2.commit();
            })
        .doesNotThrowAnyException();

    // Assert
    TwoPhaseConsensusCommit another = manager.start();
    Optional<Result> result = another.get(prepareGet(account1Id, account1Type, TABLE_1));
    assertThat(result).isNotPresent();

    result = another.get(prepareGet(account2Id, account2Type, TABLE_2));
    assertThat(result).isNotPresent();

    result = another.get(prepareGet(account3Id, account3Type, TABLE_2));
    assertThat(result).isNotPresent();

    result = another.get(prepareGet(account4Id, account4Type, TABLE_1));
    assertThat(result).isNotPresent();
  }

  @Test
  public void commit_WriteSkewOnExistingRecordsWithSnapshot_ShouldProduceNonSerializableResult()
      throws TransactionException {
    // Arrange
    TwoPhaseConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction.put(preparePut(1, 0, TABLE_2).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.commit();

    // Act
    TwoPhaseConsensusCommit tx1Sub1 = manager.start();
    TwoPhaseConsensusCommit tx1Sub2 = manager.join(tx1Sub1.getId());
    Optional<Result> result = tx1Sub2.get(prepareGet(1, 0, TABLE_2));
    assertThat(result).isPresent();
    int current1 = getBalance(result.get());
    tx1Sub1.get(prepareGet(0, 0, TABLE_1));
    tx1Sub1.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, current1 + 1));

    TwoPhaseConsensusCommit tx2Sub1 = manager.start();
    TwoPhaseConsensusCommit tx2Sub2 = manager.join(tx2Sub1.getId());
    result = tx2Sub1.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isPresent();
    int current2 = getBalance(result.get());
    tx2Sub2.get(prepareGet(1, 0, TABLE_2));
    tx2Sub2.put(preparePut(1, 0, TABLE_2).withValue(BALANCE, current2 + 1));

    tx1Sub1.prepare();
    tx1Sub2.prepare();
    tx1Sub1.commit();
    tx1Sub2.commit();

    tx2Sub1.prepare();
    tx2Sub2.prepare();
    tx2Sub1.commit();
    tx2Sub2.commit();

    // Assert
    transaction = manager.start();
    // the results can not be produced by executing the transactions serially
    result = transaction.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(current1 + 1);
    result = transaction.get(prepareGet(1, 0, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(current2 + 1);
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowPreparationException()
          throws TransactionException {
    // Arrange
    TwoPhaseConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction.put(preparePut(1, 0, TABLE_2).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.commit();

    Isolation isolation = Isolation.SERIALIZABLE;
    SerializableStrategy strategy = SerializableStrategy.EXTRA_WRITE;

    // Act Assert
    TwoPhaseConsensusCommit tx1Sub1 = manager.start(isolation, strategy);
    TwoPhaseConsensusCommit tx1Sub2 = manager.join(tx1Sub1.getId(), isolation, strategy);
    Optional<Result> result = tx1Sub2.get(prepareGet(1, 0, TABLE_2));
    assertThat(result).isPresent();
    int current1 = getBalance(result.get());
    tx1Sub1.get(prepareGet(0, 0, TABLE_1));
    tx1Sub1.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, current1 + 1));

    TwoPhaseConsensusCommit tx2Sub1 = manager.start(isolation, strategy);
    TwoPhaseConsensusCommit tx2Sub2 = manager.join(tx2Sub1.getId(), isolation, strategy);
    result = tx2Sub1.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isPresent();
    int current2 = getBalance(result.get());
    tx2Sub2.get(prepareGet(1, 0, TABLE_2));
    tx2Sub2.put(preparePut(1, 0, TABLE_2).withValue(BALANCE, current2 + 1));

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
    transaction = manager.start();
    result = transaction.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(current1 + 1);
    result = transaction.get(prepareGet(1, 0, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(current2);
  }

  @Test
  public void
      commit_WriteSkewOnExistingRecordsWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowValidationException()
          throws TransactionException {
    // Arrange
    TwoPhaseConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction.put(preparePut(1, 0, TABLE_2).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.commit();

    Isolation isolation = Isolation.SERIALIZABLE;
    SerializableStrategy strategy = SerializableStrategy.EXTRA_READ;

    // Act Assert
    TwoPhaseConsensusCommit tx1Sub1 = manager.start(isolation, strategy);
    TwoPhaseConsensusCommit tx1Sub2 = manager.join(tx1Sub1.getId(), isolation, strategy);
    Optional<Result> result = tx1Sub2.get(prepareGet(1, 0, TABLE_2));
    assertThat(result).isPresent();
    int current1 = getBalance(result.get());
    tx1Sub1.get(prepareGet(0, 0, TABLE_1));
    tx1Sub1.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, current1 + 1));

    TwoPhaseConsensusCommit tx2Sub1 = manager.start(isolation, strategy);
    TwoPhaseConsensusCommit tx2Sub2 = manager.join(tx2Sub1.getId(), isolation, strategy);
    result = tx2Sub1.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isPresent();
    int current2 = getBalance(result.get());
    tx2Sub2.get(prepareGet(1, 0, TABLE_2));
    tx2Sub2.put(preparePut(1, 0, TABLE_2).withValue(BALANCE, current2 + 1));

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
    transaction = manager.start();
    result = transaction.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(current1 + 1);
    result = transaction.get(prepareGet(1, 0, TABLE_2));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(current2);
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWrite_OneShouldCommitTheOtherShouldThrowPreparationException()
          throws TransactionException {
    // Arrange
    Isolation isolation = Isolation.SERIALIZABLE;
    SerializableStrategy strategy = SerializableStrategy.EXTRA_WRITE;

    // Act Assert
    TwoPhaseConsensusCommit tx1Sub1 = manager.start(isolation, strategy);
    TwoPhaseConsensusCommit tx1Sub2 = manager.join(tx1Sub1.getId(), isolation, strategy);
    Optional<Result> result = tx1Sub2.get(prepareGet(1, 0, TABLE_2));
    assertThat(result).isNotPresent();
    int current1 = 0;
    tx1Sub1.get(prepareGet(0, 0, TABLE_1));
    tx1Sub1.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, current1 + 1));

    TwoPhaseConsensusCommit tx2Sub1 = manager.start(isolation, strategy);
    TwoPhaseConsensusCommit tx2Sub2 = manager.join(tx2Sub1.getId(), isolation, strategy);
    result = tx2Sub1.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isNotPresent();
    int current2 = 0;
    tx2Sub2.get(prepareGet(1, 0, TABLE_2));
    tx2Sub2.put(preparePut(1, 0, TABLE_2).withValue(BALANCE, current2 + 1));

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
    TwoPhaseConsensusCommit transaction = manager.start();
    result = transaction.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(current1 + 1);
    result = transaction.get(prepareGet(1, 0, TABLE_2));
    assertThat(result).isNotPresent();
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraWriteAndCommitStatusFailed_ShouldRollbackProperly()
          throws TransactionException, CoordinatorException {
    // Arrange
    State state = new State(ANY_ID_1, TransactionState.ABORTED);
    coordinator.putState(state);

    Isolation isolation = Isolation.SERIALIZABLE;
    SerializableStrategy strategy = SerializableStrategy.EXTRA_WRITE;

    // Act
    TwoPhaseConsensusCommit txSub1 = manager.start(ANY_ID_1, isolation, strategy);
    TwoPhaseConsensusCommit txSub2 = manager.join(txSub1.getId(), isolation, strategy);

    Optional<Result> result = txSub2.get(prepareGet(1, 0, TABLE_2));
    assertThat(result).isNotPresent();
    result = txSub1.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isNotPresent();
    int current1 = 0;
    txSub1.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, current1 + 1));

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
    TwoPhaseConsensusCommit transaction = manager.start();
    result = transaction.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isNotPresent();
    result = transaction.get(prepareGet(1, 0, TABLE_2));
    assertThat(result).isNotPresent();
  }

  @Test
  public void
      commit_WriteSkewOnNonExistingRecordsWithSerializableWithExtraRead_OneShouldCommitTheOtherShouldThrowValidationException()
          throws TransactionException {
    // Arrange
    Isolation isolation = Isolation.SERIALIZABLE;
    SerializableStrategy strategy = SerializableStrategy.EXTRA_READ;

    // Act
    TwoPhaseConsensusCommit tx1Sub1 = manager.start(isolation, strategy);
    TwoPhaseConsensusCommit tx1Sub2 = manager.join(tx1Sub1.getId(), isolation, strategy);
    Optional<Result> result = tx1Sub2.get(prepareGet(1, 0, TABLE_2));
    assertThat(result).isNotPresent();
    int current1 = 0;
    tx1Sub1.get(prepareGet(0, 0, TABLE_1));
    tx1Sub1.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, current1 + 1));

    TwoPhaseConsensusCommit tx2Sub1 = manager.start(isolation, strategy);
    TwoPhaseConsensusCommit tx2Sub2 = manager.join(tx2Sub1.getId(), isolation, strategy);
    result = tx2Sub1.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isNotPresent();
    int current2 = 0;
    tx2Sub2.get(prepareGet(1, 0, TABLE_2));
    tx2Sub2.put(preparePut(1, 0, TABLE_2).withValue(BALANCE, current2 + 1));

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
    TwoPhaseConsensusCommit transaction = manager.start();
    result = transaction.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(current1 + 1);
    result = transaction.get(prepareGet(1, 0, TABLE_2));
    assertThat(result).isNotPresent();
  }

  @Test
  public void
      commit_WriteSkewWithScanOnNonExistingRecordsWithSerializableWithExtraWrite_ShouldThrowPreparationException()
          throws TransactionException {
    // Arrange
    Isolation isolation = Isolation.SERIALIZABLE;
    SerializableStrategy strategy = SerializableStrategy.EXTRA_WRITE;

    // Act Assert
    TwoPhaseConsensusCommit transaction1 = manager.start(isolation, strategy);
    List<Result> results = transaction1.scan(prepareScan(0, 0, 1, TABLE_1));
    assertThat(results).isEmpty();
    int count1 = 0;
    transaction1.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, count1 + 1));

    TwoPhaseConsensusCommit transaction2 = manager.start(isolation, strategy);
    results = transaction2.scan(prepareScan(0, 0, 1, TABLE_1));
    assertThat(results).isEmpty();
    int count2 = 0;
    transaction2.put(preparePut(0, 1, TABLE_1).withValue(BALANCE, count2 + 1));

    assertThatThrownBy(transaction1::prepare).isInstanceOf(PreparationException.class);
    transaction1.rollback();

    assertThatThrownBy(transaction2::prepare).isInstanceOf(PreparationException.class);
    transaction2.rollback();

    // Assert
    TwoPhaseConsensusCommit transaction = manager.start();
    Optional<Result> result = transaction.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isNotPresent();
    result = transaction.get(prepareGet(0, 1, TABLE_1));
    assertThat(result).isNotPresent();
  }

  @Test
  public void
      commit_WriteSkewWithScanOnNonExistingRecordsWithSerializableWithExtraRead_ShouldThrowValidationException()
          throws TransactionException {
    // Arrange
    Isolation isolation = Isolation.SERIALIZABLE;
    SerializableStrategy strategy = SerializableStrategy.EXTRA_READ;

    // Act Assert
    TwoPhaseConsensusCommit transaction1 = manager.start(isolation, strategy);
    List<Result> results = transaction1.scan(prepareScan(0, 0, 1, TABLE_1));
    assertThat(results).isEmpty();
    int count1 = 0;
    transaction1.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, count1 + 1));

    TwoPhaseConsensusCommit transaction2 = manager.start(isolation, strategy);
    results = transaction2.scan(prepareScan(0, 0, 1, TABLE_1));
    assertThat(results).isEmpty();
    int count2 = 0;
    transaction2.put(preparePut(0, 1, TABLE_1).withValue(BALANCE, count2 + 1));

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
    TwoPhaseConsensusCommit transaction = manager.start();
    Optional<Result> result = transaction.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(count1 + 1);
    result = transaction.get(prepareGet(0, 1, TABLE_1));
    assertThat(result).isNotPresent();
  }

  @Test
  public void
      commit_WriteSkewWithScanOnExistingRecordsWithSerializableWithExtraRead_ShouldThrowValidationException()
          throws TransactionException {
    // Arrange
    TwoPhaseConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction.put(preparePut(0, 1, TABLE_1).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.commit();

    Isolation isolation = Isolation.SERIALIZABLE;
    SerializableStrategy strategy = SerializableStrategy.EXTRA_READ;

    // Act Assert
    TwoPhaseConsensusCommit transaction1 = manager.start(isolation, strategy);
    List<Result> results = transaction1.scan(prepareScan(0, 0, 1, TABLE_1));
    assertThat(results.size()).isEqualTo(2);
    int count1 = results.size();
    transaction1.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, count1 + 1));

    TwoPhaseConsensusCommit transaction2 = manager.start(isolation, strategy);
    results = transaction2.scan(prepareScan(0, 0, 1, TABLE_1));
    assertThat(results.size()).isEqualTo(2);
    int count2 = results.size();
    transaction2.put(preparePut(0, 1, TABLE_1).withValue(BALANCE, count2 + 1));

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
    transaction = manager.start();
    Optional<Result> result = transaction.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(count1 + 1);
    result = transaction.get(prepareGet(0, 1, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(1);
  }

  @Test
  public void scanAndCommit_MultipleScansGivenInTransactionWithExtraRead_ShouldCommitProperly()
      throws TransactionException {
    // Arrange
    populate(TABLE_1);

    // Act Assert
    TwoPhaseConsensusCommit transaction =
        manager.start(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    transaction.scan(prepareScan(0, TABLE_1));
    transaction.scan(prepareScan(1, TABLE_1));
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
    TwoPhaseConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 2));
    transaction.prepare();
    transaction.commit();

    // Act
    TwoPhaseConsensusCommit transaction1 = manager.start();
    Optional<Result> result = transaction1.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isPresent();
    int balance1 = getBalance(result.get());
    transaction1.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, balance1 + 1));

    TwoPhaseConsensusCommit transaction2 = manager.start();
    transaction2.get(prepareGet(0, 0, TABLE_1));
    transaction2.delete(prepareDelete(0, 0, TABLE_1));
    transaction2.prepare();
    transaction2.commit();

    // the same transaction processing as transaction1
    TwoPhaseConsensusCommit transaction3 = manager.start();
    result = transaction3.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isNotPresent();
    int balance3 = 0;
    transaction3.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, balance3 + 1));
    transaction3.prepare();
    transaction3.commit();

    assertThatThrownBy(transaction1::prepare).isInstanceOf(PreparationException.class);
    transaction1.rollback();

    // Assert
    transaction = manager.start();
    result = transaction.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(1);
  }

  @Test
  public void deleteAndCommit_DeleteGivenInBetweenTransactions_ShouldProduceSerializableResults()
      throws TransactionException {
    // Arrange
    TwoPhaseConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 2));
    transaction.prepare();
    transaction.commit();

    // Act
    TwoPhaseConsensusCommit transaction1 = manager.start();
    transaction1.get(prepareGet(0, 0, TABLE_1));
    transaction1.delete(prepareDelete(0, 0, TABLE_1));

    TwoPhaseConsensusCommit transaction2 = manager.start();
    transaction2.get(prepareGet(0, 0, TABLE_1));
    transaction2.delete(prepareDelete(0, 0, TABLE_1));
    transaction2.prepare();
    transaction2.commit();

    TwoPhaseConsensusCommit transaction3 = manager.start();
    Optional<Result> result = transaction3.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isNotPresent();
    int balance3 = 0;
    transaction3.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, balance3 + 1));
    transaction3.prepare();
    transaction3.commit();

    assertThatThrownBy(transaction1::prepare).isInstanceOf(PreparationException.class);
    transaction1.rollback();

    // Assert
    transaction = manager.start();
    result = transaction.get(prepareGet(0, 0, TABLE_1));
    assertThat(result).isPresent();
    assertThat(getBalance(result.get())).isEqualTo(1);
  }

  @Test
  public void get_PutCalledBefore_ShouldGet() throws CrudException {
    // Arrange
    TwoPhaseConsensusCommit transaction = manager.start();

    // Act
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    Get get = prepareGet(0, 0, TABLE_1);
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
    TwoPhaseConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.commit();

    // Act
    TwoPhaseConsensusCommit transaction1 = manager.start();
    Optional<Result> resultBefore = transaction1.get(prepareGet(0, 0, TABLE_1));
    transaction1.delete(prepareDelete(0, 0, TABLE_1));
    Optional<Result> resultAfter = transaction1.get(prepareGet(0, 0, TABLE_1));

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
    TwoPhaseConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.commit();

    // Act Assert
    TwoPhaseConsensusCommit transaction1 = manager.start();
    List<Result> resultBefore = transaction1.scan(prepareScan(0, 0, 0, TABLE_1));
    transaction1.delete(prepareDelete(0, 0, TABLE_1));
    List<Result> resultAfter = transaction1.scan(prepareScan(0, 0, 0, TABLE_1));

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
    TwoPhaseConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.commit();

    // Act Assert
    TwoPhaseConsensusCommit transaction1 = manager.start();
    Optional<Result> resultBefore = transaction1.get(prepareGet(0, 0, TABLE_1));
    transaction1.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 2));
    transaction1.delete(prepareDelete(0, 0, TABLE_1));

    assertThatCode(
            () -> {
              transaction1.prepare();
              transaction1.commit();
            })
        .doesNotThrowAnyException();

    // Assert
    TwoPhaseConsensusCommit transaction2 = manager.start();
    Optional<Result> resultAfter = transaction2.get(prepareGet(0, 0, TABLE_1));

    assertThat(resultBefore.isPresent()).isTrue();
    assertThat(resultAfter.isPresent()).isFalse();
  }

  @Test
  public void put_DeleteCalledBefore_ShouldThrowIllegalArgumentException()
      throws TransactionException {
    // Arrange
    TwoPhaseConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.commit();

    // Act
    TwoPhaseConsensusCommit transaction1 = manager.start();
    Get get = prepareGet(0, 0, TABLE_1);
    transaction1.get(get);
    transaction1.delete(prepareDelete(0, 0, TABLE_1));
    Throwable thrown =
        catchThrowable(() -> transaction1.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 2)));
    transaction1.rollback();

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void scan_OverlappingPutGivenBefore_ShouldScan() throws TransactionException {
    // Arrange
    TwoPhaseConsensusCommit transaction = manager.start();
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction.put(preparePut(0, 5, TABLE_1).withValue(BALANCE, 3));
    transaction.put(preparePut(0, 3, TABLE_1).withValue(BALANCE, 2));

    // Act
    Scan scan = prepareScan(0, 0, 10, TABLE_1);
    List<Result> results = transaction.scan(scan);
    assertThatCode(
            () -> {
              transaction.prepare();
              transaction.commit();
            })
        .doesNotThrowAnyException();

    // Assert
    assertThat(results.size()).isEqualTo(3);
    assertThat(getBalance(results.get(0))).isEqualTo(1);
    assertThat(getBalance(results.get(1))).isEqualTo(2);
    assertThat(getBalance(results.get(2))).isEqualTo(3);
  }

  @Test
  public void start_CorrectTransactionIdGiven_ShouldNotThrowAnyExceptions() {
    // Arrange
    String transactionId = ANY_ID_1;

    // Act Assert
    assertThatCode(
            () -> {
              TwoPhaseConsensusCommit transaction = manager.start(transactionId);
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
    assertThatThrownBy(() -> manager.start(transactionId))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void getState_forSuccessfulTransaction_ShouldReturnCommittedState()
      throws TransactionException {
    // Arrange
    TwoPhaseConsensusCommit transaction = manager.start();
    transaction.get(prepareGet(0, 0, TABLE_1));
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction.prepare();
    transaction.commit();

    // Act
    TransactionState state = manager.getState(transaction.getId());

    // Assert
    assertThat(state).isEqualTo(TransactionState.COMMITTED);
  }

  @Test
  public void getState_forFailedTransaction_ShouldReturnAbortedState() throws TransactionException {
    // Arrange
    TwoPhaseConsensusCommit transaction1 = manager.start();
    transaction1.get(prepareGet(0, 0, TABLE_1));
    transaction1.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));

    TwoPhaseConsensusCommit transaction2 = manager.start();
    transaction2.get(prepareGet(0, 0, TABLE_1));
    transaction2.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));
    transaction2.prepare();
    transaction2.commit();

    assertThatCode(transaction1::prepare).isInstanceOf(PreparationException.class);
    transaction1.rollback();

    // Act
    TransactionState state = manager.getState(transaction1.getId());

    // Assert
    assertThat(state).isEqualTo(TransactionState.ABORTED);
  }

  @Test
  public void abort_forOngoingTransaction_ShouldAbortCorrectly() throws TransactionException {
    // Arrange
    TwoPhaseConsensusCommit transaction = manager.start();
    transaction.get(prepareGet(0, 0, TABLE_1));
    transaction.put(preparePut(0, 0, TABLE_1).withValue(BALANCE, 1));

    // Act
    manager.abort(transaction.getId());

    transaction.prepare();
    assertThatCode(transaction::commit).isInstanceOf(CommitException.class);
    transaction.rollback();

    // Assert
    TransactionState state = manager.getState(transaction.getId());
    assertThat(state).isEqualTo(TransactionState.ABORTED);
  }

  private void populate(String table) throws TransactionException {
    TwoPhaseConsensusCommit transaction = manager.start();
    for (int i = 0; i < NUM_ACCOUNTS; i++) {
      for (int j = 0; j < NUM_TYPES; j++) {
        transaction.put(preparePut(i, j, table).withValue(BALANCE, INITIAL_BALANCE));
      }
    }
    transaction.prepare();
    transaction.commit();
  }

  private void populatePreparedRecordAndCoordinatorStateRecord(
      String table,
      TransactionState recordState,
      long preparedAt,
      TransactionState coordinatorState)
      throws ExecutionException, CoordinatorException {
    Key partitionKey = new Key(new IntValue(ACCOUNT_ID, 0));
    Key clusteringKey = new Key(new IntValue(ACCOUNT_TYPE, 0));
    Put put =
        new Put(partitionKey, clusteringKey)
            .forNamespace(NAMESPACE)
            .forTable(table)
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
    storage.put(put);

    if (coordinatorState == null) {
      return;
    }
    State state = new State(ANY_ID_2, coordinatorState);
    coordinator.putState(state);
  }

  private void transfer(
      int fromId,
      int fromType,
      String fromTable,
      TwoPhaseConsensusCommit fromTx,
      int toId,
      int toType,
      String toTable,
      TwoPhaseConsensusCommit toTx,
      int amount)
      throws TransactionException {
    int fromBalance =
        fromTx
            .get(prepareGet(fromId, fromType, fromTable))
            .get()
            .getValue(BALANCE)
            .get()
            .getAsInt();
    int toBalance =
        toTx.get(prepareGet(toId, toType, toTable)).get().getValue(BALANCE).get().getAsInt();
    fromTx.put(preparePut(fromId, fromType, fromTable).withValue(BALANCE, fromBalance - amount));
    toTx.put(preparePut(toId, toType, toTable).withValue(BALANCE, toBalance + amount));
  }

  private void deletes(
      int id,
      int type,
      String table,
      TwoPhaseConsensusCommit tx,
      int anotherId,
      int anotherType,
      String anotherTable,
      TwoPhaseConsensusCommit anotherTx)
      throws TransactionException {
    tx.get(prepareGet(id, type, table));
    anotherTx.get(prepareGet(anotherId, anotherType, anotherTable));
    tx.delete(prepareDelete(id, type, table));
    anotherTx.delete(prepareDelete(anotherId, anotherType, anotherTable));
  }

  private Get prepareGet(int id, int type, String table) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    Key clusteringKey = new Key(ACCOUNT_TYPE, type);
    return new Get(partitionKey, clusteringKey)
        .forNamespace(NAMESPACE)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  private Scan prepareScan(int id, int fromType, int toType, String table) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    return new Scan(partitionKey)
        .forNamespace(NAMESPACE)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE)
        .withStart(new Key(ACCOUNT_TYPE, fromType))
        .withEnd(new Key(ACCOUNT_TYPE, toType));
  }

  private Scan prepareScan(int id, String table) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    return new Scan(partitionKey)
        .forNamespace(NAMESPACE)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  private Put preparePut(int id, int type, String table) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    Key clusteringKey = new Key(ACCOUNT_TYPE, type);
    return new Put(partitionKey, clusteringKey)
        .forNamespace(NAMESPACE)
        .forTable(table)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  private Delete prepareDelete(int id, int type, String table) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    Key clusteringKey = new Key(ACCOUNT_TYPE, type);
    return new Delete(partitionKey, clusteringKey)
        .forNamespace(NAMESPACE)
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
}
