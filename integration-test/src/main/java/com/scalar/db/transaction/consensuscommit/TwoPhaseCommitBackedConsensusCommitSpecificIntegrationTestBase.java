package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.service.TransactionFactory;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * A small, purpose-built suite that asserts the storage-level effects of the new {@link
 * com.scalar.db.api.TwoPhaseCommit} Coordinator / Participant roles when driven through the
 * single-phase {@link com.scalar.db.common.TwoPhaseCommitBackedDistributedTransactionManager}
 * facade.
 *
 * <p>Unlike {@link ConsensusCommitSpecificIntegrationTestBase} (a white-box suite that instantiates
 * a concrete {@code ConsensusCommitManager} and spies on its internals), this suite drives
 * transactions only through the public facade and then inspects the storage-visible outcome — the
 * Coordinator-state row (read directly via {@link CoordinatorStateAccessor}). It focuses on the
 * coordinator-write-omission-on-read-only behavior, which is not observable through the public
 * transaction API. Deeper role internals (write-set participant attribution, lazy recovery, group
 * commit) are covered by the Coordinator/Participant unit tests and are out of scope here.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TwoPhaseCommitBackedConsensusCommitSpecificIntegrationTestBase {

  private static final String NAMESPACE_BASE_NAME = "int_test_";
  private static final String TABLE = "tbl";
  private static final String ACCOUNT_ID = "account_id";
  private static final String ACCOUNT_TYPE = "account_type";
  private static final String BALANCE = "balance";
  private static final int INITIAL_BALANCE = 1000;

  private Properties storageProperties;
  private DistributedTransactionManager manager;
  private DistributedTransactionAdmin admin;
  private DistributedStorage storage;
  private CoordinatorStateAccessor coordinator;
  private String namespace;

  @BeforeAll
  public void beforeAll() throws Exception {
    String testName = getTestName();
    namespace = getNamespaceBaseName() + testName;

    storageProperties = getFacadeStorageProperties(testName);
    if (storageProperties.getProperty(ConsensusCommitConfig.PARTICIPANT_ID) == null) {
      storageProperties.setProperty(ConsensusCommitConfig.PARTICIPANT_ID, "participant-1");
    }

    // Default facade manager (coordinator-write omission on read-only is enabled by default) and
    // its
    // admin, routed to the test provider.
    TransactionFactory transactionFactory =
        TransactionFactory.create(copyForFacade(storageProperties));
    manager = transactionFactory.getTransactionManager();
    admin = transactionFactory.getTransactionAdmin();

    // Raw storage + Coordinator-state accessor for inspection, using a consensus-commit config.
    Properties ccProperties =
        copyWith(storageProperties, ConsensusCommitConfig.TRANSACTION_MANAGER_NAME);
    storage = StorageFactory.create(ccProperties).getStorage();
    coordinator =
        new CoordinatorStateAccessor(
            storage, new ConsensusCommitConfig(new DatabaseConfig(ccProperties)));

    Map<String, String> options = getCreationOptions();
    admin.createCoordinatorTables(true, options);
    admin.createNamespace(namespace, true, options);
    admin.createTable(namespace, TABLE, getTableMetadata(), true, options);
  }

  @BeforeEach
  public void beforeEach() throws Exception {
    admin.truncateTable(namespace, TABLE);
    admin.truncateCoordinatorTables();
  }

  @AfterAll
  public void afterAll() throws Exception {
    if (admin != null) {
      admin.dropTable(namespace, TABLE, true);
      admin.dropNamespace(namespace, true);
      admin.dropCoordinatorTables(true);
      admin.close();
    }
    if (manager != null) {
      manager.close();
    }
    if (storage != null) {
      storage.close();
    }
  }

  @Test
  public void commit_ShouldWriteCommittedCoordinatorState() throws Exception {
    // Act: commit a write transaction through the facade.
    DistributedTransaction transaction = manager.begin();
    String transactionId = transaction.getId();
    transaction.put(putBalance(0, 0));
    transaction.commit();

    // Assert: the Coordinator state row is COMMITTED for this transaction.
    Optional<CoordinatorStateAccessor.State> state = coordinator.getState(transactionId);
    assertThat(state).isPresent();
    assertThat(state.get().getState()).isEqualTo(TransactionState.COMMITTED);
  }

  @Test
  public void readOnlyCommit_WithWriteOmissionEnabled_ShouldNotWriteCoordinatorState()
      throws Exception {
    // Arrange: a committed record to read (using the default, write-omission-enabled manager).
    commitRecord(manager, 1, 1);

    // Act: a read-only transaction that writes nothing.
    DistributedTransaction readOnly = manager.beginReadOnly();
    String readOnlyId = readOnly.getId();
    readOnly.get(getBalance(1, 1));
    readOnly.commit();

    // Assert: no Coordinator state row was written for the read-only transaction.
    assertThat(coordinator.getState(readOnlyId)).isEmpty();
  }

  @Test
  public void readOnlyCommit_WithWriteOmissionDisabled_ShouldWriteCoordinatorState()
      throws Exception {
    Properties properties = copyForFacade(storageProperties);
    properties.setProperty(
        ConsensusCommitConfig.COORDINATOR_WRITE_OMISSION_ON_READ_ONLY_ENABLED, "false");
    try (DistributedTransactionManager noOmissionManager =
        TransactionFactory.create(properties).getTransactionManager()) {
      // Arrange: a committed record to read.
      commitRecord(noOmissionManager, 2, 2);

      // Act: a read-only transaction that writes nothing.
      DistributedTransaction readOnly = noOmissionManager.beginReadOnly();
      String readOnlyId = readOnly.getId();
      readOnly.get(getBalance(2, 2));
      readOnly.commit();

      // Assert: with omission disabled, a COMMITTED Coordinator state row is written even for the
      // read-only transaction.
      Optional<CoordinatorStateAccessor.State> state = coordinator.getState(readOnlyId);
      assertThat(state).isPresent();
      assertThat(state.get().getState()).isEqualTo(TransactionState.COMMITTED);
    }
  }

  @Test
  public void commit_TransactionExceedingExpirationWithContinuousCrud_ShouldCommitSuccessfully()
      throws Exception {
    // Arrange: a facade manager whose active-transaction expiration is much shorter than the
    // transaction below. Continuous CRUD keeps the participant-side (and manager-level) idle
    // timers fresh, but the coordinator role observes only begin/commit, so from the coordinator's
    // point of view this healthy transaction is silent for its whole lifetime.
    Properties properties = copyForFacade(storageProperties);
    properties.setProperty(
        DatabaseConfig.ACTIVE_TRANSACTION_MANAGEMENT_EXPIRATION_TIME_MILLIS, "1000");
    try (DistributedTransactionManager shortExpirationManager =
        TransactionFactory.create(properties).getTransactionManager()) {
      // Act: keep issuing CRUD for ~3x the expiration time, then commit.
      DistributedTransaction transaction = shortExpirationManager.begin();
      String transactionId = transaction.getId();
      long deadlineMillis = System.currentTimeMillis() + 3000;
      while (System.currentTimeMillis() < deadlineMillis) {
        transaction.get(getBalance(0, 0));
        TimeUnit.MILLISECONDS.sleep(100);
      }
      transaction.put(putBalance(0, 0));
      transaction.commit();

      // Assert: the transaction committed and its Coordinator state row is COMMITTED.
      Optional<CoordinatorStateAccessor.State> state = coordinator.getState(transactionId);
      assertThat(state).isPresent();
      assertThat(state.get().getState()).isEqualTo(TransactionState.COMMITTED);
    }
  }

  private void commitRecord(DistributedTransactionManager manager, int id, int type)
      throws Exception {
    DistributedTransaction transaction = manager.begin();
    transaction.put(putBalance(id, type));
    transaction.commit();
  }

  private Put putBalance(int id, int type) {
    return Put.newBuilder()
        .namespace(namespace)
        .table(TABLE)
        .partitionKey(Key.ofInt(ACCOUNT_ID, id))
        .clusteringKey(Key.ofInt(ACCOUNT_TYPE, type))
        .intValue(BALANCE, INITIAL_BALANCE)
        .build();
  }

  private Get getBalance(int id, int type) {
    return Get.newBuilder()
        .namespace(namespace)
        .table(TABLE)
        .partitionKey(Key.ofInt(ACCOUNT_ID, id))
        .clusteringKey(Key.ofInt(ACCOUNT_TYPE, type))
        .build();
  }

  private Properties copyForFacade(Properties source) {
    return copyWith(source, TwoPhaseCommitBackedConsensusCommitProvider.NAME);
  }

  private static Properties copyWith(Properties source, String transactionManagerName) {
    Properties properties = new Properties();
    properties.putAll(source);
    properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, transactionManagerName);
    return properties;
  }

  protected String getTestName() {
    return "tx_2pc_backed_specific";
  }

  protected String getNamespaceBaseName() {
    return NAMESPACE_BASE_NAME;
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  protected TableMetadata getTableMetadata() {
    return TableMetadata.newBuilder()
        .addColumn(ACCOUNT_ID, DataType.INT)
        .addColumn(ACCOUNT_TYPE, DataType.INT)
        .addColumn(BALANCE, DataType.INT)
        .addPartitionKey(ACCOUNT_ID)
        .addClusteringKey(ACCOUNT_TYPE)
        .build();
  }

  /** Storage-specific ConsensusCommit properties (typically from a {@code ConsensusCommit*Env}). */
  protected abstract Properties getFacadeStorageProperties(String testName);
}
