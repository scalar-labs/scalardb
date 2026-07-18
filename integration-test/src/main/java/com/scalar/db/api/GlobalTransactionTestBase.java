package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration-test corpus for the {@link GlobalTransactionManager} / {@link GlobalTransaction} /
 * {@link BranchTransaction} API, exercised against a real storage.
 *
 * <p>Every test drives a global transaction spanning <b>two branches</b>: begin the overall
 * transaction on {@link #manager1}, join a branch on {@link #manager1} and another on {@link
 * #manager2} (both keyed by {@link GlobalTransaction#getId()}), run CRUD through each branch on a
 * distinct record, then drive the outcome atomically across both branches with {@link
 * GlobalTransaction#commit()} / {@link GlobalTransaction#rollback()}.
 *
 * <p>{@link #manager1} and {@link #manager2} may be the same instance or two distinct instances,
 * depending on the backing the subclass wires in {@link #setUpManagers()}: a two-phase-commit
 * backing coordinates two participants across two managers, whereas a single-phase (fully-shared)
 * backing serves both branches from one underlying transaction on a single manager. Either way,
 * both branches join the same global transaction and are committed/rolled back as a whole.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class GlobalTransactionTestBase {
  private static final Logger logger = LoggerFactory.getLogger(GlobalTransactionTestBase.class);

  protected static final String NAMESPACE_BASE_NAME = "int_test_";
  protected static final String TABLE = "tbl";
  protected static final String ACCOUNT_ID = "account_id";
  protected static final String ACCOUNT_TYPE = "account_type";
  protected static final String BALANCE = "balance";
  protected static final String SOME_COLUMN = "some_column";
  protected static final int INITIAL_BALANCE = 1000;

  protected DistributedTransactionAdmin admin;
  protected GlobalTransactionManager manager1;
  protected GlobalTransactionManager manager2;
  protected String namespace;

  @BeforeAll
  public void beforeAll() throws Exception {
    String testName = getTestName();
    initialize(testName);
    Properties properties = getProperties(testName);
    TransactionFactory factory = TransactionFactory.create(properties);
    admin = factory.getTransactionAdmin();
    namespace = getNamespaceBaseName() + testName;
    createTables();
    setUpManagers();
  }

  protected void initialize(String testName) throws Exception {}

  protected abstract String getTestName();

  protected abstract Properties getProperties(String testName);

  /**
   * Sets up {@link #manager1} and {@link #manager2}. The two must be able to participate in the
   * same global transaction: a branch begun on {@link #manager2} joins a transaction begun on
   * {@link #manager1}, and both are driven by the same {@link GlobalTransaction#commit()} / {@link
   * GlobalTransaction#rollback()}. They may be the same instance (single-phase backing) or two
   * distinct instances sharing a coordinator (two-phase-commit backing).
   */
  protected abstract void setUpManagers() throws Exception;

  /** Releases the resources set up in {@link #setUpManagers()}. */
  protected abstract void tearDownManagers() throws Exception;

  protected String getNamespaceBaseName() {
    return NAMESPACE_BASE_NAME;
  }

  protected TableMetadata getTableMetadata() {
    return TableMetadata.newBuilder()
        .addColumn(ACCOUNT_ID, DataType.INT)
        .addColumn(ACCOUNT_TYPE, DataType.INT)
        .addColumn(BALANCE, DataType.INT)
        .addColumn(SOME_COLUMN, DataType.INT)
        .addPartitionKey(ACCOUNT_ID)
        .addClusteringKey(ACCOUNT_TYPE)
        .build();
  }

  private void createTables() throws ExecutionException {
    Map<String, String> options = getCreationOptions();
    admin.createCoordinatorTables(true, options);
    admin.createNamespace(namespace, true, options);
    admin.createTable(namespace, TABLE, getTableMetadata(), true, options);
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  @BeforeEach
  public void setUp() throws Exception {
    truncateTable(namespace, TABLE);
    truncateCoordinatorTables();
  }

  protected void truncateTable(String namespace, String table) throws ExecutionException {
    admin.truncateTable(namespace, table);
  }

  protected void truncateCoordinatorTables() throws ExecutionException {
    admin.truncateCoordinatorTables();
  }

  @AfterAll
  public void afterAll() throws Exception {
    try {
      dropTables();
    } catch (Exception e) {
      logger.warn("Failed to drop tables", e);
    }

    try {
      if (admin != null) {
        admin.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close admin", e);
    }

    try {
      tearDownManagers();
    } catch (Exception e) {
      logger.warn("Failed to tear down managers", e);
    }
  }

  private void dropTables() throws ExecutionException {
    admin.dropTable(namespace, TABLE);
    admin.dropNamespace(namespace);
    admin.dropCoordinatorTables();
  }

  @Test
  public void commit_PutsOnTwoBranches_ShouldPersistBothRecords() throws TransactionException {
    // Act: one global transaction with two branches on two managers, each writing a distinct
    // record.
    GlobalTransaction global = manager1.beginGlobal();
    BranchTransaction branch1 = manager1.beginBranch(global.getId());
    BranchTransaction branch2 = manager2.beginBranch(global.getId());
    branch1.put(preparePut(0, 0, 100));
    branch2.put(preparePut(1, 1, 200));
    global.commit();

    // Assert: both branches' writes are committed atomically.
    Optional<Result> result1 = get(0, 0);
    Optional<Result> result2 = get(1, 1);
    assertThat(result1).isPresent();
    assertThat(result1.get().getInt(BALANCE)).isEqualTo(100);
    assertThat(result2).isPresent();
    assertThat(result2.get().getInt(BALANCE)).isEqualTo(200);
  }

  @Test
  public void commit_InsertsOnTwoBranches_ShouldStoreBothRecords() throws TransactionException {
    // Act
    GlobalTransaction global = manager1.beginGlobal();
    BranchTransaction branch1 = manager1.beginBranch(global.getId());
    BranchTransaction branch2 = manager2.beginBranch(global.getId());
    branch1.insert(prepareInsert(0, 0, 100));
    branch2.insert(prepareInsert(1, 1, 200));
    global.commit();

    // Assert
    assertThat(get(0, 0).get().getInt(BALANCE)).isEqualTo(100);
    assertThat(get(1, 1).get().getInt(BALANCE)).isEqualTo(200);
  }

  @Test
  public void commit_ReadModifyWriteOnTwoBranches_ShouldReflectBothUpdates()
      throws TransactionException {
    // Arrange
    putThenCommit(0, 0, INITIAL_BALANCE);
    putThenCommit(1, 1, INITIAL_BALANCE);

    // Act: each branch reads then updates its own record within the one global transaction.
    GlobalTransaction global = manager1.beginGlobal();
    BranchTransaction branch1 = manager1.beginBranch(global.getId());
    BranchTransaction branch2 = manager2.beginBranch(global.getId());
    int balance1 = branch1.get(prepareGet(0, 0)).get().getInt(BALANCE);
    int balance2 = branch2.get(prepareGet(1, 1)).get().getInt(BALANCE);
    branch1.put(preparePut(0, 0, balance1 + 100));
    branch2.put(preparePut(1, 1, balance2 + 200));
    global.commit();

    // Assert
    assertThat(get(0, 0).get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE + 100);
    assertThat(get(1, 1).get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE + 200);
  }

  @Test
  public void commit_DeletesOnTwoBranches_ShouldRemoveBothRecords() throws TransactionException {
    // Arrange
    putThenCommit(0, 0, INITIAL_BALANCE);
    putThenCommit(1, 1, INITIAL_BALANCE);

    // Act
    GlobalTransaction global = manager1.beginGlobal();
    BranchTransaction branch1 = manager1.beginBranch(global.getId());
    BranchTransaction branch2 = manager2.beginBranch(global.getId());
    branch1.delete(prepareDelete(0, 0));
    branch2.delete(prepareDelete(1, 1));
    global.commit();

    // Assert
    assertThat(get(0, 0)).isNotPresent();
    assertThat(get(1, 1)).isNotPresent();
  }

  @Test
  public void commit_OneBranchConflictsWithCommittedTransaction_ShouldRollBackBothBranches()
      throws TransactionException {
    // Arrange
    putThenCommit(0, 0, INITIAL_BALANCE);
    putThenCommit(1, 1, INITIAL_BALANCE);

    // Act: each branch reads then stages an update on its own record; then an independent
    // transaction commits a change to branch1's record after it was read, creating a write-write
    // conflict that must abort the global transaction at commit time.
    GlobalTransaction global = manager1.beginGlobal();
    BranchTransaction branch1 = manager1.beginBranch(global.getId());
    BranchTransaction branch2 = manager2.beginBranch(global.getId());
    int balance1 = branch1.get(prepareGet(0, 0)).get().getInt(BALANCE);
    int balance2 = branch2.get(prepareGet(1, 1)).get().getInt(BALANCE);
    branch1.put(preparePut(0, 0, balance1 + 100));
    branch2.put(preparePut(1, 1, balance2 + 200));

    GlobalTransaction interfering = manager1.beginGlobal();
    BranchTransaction interferingBranch = manager1.beginBranch(interfering.getId());
    int interferingBalance = interferingBranch.get(prepareGet(0, 0)).get().getInt(BALANCE);
    interferingBranch.put(preparePut(0, 0, interferingBalance + 1));
    interfering.commit();

    // Assert: the commit fails as a conflict, and neither branch's staged write is durable — the
    // conflicting branch's record reflects only the interfering transaction, and the other
    // branch's staged write must be rolled back too (no partial commit).
    assertThatThrownBy(global::commit).isInstanceOf(CommitConflictException.class);
    assertThat(get(0, 0).get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE + 1);
    assertThat(get(1, 1).get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
  }

  @Test
  public void rollback_PutsOnTwoBranches_ShouldNotPersistEitherRecord()
      throws TransactionException {
    // Act
    GlobalTransaction global = manager1.beginGlobal();
    BranchTransaction branch1 = manager1.beginBranch(global.getId());
    BranchTransaction branch2 = manager2.beginBranch(global.getId());
    branch1.put(preparePut(0, 0, 100));
    branch2.put(preparePut(1, 1, 200));
    global.rollback();

    // Assert: neither branch's write persists.
    assertThat(get(0, 0)).isNotPresent();
    assertThat(get(1, 1)).isNotPresent();
  }

  @Test
  public void abort_PutsOnTwoBranches_ShouldNotPersistEitherRecord() throws TransactionException {
    // Act
    GlobalTransaction global = manager1.beginGlobal();
    BranchTransaction branch1 = manager1.beginBranch(global.getId());
    BranchTransaction branch2 = manager2.beginBranch(global.getId());
    branch1.put(preparePut(0, 0, 100));
    branch2.put(preparePut(1, 1, 200));
    global.abort();

    // Assert
    assertThat(get(0, 0)).isNotPresent();
    assertThat(get(1, 1)).isNotPresent();
  }

  @Test
  public void beginGlobalReadOnly_GetsOnTwoBranches_ShouldReturnBothRecords()
      throws TransactionException {
    // Arrange
    putThenCommit(0, 0, 100);
    putThenCommit(1, 1, 200);

    // Act
    GlobalTransaction global = manager1.beginGlobalReadOnly();
    BranchTransaction branch1 = manager1.beginBranch(global.getId());
    BranchTransaction branch2 = manager2.beginBranch(global.getId());
    Optional<Result> result1 = branch1.get(prepareGet(0, 0));
    Optional<Result> result2 = branch2.get(prepareGet(1, 1));
    global.commit();

    // Assert
    assertThat(result1).isPresent();
    assertThat(result1.get().getInt(BALANCE)).isEqualTo(100);
    assertThat(result2).isPresent();
    assertThat(result2.get().getInt(BALANCE)).isEqualTo(200);
  }

  @Test
  public void beginBranch_ForTwoBranches_ShouldReturnBranchesKeyedByGlobalTransactionId()
      throws TransactionException {
    // Act
    GlobalTransaction global = manager1.beginGlobal();
    BranchTransaction branch1 = manager1.beginBranch(global.getId());
    BranchTransaction branch2 = manager2.beginBranch(global.getId());

    // Assert: every branch is keyed by the global transaction ID.
    assertThat(branch1.getId()).isEqualTo(global.getId());
    assertThat(branch2.getId()).isEqualTo(global.getId());
    global.rollback();
  }

  private void putThenCommit(int id, int type, int balance) throws TransactionException {
    GlobalTransaction global = manager1.beginGlobal();
    BranchTransaction branch = manager1.beginBranch(global.getId());
    branch.put(preparePut(id, type, balance));
    global.commit();
  }

  private Optional<Result> get(int id, int type) throws TransactionException {
    GlobalTransaction global = manager1.beginGlobal();
    BranchTransaction branch = manager1.beginBranch(global.getId());
    Optional<Result> result = branch.get(prepareGet(id, type));
    global.commit();
    return result;
  }

  private Put preparePut(int id, int type, int balance) {
    return Put.newBuilder()
        .namespace(namespace)
        .table(TABLE)
        .partitionKey(Key.ofInt(ACCOUNT_ID, id))
        .clusteringKey(Key.ofInt(ACCOUNT_TYPE, type))
        .intValue(BALANCE, balance)
        .build();
  }

  private Insert prepareInsert(int id, int type, int balance) {
    return Insert.newBuilder()
        .namespace(namespace)
        .table(TABLE)
        .partitionKey(Key.ofInt(ACCOUNT_ID, id))
        .clusteringKey(Key.ofInt(ACCOUNT_TYPE, type))
        .intValue(BALANCE, balance)
        .build();
  }

  private Delete prepareDelete(int id, int type) {
    return Delete.newBuilder()
        .namespace(namespace)
        .table(TABLE)
        .partitionKey(Key.ofInt(ACCOUNT_ID, id))
        .clusteringKey(Key.ofInt(ACCOUNT_TYPE, type))
        .build();
  }

  private Get prepareGet(int id, int type) {
    return Get.newBuilder()
        .namespace(namespace)
        .table(TABLE)
        .partitionKey(Key.ofInt(ACCOUNT_ID, id))
        .clusteringKey(Key.ofInt(ACCOUNT_TYPE, type))
        .build();
  }
}
