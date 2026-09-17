package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitSpecificIntegrationTestBase;
import com.scalar.db.transaction.consensuscommit.CoordinatorException;
import com.scalar.db.transaction.consensuscommit.Isolation;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ConsensusCommitSpecificIntegrationTestWithJdbcDatabase
    extends ConsensusCommitSpecificIntegrationTestBase {

  private JdbcAdminTestUtils jdbcAdminTestUtils;
  private RdbEngineStrategy rdbEngine;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = ConsensusCommitJdbcEnv.getProperties(testName);
    rdbEngine = RdbEngineFactory.create(new JdbcConfig(new DatabaseConfig(properties)));
    if (JdbcEnv.isYugabyte() && jdbcAdminTestUtils == null) {
      jdbcAdminTestUtils = new JdbcAdminTestUtils(properties);
    }
    return properties;
  }

  @Override
  protected boolean isConcurrentWriteToRowUnderOpenScanSupported() {
    // SQL Server and Db2 use lock-based concurrency even at their default isolation (READ
    // COMMITTED), so an open scan keeps a shared lock that blocks the scan-path recovery
    // simulation's concurrent write to a scanned row, and their lock wait is effectively unbounded,
    // so it hangs. MySQL and MariaDB are MVCC at this class's (default) isolation and do not block;
    // they only block under SERIALIZABLE (see the highest-isolation subclass).
    return !(JdbcTestUtils.isSqlServer(rdbEngine) || JdbcTestUtils.isDb2(rdbEngine));
  }

  @AfterAll
  void closeJdbcAdminTestUtils() throws Exception {
    if (jdbcAdminTestUtils != null) {
      jdbcAdminTestUtils.close();
    }
  }

  @Override
  protected void truncateTable(String namespace, String table) throws ExecutionException {
    // Use DML DELETE for YugabyteDB: TRUNCATE is DDL that conflicts with table locking.
    // This only affects @BeforeEach cleanup. The actual truncateTable() API is tested in admin ITs.
    if (JdbcEnv.isYugabyte()) {
      jdbcAdminTestUtils.deleteAllRowsWithSql(namespace, table);
      return;
    }
    super.truncateTable(namespace, table);
  }

  @Override
  protected void truncateCoordinatorTables() throws ExecutionException {
    if (JdbcEnv.isYugabyte()) {
      jdbcAdminTestUtils.deleteAllRowsFromCoordinatorTableWithSql();
      return;
    }
    super.truncateCoordinatorTables();
  }

  @Override
  @ParameterizedTest
  @MethodSource("provideIsolation")
  public void
      getScanner_ScanAllWithIndexConditionForDeletedWhenCoordinatorStateCommitted_ShouldRollForwardAndReturnCommittedRecords(
          Isolation isolation)
          throws TransactionException, CoordinatorException, ExecutionException {
    super
        .getScanner_ScanAllWithIndexConditionForDeletedWhenCoordinatorStateCommitted_ShouldRollForwardAndReturnCommittedRecords(
            isolation);
  }

  @Override
  @ParameterizedTest
  @MethodSource("provideIsolation")
  public void
      getScanner_ScanWithIndexForDeletedWhenCoordinatorStateCommitted_ShouldRollForwardAndReturnCommittedRecords(
          Isolation isolation)
          throws ExecutionException, CoordinatorException, TransactionException {
    super
        .getScanner_ScanWithIndexForDeletedWhenCoordinatorStateCommitted_ShouldRollForwardAndReturnCommittedRecords(
            isolation);
  }

  @Override
  @ParameterizedTest
  @MethodSource("provideIsolation")
  public void
      getScanner_ScanWithIndexForPreparedWhenCoordinatorStateAbortedAndIndexKeyMatchesAfterImage_ShouldRollBackAndFilterOutResult(
          Isolation isolation)
          throws ExecutionException, CoordinatorException, TransactionException {
    super
        .getScanner_ScanWithIndexForPreparedWhenCoordinatorStateAbortedAndIndexKeyMatchesAfterImage_ShouldRollBackAndFilterOutResult(
            isolation);
  }

  @Override
  @ParameterizedTest
  @MethodSource("provideIsolation")
  public void
      scan_ScanAllWithIndexConditionForDeletedWhenCoordinatorStateCommitted_ShouldRollForwardAndReturnCommittedRecords(
          Isolation isolation)
          throws ExecutionException, CoordinatorException, TransactionException {
    super
        .scan_ScanAllWithIndexConditionForDeletedWhenCoordinatorStateCommitted_ShouldRollForwardAndReturnCommittedRecords(
            isolation);
  }

  @Override
  @ParameterizedTest
  @MethodSource("provideIsolation")
  public void
      scan_ScanWithIndexForDeletedWhenCoordinatorStateCommitted_ShouldRollForwardAndReturnCommittedRecords(
          Isolation isolation)
          throws ExecutionException, CoordinatorException, TransactionException {
    super
        .scan_ScanWithIndexForDeletedWhenCoordinatorStateCommitted_ShouldRollForwardAndReturnCommittedRecords(
            isolation);
  }

  @Override
  @ParameterizedTest
  @MethodSource("provideIsolation")
  public void
      scan_ScanWithIndexForPreparedWhenCoordinatorStateAbortedAndIndexKeyMatchesAfterImage_ShouldRollBackAndFilterOutResult(
          Isolation isolation)
          throws ExecutionException, CoordinatorException, TransactionException {
    super
        .scan_ScanWithIndexForPreparedWhenCoordinatorStateAbortedAndIndexKeyMatchesAfterImage_ShouldRollBackAndFilterOutResult(
            isolation);
  }

  protected Stream<Arguments> provideIsolation() {
    // Skip SERIALIZABLE for the Spanner emulator
    // The Spanner Emulator randomizes query results without ORDER BY (cross-partition scan or
    // scan-by-index), which breaks the order-dependent serializability verification in
    // Snapshot.validateScanResults() and causes the test to fail. Real Spanner preserves ordering,
    // so we only need to skip SERIALIZABLE on the emulator.
    return Arrays.stream(Isolation.values())
        .filter(i -> !(JdbcEnv.isSpannerEmulator() && i == Isolation.SERIALIZABLE))
        .map(Arguments::of);
  }
}
