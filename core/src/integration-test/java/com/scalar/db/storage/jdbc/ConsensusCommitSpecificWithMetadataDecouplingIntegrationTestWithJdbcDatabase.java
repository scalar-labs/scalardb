package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.service.TransactionFactory;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitSpecificWithMetadataDecouplingIntegrationTestBase;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitTestUtils;
import com.scalar.db.transaction.consensuscommit.CoordinatorException;
import com.scalar.db.transaction.consensuscommit.Isolation;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ConsensusCommitSpecificWithMetadataDecouplingIntegrationTestWithJdbcDatabase
    extends ConsensusCommitSpecificWithMetadataDecouplingIntegrationTestBase {

  private RdbEngineStrategy rdbEngine;
  private DistributedTransactionManager truncationManager;
  private DistributedTransactionAdmin truncationAdmin;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = ConsensusCommitJdbcEnv.getProperties(testName);

    // Set the isolation level for consistency reads for virtual tables
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
    properties.setProperty(
        JdbcConfig.ISOLATION_LEVEL,
        JdbcTestUtils.getIsolationLevel(
                rdbEngine.getMinimumIsolationLevelForConsistentVirtualTableRead())
            .name());

    if (JdbcTestUtils.isYugabyte(rdbEngine)) {
      Properties managerProps = new Properties(properties);
      ConsensusCommitTestUtils.addSuffixToCoordinatorNamespace(managerProps, testName);
      TransactionFactory factory = TransactionFactory.create(managerProps);
      truncationManager = factory.getTransactionManager();
      truncationAdmin = factory.getTransactionAdmin();
    }

    return properties;
  }

  @Override
  protected void truncateTable(String namespace, String table) throws ExecutionException {
    if (JdbcTestUtils.isYugabyte(rdbEngine)) {
      JdbcTestUtils.deleteAllRows(truncationManager, truncationAdmin, namespace, table);
      return;
    }
    super.truncateTable(namespace, table);
  }

  @AfterAll
  @Override
  protected void afterAll() throws Exception {
    try {
      super.afterAll();
    } finally {
      if (truncationAdmin != null) {
        truncationAdmin.close();
      }
      if (truncationManager != null) {
        truncationManager.close();
      }
    }
  }

  @Override
  @ParameterizedTest
  @MethodSource("provideIsolation")
  public void
      getScanner_ScanAllWithIndexConditionForDeletedWhenCoordinatorStateCommitted_ShouldRollForwardAndReturnCommittedRecords(
          com.scalar.db.transaction.consensuscommit.Isolation isolation)
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
          com.scalar.db.transaction.consensuscommit.Isolation isolation)
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
          com.scalar.db.transaction.consensuscommit.Isolation isolation)
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
          com.scalar.db.transaction.consensuscommit.Isolation isolation)
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
          com.scalar.db.transaction.consensuscommit.Isolation isolation)
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
          com.scalar.db.transaction.consensuscommit.Isolation isolation)
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
    return Arrays.stream(com.scalar.db.transaction.consensuscommit.Isolation.values())
        .filter(i -> !(JdbcEnv.isSpannerEmulator() && i == Isolation.SERIALIZABLE))
        .map(Arguments::of);
  }
}
