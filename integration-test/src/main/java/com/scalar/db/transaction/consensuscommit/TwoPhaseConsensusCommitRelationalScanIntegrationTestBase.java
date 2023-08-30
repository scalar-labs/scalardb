package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanBuilder.BuildableScanOrScanAllFromExisting;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.api.TwoPhaseCommitTransactionRelationalScanIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public abstract class TwoPhaseConsensusCommitRelationalScanIntegrationTestBase
    extends TwoPhaseCommitTransactionRelationalScanIntegrationTestBase {
  private TwoPhaseCommitTransactionManager managerWithWithIncludeMetadataEnabled;

  @BeforeAll
  @Override
  public void beforeAll() throws Exception {
    super.beforeAll();

    Properties includeMetadataEnabledProperties = getPropsWithIncludeMetadataEnabled(getTestName());
    managerWithWithIncludeMetadataEnabled =
        TransactionFactory.create(includeMetadataEnabledProperties)
            .getTwoPhaseCommitTransactionManager();
  }

  @AfterAll
  @Override
  public void afterAll() throws Exception {
    super.afterAll();

    managerWithWithIncludeMetadataEnabled.close();
  }

  @Override
  protected String getTestName() {
    return "2pc_cc";
  }

  @Override
  protected final Properties getProperties1(String testName) {
    Properties properties = new Properties();
    properties.putAll(getProps1(testName));
    if (!properties.containsKey(DatabaseConfig.TRANSACTION_MANAGER)) {
      modifyProperties(properties, testName);
    }
    return properties;
  }

  @Override
  protected final Properties getProperties2(String testName) {
    Properties properties = new Properties();
    properties.putAll(getProps2(testName));
    if (!properties.containsKey(DatabaseConfig.TRANSACTION_MANAGER)) {
      modifyProperties(properties, testName);
    }
    return properties;
  }

  private void modifyProperties(Properties properties, String testName) {
    properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "consensus-commit");

    // Add testName as a coordinator namespace suffix
    String coordinatorNamespace =
        properties.getProperty(ConsensusCommitConfig.COORDINATOR_NAMESPACE, Coordinator.NAMESPACE);
    properties.setProperty(
        ConsensusCommitConfig.COORDINATOR_NAMESPACE, coordinatorNamespace + "_" + testName);
  }

  protected abstract Properties getProps1(String testName);

  protected Properties getProps2(String testName) {
    return getProps1(testName);
  }

  protected Properties getPropsWithIncludeMetadataEnabled(String testName) {
    Properties properties = getProperties1(testName);
    properties.setProperty(ConsensusCommitConfig.INCLUDE_METADATA_ENABLED, "true");
    return properties;
  }

  @Test
  public void scan_PutAndOverlappedRelationalScanGiven_ShouldThrowException()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.start();
    Put put = preparePut(namespace1, TABLE_1, 10, NUM_TYPES + 1);
    Scan scan =
        Scan.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .all()
            .where(ConditionBuilder.column(ACCOUNT_ID).isLessThanOrEqualToInt(10))
            .consistency(Consistency.LINEARIZABLE)
            .build();

    // Act
    Throwable thrown =
        catchThrowable(
            () -> {
              transaction.put(put);
              transaction.scan(scan);
              transaction.commit();
            });

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void scan_PutResultNonOverlappedWithRelationalScanGiven_ShouldThrowException()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.start();
    Put put = preparePut(namespace1, TABLE_1, 10, NUM_TYPES);
    Scan scan = prepareRelationalScan(namespace1, TABLE_1, 1, 0, NUM_TYPES - 1);

    // Act
    Throwable thrown =
        catchThrowable(
            () -> {
              transaction.put(put);
              transaction.scan(scan);
              transaction.commit();
            });

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void scan_PutResultOverlappedWithRelationalScanGiven_ShouldThrowException()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.start();
    Put put = preparePut(namespace1, TABLE_1, 10, NUM_TYPES);
    Scan scan = prepareRelationalScan(namespace1, TABLE_1, 1, NUM_TYPES, NUM_TYPES);

    // Act
    Throwable thrown =
        catchThrowable(
            () -> {
              transaction.put(put);
              transaction.scan(scan);
              transaction.commit();
            });

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void scan_PutNonOverlappedWithRelationalScanWithLikeGiven_ShouldNotThrowAnyException()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    populateRecordsForLike(manager2, namespace2, TABLE_2);
    Scan scan1 = Scan.newBuilder(prepareRelationalScan(namespace1, TABLE_1, 1, 0, 2)).build();
    Scan scan2 = prepareRelationalScanWithLike(namespace2, TABLE_2, true, "\\_scalar[$]", "");
    Put put = preparePut(namespace2, TABLE_2, 999, "\\scalar[$]");

    TwoPhaseCommitTransaction transaction1 = manager1.start();
    TwoPhaseCommitTransaction transaction2 = manager2.join(transaction1.getId());

    // Act Assert
    assertDoesNotThrow(
        () -> {
          // Non-overlapped scan-after-write
          transaction1.scan(scan1);
          transaction2.put(put);
          transaction2.scan(scan2);

          // Prepare
          transaction1.prepare();
          transaction2.prepare();

          // Validate
          transaction1.validate();
          transaction2.validate();

          // Commit
          transaction1.commit();
          transaction2.commit();
        });
  }

  @Test
  public void scan_PutResultOverlappedWithRelationalScanWithLikeGiven_ShouldThrowException()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    populateRecordsForLike(manager2, namespace2, TABLE_2);
    Scan scan1 = Scan.newBuilder(prepareRelationalScan(namespace1, TABLE_1, 1, 0, 2)).build();
    Scan scan2 = prepareRelationalScanWithLike(namespace2, TABLE_2, true, "\\scalar[$]", "");
    Put put = preparePut(namespace2, TABLE_2, 999, "\\scalar[$]");

    TwoPhaseCommitTransaction transaction1 = manager1.start();
    TwoPhaseCommitTransaction transaction2 = manager2.join(transaction1.getId());

    transaction1.scan(scan1);
    transaction2.put(put);

    // Act
    Throwable thrown =
        catchThrowable(
            () -> {
              // Overlapped scan-after-write
              transaction2.scan(scan2);
            });

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void scan_WithIncludeMetadataEnabled_ShouldReturnTransactionMetadataColumns()
      throws TransactionException {
    scan_WithIncludeMetadataEnabled_ShouldReturnCorrectColumns(false);
  }

  @Test
  public void scan_WithIncludeMetadataEnabledAndProjections_ShouldReturnProjectedColumns()
      throws TransactionException {
    scan_WithIncludeMetadataEnabled_ShouldReturnCorrectColumns(true);
  }

  private void scan_WithIncludeMetadataEnabled_ShouldReturnCorrectColumns(boolean hasProjections)
      throws TransactionException {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace(namespace1)
            .table(TABLE_1)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .intValue(ACCOUNT_TYPE, 0)
            .intValue(BALANCE, INITIAL_BALANCE)
            .build();
    TwoPhaseCommitTransaction transaction = managerWithWithIncludeMetadataEnabled.start();
    transaction.put(put);
    transaction.prepare();
    transaction.validate();
    transaction.commit();
    transaction = managerWithWithIncludeMetadataEnabled.start();
    Set<String> projections =
        ImmutableSet.of(ACCOUNT_ID, Attribute.BEFORE_PREFIX + BALANCE, Attribute.STATE);

    // Act Assert
    BuildableScanOrScanAllFromExisting scanBuilder =
        Scan.newBuilder(prepareRelationalScan(namespace1, TABLE_1, 0, 0, 1));
    if (hasProjections) {
      scanBuilder.projections(projections);
    }
    List<Result> results = transaction.scan(scanBuilder.build());
    assertThat(results.size()).isOne();
    Result result = results.get(0);
    transaction.prepare();
    transaction.validate();
    transaction.commit();

    // Assert the actual result
    TableMetadata transactionTableMetadata =
        ConsensusCommitUtils.buildTransactionTableMetadata(TABLE_1_METADATA);
    if (hasProjections) {
      assertThat(result.getContainedColumnNames()).isEqualTo(projections);
    } else {
      assertThat(result.getContainedColumnNames().size())
          .isEqualTo(transactionTableMetadata.getColumnNames().size());
    }
    for (Column<?> column : result.getColumns().values()) {
      assertThat(column.getName()).isIn(transactionTableMetadata.getColumnNames());
      assertThat(column.getDataType())
          .isEqualTo(transactionTableMetadata.getColumnDataType(column.getName()));
    }
  }
}
