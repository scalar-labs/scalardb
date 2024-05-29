package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.api.TwoPhaseCommitTransactionCrossPartitionScanIntegrationTestBase;
import com.scalar.db.exception.transaction.TransactionException;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public abstract class TwoPhaseConsensusCommitCrossPartitionScanIntegrationTestBase
    extends TwoPhaseCommitTransactionCrossPartitionScanIntegrationTestBase {

  @Override
  protected String getTestName() {
    return "2pcc_cross_scan";
  }

  @Override
  protected final Properties getProperties1(String testName) {
    Properties properties = new Properties();
    properties.putAll(getProps1(testName));

    // Add testName as a coordinator namespace suffix
    ConsensusCommitIntegrationTestUtils.addSuffixToCoordinatorNamespace(properties, testName);

    return properties;
  }

  @Override
  protected final Properties getProperties2(String testName) {
    Properties properties = new Properties();
    properties.putAll(getProps2(testName));

    // Add testName as a coordinator namespace suffix
    ConsensusCommitIntegrationTestUtils.addSuffixToCoordinatorNamespace(properties, testName);

    return properties;
  }

  protected abstract Properties getProps1(String testName);

  protected Properties getProps2(String testName) {
    return getProps1(testName);
  }

  @Test
  public void scan_PutAndOverlappedCrossPartitionScanGiven_ShouldThrowException()
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
  public void scan_PutResultNonOverlappedWithCrossPartitionScanGiven_ShouldThrowException()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.start();
    Put put = preparePut(namespace1, TABLE_1, 10, NUM_TYPES);
    Scan scan = prepareCrossPartitionScan(namespace1, TABLE_1, 1, 0, NUM_TYPES - 1);

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
  public void scan_PutResultOverlappedWithCrossPartitionScanGiven_ShouldThrowException()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    TwoPhaseCommitTransaction transaction = manager1.start();
    Put put = preparePut(namespace1, TABLE_1, 10, NUM_TYPES);
    Scan scan = prepareCrossPartitionScan(namespace1, TABLE_1, 1, NUM_TYPES, NUM_TYPES);

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
  public void scan_PutNonOverlappedWithCrossPartitionScanWithLikeGiven_ShouldNotThrowAnyException()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    populateRecordsForLike(manager2, namespace2, TABLE_2);
    Scan scan1 = Scan.newBuilder(prepareCrossPartitionScan(namespace1, TABLE_1, 1, 0, 2)).build();
    Scan scan2 = prepareCrossPartitionScanWithLike(namespace2, TABLE_2, true, "\\_scalar[$]", "");
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
  public void scan_PutResultOverlappedWithCrossPartitionScanWithLikeGiven_ShouldThrowException()
      throws TransactionException {
    // Arrange
    populateRecords(manager1, namespace1, TABLE_1);
    populateRecordsForLike(manager2, namespace2, TABLE_2);
    Scan scan1 = Scan.newBuilder(prepareCrossPartitionScan(namespace1, TABLE_1, 1, 0, 2)).build();
    Scan scan2 = prepareCrossPartitionScanWithLike(namespace2, TABLE_2, true, "\\scalar[$]", "");
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
}
