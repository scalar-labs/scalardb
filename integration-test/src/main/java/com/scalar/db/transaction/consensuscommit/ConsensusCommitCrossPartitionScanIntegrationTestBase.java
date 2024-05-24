package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionCrossPartitionScanIntegrationTestBase;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.exception.transaction.TransactionException;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public abstract class ConsensusCommitCrossPartitionScanIntegrationTestBase
    extends DistributedTransactionCrossPartitionScanIntegrationTestBase {

  @Override
  protected String getTestName() {
    return "tx_cross_scan";
  }

  @Override
  protected final Properties getProperties(String testName) {
    Properties properties = new Properties();
    properties.putAll(getProps(testName));

    // Add testName as a coordinator namespace suffix
    ConsensusCommitIntegrationTestUtils.addSuffixToCoordinatorNamespace(properties, testName);

    return properties;
  }

  protected abstract Properties getProps(String testName);

  @Test
  public void scan_PutAndOverlappedCrossPartitionScanGiven_ShouldThrowException()
      throws TransactionException {
    // Arrange
    populateRecords();
    DistributedTransaction transaction = manager.start();
    Put put = preparePut(10, NUM_TYPES + 1);
    Scan scan =
        Scan.newBuilder()
            .namespace(namespace)
            .table(TABLE)
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
    populateRecords();
    DistributedTransaction transaction = manager.start();
    Put put = preparePut(10, NUM_TYPES);
    Scan scan = prepareCrossPartitionScan(1, 0, NUM_TYPES - 1);

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
    populateRecords();
    DistributedTransaction transaction = manager.start();
    Put put = preparePut(10, NUM_TYPES);
    Scan scan = prepareCrossPartitionScan(1, NUM_TYPES, NUM_TYPES);

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
    populateRecordsForLike();
    DistributedTransaction transaction = manager.start();
    Put put = preparePut(999, "\\scalar[$]");
    Scan scan = prepareCrossPartitionScanWithLike(true, "\\_scalar[$]", "");

    // Act Assert
    assertDoesNotThrow(
        () -> {
          transaction.put(put);
          transaction.scan(scan);
          transaction.commit();
        });
  }

  @Test
  public void scan_PutResultOverlappedWithCrossPartitionScanWithLikeGiven_ShouldThrowException()
      throws TransactionException {
    // Arrange
    populateRecordsForLike();
    DistributedTransaction transaction = manager.start();
    Put put = preparePut(999, "\\scalar[$]");
    Scan scan = prepareCrossPartitionScanWithLike(true, "\\%scalar[$]", "");

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
}
