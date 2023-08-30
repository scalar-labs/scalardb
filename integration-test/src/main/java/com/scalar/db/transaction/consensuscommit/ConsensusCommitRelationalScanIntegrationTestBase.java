package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.DistributedTransactionRelationalScanIntegrationTestBase;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanBuilder.BuildableScanOrScanAllFromExisting;
import com.scalar.db.api.TableMetadata;
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

public abstract class ConsensusCommitRelationalScanIntegrationTestBase
    extends DistributedTransactionRelationalScanIntegrationTestBase {
  private DistributedTransactionManager managerWithIncludeMetadataEnabled;

  @BeforeAll
  @Override
  public void beforeAll() throws Exception {
    super.beforeAll();

    Properties includeMetadataEnabledProperties = getPropsWithIncludeMetadataEnabled(getTestName());
    managerWithIncludeMetadataEnabled =
        TransactionFactory.create(includeMetadataEnabledProperties).getTransactionManager();
  }

  @AfterAll
  @Override
  public void afterAll() throws Exception {
    super.afterAll();

    managerWithIncludeMetadataEnabled.close();
  }

  @Override
  protected String getTestName() {
    return "tx_cc";
  }

  @Override
  protected final Properties getProperties(String testName) {
    Properties properties = new Properties();
    properties.putAll(getProps(testName));
    if (!properties.containsKey(DatabaseConfig.TRANSACTION_MANAGER)) {
      properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "consensus-commit");

      // Add testName as a coordinator namespace suffix
      String coordinatorNamespace =
          properties.getProperty(
              ConsensusCommitConfig.COORDINATOR_NAMESPACE, Coordinator.NAMESPACE);
      properties.setProperty(
          ConsensusCommitConfig.COORDINATOR_NAMESPACE, coordinatorNamespace + "_" + testName);
    }
    return properties;
  }

  protected abstract Properties getProps(String testName);

  protected Properties getPropsWithIncludeMetadataEnabled(String testName) {
    Properties properties = getProperties(testName);
    properties.setProperty(ConsensusCommitConfig.INCLUDE_METADATA_ENABLED, "true");
    return properties;
  }

  @Test
  public void scan_PutAndOverlappedRelationalScanGiven_ShouldThrowException()
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
  public void scan_PutResultNonOverlappedWithRelationalScanGiven_ShouldThrowException()
      throws TransactionException {
    // Arrange
    populateRecords();
    DistributedTransaction transaction = manager.start();
    Put put = preparePut(10, NUM_TYPES);
    Scan scan = prepareRelationalScan(1, 0, NUM_TYPES - 1);

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
    populateRecords();
    DistributedTransaction transaction = manager.start();
    Put put = preparePut(10, NUM_TYPES);
    Scan scan = prepareRelationalScan(1, NUM_TYPES, NUM_TYPES);

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
    populateRecordsForLike();
    DistributedTransaction transaction = manager.start();
    Put put = preparePut(999, "\\scalar[$]");
    Scan scan = prepareRelationalScanWithLike(true, "\\_scalar[$]", "");

    // Act Assert
    assertDoesNotThrow(
        () -> {
          transaction.put(put);
          transaction.scan(scan);
          transaction.commit();
        });
  }

  @Test
  public void scan_PutResultOverlappedWithRelationalScanWithLikeGiven_ShouldThrowException()
      throws TransactionException {
    // Arrange
    populateRecordsForLike();
    DistributedTransaction transaction = manager.start();
    Put put = preparePut(999, "\\scalar[$]");
    Scan scan = prepareRelationalScanWithLike(true, "\\%scalar[$]", "");

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
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .intValue(ACCOUNT_TYPE, 0)
            .intValue(BALANCE, INITIAL_BALANCE)
            .build();
    DistributedTransaction transaction = managerWithIncludeMetadataEnabled.start();
    transaction.put(put);
    transaction.commit();
    transaction = managerWithIncludeMetadataEnabled.start();
    Set<String> projections =
        ImmutableSet.of(ACCOUNT_ID, Attribute.BEFORE_PREFIX + BALANCE, Attribute.STATE);

    // Act Assert
    BuildableScanOrScanAllFromExisting scanBuilder =
        Scan.newBuilder(prepareRelationalScan(0, 0, 1));
    if (hasProjections) {
      scanBuilder.projections(projections);
    }
    List<Result> results = transaction.scan(scanBuilder.build());
    assertThat(results.size()).isOne();
    Result result = results.get(0);
    transaction.commit();

    // Assert the actual result
    TableMetadata transactionTableMetadata =
        ConsensusCommitUtils.buildTransactionTableMetadata(TABLE_METADATA);
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
