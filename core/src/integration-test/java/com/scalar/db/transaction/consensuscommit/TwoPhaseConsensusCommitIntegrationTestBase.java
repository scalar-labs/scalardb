package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.Get;
import com.scalar.db.api.GetBuilder.BuildableGet;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanBuilder.BuildableScanOrScanAllFromExisting;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.api.TwoPhaseCommitTransactionIntegrationTestBase;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;
import com.scalar.db.util.TestUtils;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public abstract class TwoPhaseConsensusCommitIntegrationTestBase
    extends TwoPhaseCommitTransactionIntegrationTestBase {
  private TwoPhaseCommitTransactionManager managerWithDebug;

  @BeforeAll
  @Override
  public void beforeAll() throws Exception {
    super.beforeAll();

    Properties debugProperties = TestUtils.addSuffix(getProperties(), getTestName());
    debugProperties.setProperty(ConsensusCommitConfig.DEBUG, "true");
    managerWithDebug =
        TransactionFactory.create(debugProperties).getTwoPhaseCommitTransactionManager();
  }

  @AfterAll
  @Override
  public void afterAll() throws ExecutionException {
    super.afterAll();

    managerWithDebug.close();
  }

  @Override
  protected String getTestName() {
    return "2pc_cc";
  }

  @Override
  protected final Properties getProperties() {
    Properties properties = new Properties();
    properties.putAll(getProps());
    properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "consensus-commit");
    return properties;
  }

  protected abstract Properties getProps();

  @Test
  public void scan_UsingDebugMode_ShouldReturnTransactionMetadataColumns()
      throws TransactionException {
    selection_UsingDebugMode_ShouldReturnCorrectColumns(true, false);
  }

  @Test
  public void scan_UsingDebugModeWithProjections_ShouldReturnProjectedColumns()
      throws TransactionException {
    selection_UsingDebugMode_ShouldReturnCorrectColumns(true, true);
  }

  @Test
  public void get_UsingDebugMode_ShouldReturnTransactionMetadataColumns()
      throws TransactionException {
    selection_UsingDebugMode_ShouldReturnCorrectColumns(false, false);
  }

  @Test
  public void get_UsingDebugModeWithProjections_ShouldReturnProjectedColumns()
      throws TransactionException {
    selection_UsingDebugMode_ShouldReturnCorrectColumns(false, true);
  }

  private void selection_UsingDebugMode_ShouldReturnCorrectColumns(
      boolean isScan, boolean hasProjections) throws TransactionException {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(Key.ofInt(ACCOUNT_ID, 0))
            .clusteringKey(Key.ofInt(ACCOUNT_TYPE, 0))
            .intValue(BALANCE, INITIAL_BALANCE)
            .build();
    TwoPhaseCommitTransaction transaction = managerWithDebug.start();
    transaction.put(put);
    transaction.prepare();
    transaction.validate();
    transaction.commit();
    transaction = managerWithDebug.start();
    Set<String> projections =
        ImmutableSet.of(ACCOUNT_ID, Attribute.BEFORE_PREFIX + BALANCE, Attribute.STATE);

    // Act Assert
    Result result;
    if (isScan) {
      // Perform a Scan
      BuildableScanOrScanAllFromExisting scanBuilder = Scan.newBuilder(prepareScan(0, 0, 1));
      if (hasProjections) {
        scanBuilder.projections(projections);
      }
      List<Result> results = transaction.scan(scanBuilder.build());
      assertThat(results.size()).isOne();
      result = results.get(0);
    } else {
      // Perform a Get
      BuildableGet getBuilder = Get.newBuilder(prepareGet(0, 0));
      if (hasProjections) {
        getBuilder.projections(projections);
      }
      Optional<Result> optionalResult = transaction.get(getBuilder.build());
      assertThat(optionalResult).isPresent();
      result = optionalResult.get();
    }
    transaction.prepare();
    transaction.validate();
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
