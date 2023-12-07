package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.Get;
import com.scalar.db.api.GetBuilder;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanBuilder;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TwoPhaseConsensusCommitWithIncludeMetadataEnabledIntegrationTestBase {

  protected static final String TEST_NAME = "2pcc_inc_meta";
  protected static final String NAMESPACE = "int_test_" + TEST_NAME;
  protected static final String TABLE = "test_table";
  protected static final String ACCOUNT_ID = "account_id";
  protected static final String ACCOUNT_TYPE = "account_type";
  protected static final String BALANCE = "balance";
  protected static final int INITIAL_BALANCE = 1000;
  protected static final TableMetadata TABLE_METADATA =
      TableMetadata.newBuilder()
          .addColumn(ACCOUNT_ID, DataType.INT)
          .addColumn(ACCOUNT_TYPE, DataType.INT)
          .addColumn(BALANCE, DataType.INT)
          .addPartitionKey(ACCOUNT_ID)
          .addClusteringKey(ACCOUNT_TYPE)
          .build();
  protected DistributedTransactionAdmin admin;
  protected TwoPhaseCommitTransactionManager manager;
  protected String namespace;

  @BeforeAll
  public void beforeAll() throws Exception {
    initialize(TEST_NAME);
    Properties properties = getProperties(TEST_NAME);

    // Add testName as a coordinator namespace suffix
    ConsensusCommitIntegrationTestUtils.addSuffixToCoordinatorNamespace(properties, TEST_NAME);

    // Enable to include metadata
    properties.setProperty(ConsensusCommitConfig.INCLUDE_METADATA_ENABLED, "true");

    TransactionFactory factory = TransactionFactory.create(properties);
    admin = factory.getTransactionAdmin();
    namespace = getNamespace();
    createTables();
    manager = factory.getTwoPhaseCommitTransactionManager();
  }

  protected void initialize(String testName) throws Exception {}

  protected abstract Properties getProperties(String testName);

  protected String getNamespace() {
    return NAMESPACE;
  }

  private void createTables() throws ExecutionException {
    Map<String, String> options = getCreationOptions();
    admin.createNamespace(namespace, true, options);
    admin.createTable(namespace, TABLE, TABLE_METADATA, true, options);
    admin.createCoordinatorTables(true, options);
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  @BeforeEach
  public void setUp() throws Exception {
    admin.truncateTable(namespace, TABLE);
    admin.truncateCoordinatorTables();
  }

  @AfterAll
  public void afterAll() throws Exception {
    dropTables();
    admin.close();
    manager.close();
  }

  private void dropTables() throws ExecutionException {
    admin.dropTable(namespace, TABLE);
    admin.dropNamespace(namespace);
    admin.dropCoordinatorTables();
  }

  @Test
  public void scan_WithIncludeMetadataEnabled_ShouldReturnTransactionMetadataColumns()
      throws TransactionException {
    selection_WithIncludeMetadataEnabled_ShouldReturnCorrectColumns(true, false);
  }

  @Test
  public void scan_WithIncludeMetadataEnabledAndProjections_ShouldReturnProjectedColumns()
      throws TransactionException {
    selection_WithIncludeMetadataEnabled_ShouldReturnCorrectColumns(true, true);
  }

  @Test
  public void get_WithIncludeMetadataEnabled_ShouldReturnTransactionMetadataColumns()
      throws TransactionException {
    selection_WithIncludeMetadataEnabled_ShouldReturnCorrectColumns(false, false);
  }

  @Test
  public void get_WithIncludeMetadataEnabledAndProjections_ShouldReturnProjectedColumns()
      throws TransactionException {
    selection_WithIncludeMetadataEnabled_ShouldReturnCorrectColumns(false, true);
  }

  private void selection_WithIncludeMetadataEnabled_ShouldReturnCorrectColumns(
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
    TwoPhaseCommitTransaction transaction = manager.start();
    transaction.put(put);
    transaction.prepare();
    transaction.validate();
    transaction.commit();
    transaction = manager.start();
    Set<String> projections =
        ImmutableSet.of(ACCOUNT_ID, Attribute.BEFORE_PREFIX + BALANCE, Attribute.STATE);

    // Act Assert
    Result result;
    if (isScan) {
      // Perform a Scan
      ScanBuilder.BuildableScanOrScanAllFromExisting scanBuilder =
          Scan.newBuilder(prepareScan(0, 0, 1));
      if (hasProjections) {
        scanBuilder.projections(projections);
      }
      List<Result> results = transaction.scan(scanBuilder.build());
      assertThat(results.size()).isOne();
      result = results.get(0);
    } else {
      // Perform a Get
      GetBuilder.BuildableGet getBuilder = Get.newBuilder(prepareGet(0, 0));
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

  protected Get prepareGet(int id, int type) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    Key clusteringKey = new Key(ACCOUNT_TYPE, type);
    return new Get(partitionKey, clusteringKey)
        .forNamespace(namespace)
        .forTable(TABLE)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  protected Scan prepareScan(int id, int fromType, int toType) {
    Key partitionKey = new Key(ACCOUNT_ID, id);
    return new Scan(partitionKey)
        .forNamespace(namespace)
        .forTable(TABLE)
        .withConsistency(Consistency.LINEARIZABLE)
        .withStart(new Key(ACCOUNT_TYPE, fromType))
        .withEnd(new Key(ACCOUNT_TYPE, toType));
  }
}
