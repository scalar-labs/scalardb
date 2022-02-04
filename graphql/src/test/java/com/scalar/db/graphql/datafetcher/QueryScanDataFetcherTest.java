package com.scalar.db.graphql.datafetcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.graphql.schema.TableGraphQlModel;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils;
import graphql.execution.AbortExecutionException;
import graphql.execution.DataFetcherResult;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class QueryScanDataFetcherTest extends DataFetcherTestBase {
  private static final String COL1 = "c1";
  private static final String COL2 = "c2";
  private static final String COL3 = "c3";
  private static final String COL4 = "c4";
  private static final String COL5 = "c5";
  private static final String COL6 = "c5";

  private TableMetadata tableMetadata;
  private QueryScanDataFetcher dataFetcher;
  private Map<String, Object> scanInput;
  private Scan expectedScan;

  @Override
  protected void doSetUp() throws Exception {
    // Arrange
    tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(COL1, DataType.INT)
            .addColumn(COL2, DataType.TEXT)
            .addColumn(COL3, DataType.BIGINT)
            .addColumn(COL4, DataType.FLOAT)
            .addColumn(COL5, DataType.DOUBLE)
            .addColumn(COL6, DataType.BOOLEAN)
            .addPartitionKey(COL1)
            .addPartitionKey(COL2)
            .addClusteringKey(COL3)
            .addClusteringKey(COL4)
            .addClusteringKey(COL5)
            .build();
    dataFetcher =
        spy(
            new QueryScanDataFetcher(
                storage,
                new DataFetcherHelper(
                    new TableGraphQlModel(ANY_NAMESPACE, ANY_TABLE, tableMetadata))));

    // Mock scanner for storage
    Scanner mockScanner = mock(Scanner.class);
    when(mockScanner.iterator()).thenReturn(Collections.emptyIterator());
    when(storage.scan(any())).thenReturn(mockScanner);
  }

  private void prepareScanInputAndExpectedScan() {
    // table1_scan(scan: {
    //   partitionKey: { c1: 1, c2: "A" }
    // })
    scanInput = new HashMap<>();
    scanInput.put("partitionKey", ImmutableMap.of(COL1, 1, COL2, "A"));
    when(environment.getArgument("scan")).thenReturn(scanInput);

    expectedScan =
        new Scan(new Key(new IntValue(COL1, 1), new TextValue(COL2, "A")))
            .forNamespace(ANY_NAMESPACE)
            .forTable(ANY_TABLE);
  }

  @Test
  public void get_WhenTransactionNotStarted_ShouldUseStorage() throws Exception {
    // Arrange
    prepareScanInputAndExpectedScan();

    // Act
    dataFetcher.get(environment);

    // Assert
    verify(storage, times(1)).scan(expectedScan);
    verify(transaction, never()).get(any());
  }

  @Test
  public void get_WhenTransactionStarted_ShouldUseTransaction() throws Exception {
    // Arrange
    prepareScanInputAndExpectedScan();
    setTransactionStarted();

    // Act
    dataFetcher.get(environment);

    // Assert
    verify(storage, never()).get(any());
    verify(transaction, times(1)).scan(expectedScan);
  }

  @Test
  public void get_ScanArgumentGiven_ShouldRunScalarDbScan() throws Exception {
    // Arrange
    prepareScanInputAndExpectedScan();

    // Act
    dataFetcher.get(environment);

    // Assert
    ArgumentCaptor<Scan> argument = ArgumentCaptor.forClass(Scan.class);
    verify(dataFetcher, times(1)).performScan(eq(environment), argument.capture());
    assertThat(argument.getValue()).isEqualTo(expectedScan);
  }

  @Test
  public void get_WhenScanSucceeds_ShouldReturnListData() throws Exception {
    // Arrange
    prepareScanInputAndExpectedScan();
    addSelectionSetToEnvironment(environment, COL1, COL2, COL3);

    Result mockResult1 = mock(Result.class);
    when(mockResult1.getValue(COL1)).thenReturn(Optional.of(new IntValue(1)));
    when(mockResult1.getValue(COL2)).thenReturn(Optional.of(new TextValue("A")));
    when(mockResult1.getValue(COL3)).thenReturn(Optional.of(new BigIntValue(2L)));
    Result mockResult2 = mock(Result.class);
    when(mockResult2.getValue(COL1)).thenReturn(Optional.of(new IntValue(2)));
    when(mockResult2.getValue(COL2)).thenReturn(Optional.of(new TextValue("B")));
    when(mockResult2.getValue(COL3)).thenReturn(Optional.of(new BigIntValue(3L)));
    doAnswer(
            invocation -> {
              // clearProjections() is called when run with ConsensusCommit or
              // TwoPhaseConsensusCommit
              ((Scan) invocation.getArgument(1)).clearProjections();
              return Arrays.asList(mockResult1, mockResult2);
            })
        .when(dataFetcher)
        .performScan(eq(environment), any(Scan.class));

    // Act
    DataFetcherResult<Map<String, List<Map<String, Object>>>> result = dataFetcher.get(environment);

    // Assert
    List<Map<String, Object>> list = result.getData().get(ANY_TABLE);
    assertThat(list).hasSize(2);
    assertThat(list.get(0))
        .containsOnly(entry(COL1, 1), entry(COL2, Optional.of("A")), entry(COL3, 2L));
    assertThat(list.get(1))
        .containsOnly(entry(COL1, 2), entry(COL2, Optional.of("B")), entry(COL3, 3L));
  }

  @Test
  public void get_WhenScanFails_ShouldReturnNullDataWithErrors() throws Exception {
    // Arrange
    prepareScanInputAndExpectedScan();
    ExecutionException exception = new ExecutionException("error");
    doThrow(exception).when(dataFetcher).performScan(eq(environment), any(Scan.class));

    // Act
    DataFetcherResult<Map<String, List<Map<String, Object>>>> result = dataFetcher.get(environment);

    // Assert
    assertThat(result.getData()).isNull();
    assertThatDataFetcherResultHasErrorForException(result, exception);
  }

  @Test
  public void createScan_ScanInputGiven_ShouldReturnScan() {
    // Arrange
    prepareScanInputAndExpectedScan();

    // Act
    Scan actual = dataFetcher.createScan(scanInput, environment);

    // Assert
    assertThat(actual).isEqualTo(expectedScan);
  }

  @Test
  public void createScan_ScanInputWithConsistencyGiven_ShouldReturnScanWithConsistency() {
    // Arrange
    prepareScanInputAndExpectedScan();
    scanInput.put("consistency", "EVENTUAL");
    expectedScan.withConsistency(Consistency.EVENTUAL);

    // Act
    Scan actual = dataFetcher.createScan(scanInput, environment);

    // Assert
    assertThat(actual).isEqualTo(expectedScan);
  }

  @Test
  public void createScan_ScanInputWithStartGiven_ShouldReturnScanWithStart() {
    // Arrange
    prepareScanInputAndExpectedScan();
    // start: [{ name: c3, bigIntValue: 1 }],
    // startInclusive: false
    scanInput.put("start", ImmutableList.of(ImmutableMap.of("name", COL3, "bigIntValue", 1L)));
    scanInput.put("startInclusive", false);
    expectedScan.withStart(new Key(new BigIntValue(COL3, 1L)), false);

    // Act
    Scan actual = dataFetcher.createScan(scanInput, environment);

    // Assert
    assertThat(actual).isEqualTo(expectedScan);
  }

  @Test
  public void createScan_ScanInputWithEndGiven_ShouldReturnScanWithEnd() {
    // Arrange
    prepareScanInputAndExpectedScan();
    // end: [{ name: c3, bigIntValue: 10 }],
    // endInclusive: false
    scanInput.put("end", ImmutableList.of(ImmutableMap.of("name", COL3, "bigIntValue", 10L)));
    scanInput.put("endInclusive", false);
    expectedScan.withEnd(new Key(new BigIntValue(COL3, 10L)), false);

    // Act
    Scan actual = dataFetcher.createScan(scanInput, environment);

    // Assert
    assertThat(actual).isEqualTo(expectedScan);
  }

  @Test
  public void createScan_ScanInputWithOrderingsGiven_ShouldReturnScanWithOrderings() {
    // Arrange
    prepareScanInputAndExpectedScan();
    // orderings: [{ name: c4, order: ASC }, { name: c3, order: DESC }],
    scanInput.put(
        "orderings",
        ImmutableList.of(
            ImmutableMap.of("name", COL4, "order", "ASC"),
            ImmutableMap.of("name", COL3, "order", "DESC")));
    expectedScan
        .withOrdering(new Scan.Ordering(COL4, Scan.Ordering.Order.ASC))
        .withOrdering(new Scan.Ordering(COL3, Scan.Ordering.Order.DESC));

    // Act
    Scan actual = dataFetcher.createScan(scanInput, environment);

    // Assert
    assertThat(actual).isEqualTo(expectedScan);
  }

  @Test
  public void createScan_ScanInputWithLimitGiven_ShouldReturnScanWithLimit() {
    // Arrange
    prepareScanInputAndExpectedScan();
    scanInput.put("limit", 100);
    expectedScan.withLimit(100);

    // Act
    Scan actual = dataFetcher.createScan(scanInput, environment);

    // Assert
    assertThat(actual).isEqualTo(expectedScan);
  }

  @Test
  public void createScan_ScanInputWithFieldSelectionGiven_ShouldReturnScanWithProjections() {
    // Arrange
    prepareScanInputAndExpectedScan();
    // table1_scan(scan: {
    //   partitionKey: { c1: 1, c2: "A" }
    // }) {
    //   table1 {
    //     c2, c3
    //   }
    // }
    addSelectionSetToEnvironment(environment, COL2, COL3);
    expectedScan.withProjections(ImmutableList.of(COL2, COL3));

    // Act
    Scan actual = dataFetcher.createScan(scanInput, environment);

    // Assert
    assertThat(actual).isEqualTo(expectedScan);
  }

  private void prepareTransactionalTable() {
    tableMetadata = ConsensusCommitUtils.buildTransactionalTableMetadata(tableMetadata);
    dataFetcher =
        new QueryScanDataFetcher(
            storage,
            new DataFetcherHelper(new TableGraphQlModel(ANY_NAMESPACE, ANY_TABLE, tableMetadata)));
  }

  @Test
  public void performScan_WhenTransactionalMetadataTableIsAccessedWithStorage_ShouldThrowException()
      throws Exception {
    // Arrange
    prepareTransactionalTable();
    Scan scan = new Scan(new Key(COL1, 1));

    // Act Assert
    assertThatThrownBy(() -> dataFetcher.performScan(environment, scan))
        .isInstanceOf(AbortExecutionException.class);
    verify(storage, never()).scan(scan);
    verify(transaction, never()).scan(scan);
  }

  @Test
  public void performScan_WhenTransactionalMetadataTableIsAccessedWithTransaction_ShouldRunCommand()
      throws Exception {
    // Arrange
    prepareTransactionalTable();
    setTransactionStarted();
    Scan scan = new Scan(new Key(COL1, 1));

    // Act
    dataFetcher.performScan(environment, scan);

    // Assert
    verify(storage, never()).scan(scan);
    verify(transaction, times(1)).scan(scan);
  }
}
