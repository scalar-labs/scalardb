package com.scalar.db.graphql.datafetcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.graphql.schema.TableGraphQlModel;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.DataType;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils;
import java.util.Collections;
import java.util.Map;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class QueryScanDataFetcherTest extends DataFetcherTestBase {
  private TableGraphQlModel storageTableGraphQlModel;
  private TableGraphQlModel transactionalTableGraphQlModel;
  private Map<String, Object> simpleScanArgument;
  private Scan simpleExpectedScan;

  @Override
  protected void doSetUp() throws Exception {
    // Arrange
    TableMetadata storageTableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.TEXT)
            .addColumn("c3", DataType.BIGINT)
            .addColumn("c4", DataType.FLOAT)
            .addColumn("c5", DataType.DOUBLE)
            .addColumn("c6", DataType.BOOLEAN)
            .addPartitionKey("c1")
            .addPartitionKey("c2")
            .addClusteringKey("c3")
            .addClusteringKey("c4")
            .addClusteringKey("c5")
            .build();
    storageTableGraphQlModel =
        new TableGraphQlModel(ANY_NAMESPACE, ANY_TABLE, storageTableMetadata);
    TableMetadata transactionalTableMetadata =
        ConsensusCommitUtils.buildTransactionalTableMetadata(storageTableMetadata);
    transactionalTableGraphQlModel =
        new TableGraphQlModel(ANY_NAMESPACE, ANY_TABLE, transactionalTableMetadata);

    // Mock scanner for storage
    Scanner mockScanner = mock(Scanner.class);
    when(mockScanner.iterator()).thenReturn(Collections.emptyIterator());
    when(storage.scan(any())).thenReturn(mockScanner);
  }

  private void prepareSimpleScan() {
    // table1_scan(scan: {
    //   partitionKey: { c1: 1, c2: "A" },
    //   start: [{ name: c3, bigIntValue: 1 }, { name: c4, floatValue: 0.1 }],
    //   startInclusive: true,
    //   limit: 10,
    //   orderings: [{ name: c4, order: ASC }, { name: c3, order: DESC }],
    //   consistency: EVENTUAL
    // })
    simpleScanArgument =
        ImmutableMap.<String, Object>builder()
            .put("partitionKey", ImmutableMap.of("c1", 1, "c2", "A"))
            .put(
                "start",
                ImmutableList.of(
                    ImmutableMap.of("name", "c3", "bigIntValue", 1L),
                    ImmutableMap.of("name", "c4", "floatValue", 0.1F)))
            .put("startInclusive", true)
            .put("limit", 10)
            .put(
                "orderings",
                ImmutableList.of(
                    ImmutableMap.of("name", "c4", "order", "ASC"),
                    ImmutableMap.of("name", "c3", "order", "DESC")))
            .put("consistency", "EVENTUAL")
            .build();

    when(environment.getArgument("scan")).thenReturn(simpleScanArgument);

    simpleExpectedScan =
        new Scan(new Key(new IntValue("c1", 1), new TextValue("c2", "A")))
            .forNamespace(ANY_NAMESPACE)
            .forTable(ANY_TABLE)
            .withStart(new Key(new BigIntValue("c3", 1L), new FloatValue("c4", 0.1F)), true)
            .withLimit(10)
            .withOrdering(new Scan.Ordering("c4", Scan.Ordering.Order.ASC))
            .withOrdering(new Scan.Ordering("c3", Scan.Ordering.Order.DESC))
            .withConsistency(Consistency.EVENTUAL);
  }

  @Test
  public void get_ForStorageTable_ShouldUseStorage() throws Exception {
    // Arrange
    prepareSimpleScan();
    QueryScanDataFetcher dataFetcher =
        spy(new QueryScanDataFetcher(storage, new DataFetcherHelper(storageTableGraphQlModel)));

    // Act
    dataFetcher.get(environment);

    // Assert
    verify(storage, times(1)).scan(simpleExpectedScan);
    verify(transaction, never()).get(any());
  }

  @Test
  public void get_ForTransactionalTable_ShouldUseTransaction() throws Exception {
    // Arrange
    prepareSimpleScan();
    QueryScanDataFetcher dataFetcher =
        spy(
            new QueryScanDataFetcher(
                storage, new DataFetcherHelper(transactionalTableGraphQlModel)));

    // Act
    dataFetcher.get(environment);

    // Assert
    verify(storage, never()).get(any());
    verify(transaction, times(1)).scan(simpleExpectedScan);
  }

  private void testScanCommandIssued(Map<String, Object> scanArgument, Scan expectedScan)
      throws Exception {
    // Arrange
    when(environment.getArgument("scan")).thenReturn(scanArgument);
    QueryScanDataFetcher dataFetcher =
        spy(new QueryScanDataFetcher(storage, new DataFetcherHelper(storageTableGraphQlModel)));

    // Act
    dataFetcher.get(environment);

    // Assert
    ArgumentCaptor<Scan> argument = ArgumentCaptor.forClass(Scan.class);
    verify(dataFetcher, times(1)).performScan(eq(environment), argument.capture());
    assertThat(argument.getValue()).isEqualTo(expectedScan);
  }

  @Test
  public void get_ScanArgumentGiven_ShouldRunScalarDbScan() throws Exception {
    // Arrange
    prepareSimpleScan();

    // Act Assert
    testScanCommandIssued(simpleScanArgument, simpleExpectedScan);
  }

  @Test
  public void get_ScanArgumentWithStart_ShouldRunScalarDbScan() throws Exception {
    // Arrange
    // table1_scan(scan: {
    //   partitionKey: { c1: 1, c2: "A" },
    //   start: [{ name: c3, bigIntValue: 1 }],
    //   startInclusive: false
    // })
    Map<String, Object> scanArgument =
        ImmutableMap.of(
            "partitionKey",
            ImmutableMap.of("c1", 1, "c2", "A"),
            "start",
            ImmutableList.of(ImmutableMap.of("name", "c3", "bigIntValue", 1L)),
            "startInclusive",
            false);
    Scan expectedScan =
        new Scan(new Key(new IntValue("c1", 1), new TextValue("c2", "A")))
            .withStart(new Key(new BigIntValue("c3", 1L)), false)
            .forNamespace(ANY_NAMESPACE)
            .forTable(ANY_TABLE);

    // Act Assert
    testScanCommandIssued(scanArgument, expectedScan);
  }

  @Test
  public void get_ScanArgumentWithEnd_ShouldRunScalarDbScan() throws Exception {
    // Arrange
    // table1_scan(scan: {
    //   partitionKey: { c1: 1, c2: "A" },
    //   end: [{ name: c3, bigIntValue: 1 }],
    //   endInclusive: false
    // })
    Map<String, Object> scanArgument =
        ImmutableMap.of(
            "partitionKey",
            ImmutableMap.of("c1", 1, "c2", "A"),
            "end",
            ImmutableList.of(ImmutableMap.of("name", "c3", "bigIntValue", 1L)),
            "endInclusive",
            false);
    Scan expectedScan =
        new Scan(new Key(new IntValue("c1", 1), new TextValue("c2", "A")))
            .withEnd(new Key(new BigIntValue("c3", 1L)), false)
            .forNamespace(ANY_NAMESPACE)
            .forTable(ANY_TABLE);

    // Act Assert
    testScanCommandIssued(scanArgument, expectedScan);
  }
}
