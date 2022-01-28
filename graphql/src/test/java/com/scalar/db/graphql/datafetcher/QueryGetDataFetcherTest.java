package com.scalar.db.graphql.datafetcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
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
import com.scalar.db.api.Get;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.graphql.schema.TableGraphQlModel;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils;
import graphql.execution.AbortExecutionException;
import graphql.execution.DataFetcherResult;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class QueryGetDataFetcherTest extends DataFetcherTestBase {
  private static final String COL1 = "c1";
  private static final String COL2 = "c2";
  private static final String COL3 = "c3";

  private TableMetadata tableMetadata;
  private QueryGetDataFetcher dataFetcher;
  private Map<String, Object> getInput;
  private Get expectedGet;

  @Override
  public void doSetUp() {
    // Arrange
    tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(COL1, DataType.INT)
            .addColumn(COL2, DataType.TEXT)
            .addColumn(COL3, DataType.DOUBLE)
            .addPartitionKey(COL1)
            .addClusteringKey(COL2)
            .build();
    dataFetcher =
        spy(
            new QueryGetDataFetcher(
                storage,
                new DataFetcherHelper(
                    new TableGraphQlModel(ANY_NAMESPACE, ANY_TABLE, tableMetadata))));
  }

  private void prepareGetInputAndExpectedGet() {
    // table1_get(get: {
    //   key: { c1: 1, c2: "A" }
    // })
    getInput = new HashMap<>();
    getInput.put("key", ImmutableMap.of(COL1, 1, COL2, "A"));
    when(environment.getArgument("get")).thenReturn(getInput);

    expectedGet =
        new Get(new Key(COL1, 1), new Key(COL2, "A"))
            .forNamespace(ANY_NAMESPACE)
            .forTable(ANY_TABLE);
  }

  @Test
  public void get_WhenTransactionNotStarted_ShouldUseStorage() throws Exception {
    // Arrange
    prepareGetInputAndExpectedGet();

    // Act
    dataFetcher.get(environment);

    // Assert
    verify(storage, times(1)).get(expectedGet);
    verify(transaction, never()).get(any());
  }

  @Test
  public void get_WhenTransactionStarted_ShouldUseTransaction() throws Exception {
    // Arrange
    prepareGetInputAndExpectedGet();
    setTransactionStarted();

    // Act
    dataFetcher.get(environment);

    // Assert
    verify(storage, never()).get(any());
    verify(transaction, times(1)).get(expectedGet);
  }

  @Test
  public void get_GetArgumentGiven_ShouldRunScalarDbGet() throws Exception {
    // Arrange
    prepareGetInputAndExpectedGet();

    // Act
    dataFetcher.get(environment);

    // Assert
    ArgumentCaptor<Get> argument = ArgumentCaptor.forClass(Get.class);
    verify(dataFetcher, times(1)).performGet(eq(environment), argument.capture());
    assertThat(argument.getValue()).isEqualTo(expectedGet);
  }

  @Test
  public void get_WhenGetSucceeds_ShouldReturnResultAsMap() throws Exception {
    // Arrange
    prepareGetInputAndExpectedGet();
    addSelectionSetToEnvironment(environment, COL1, COL2, COL3);

    Result mockResult = mock(Result.class);
    when(mockResult.getValue(COL1)).thenReturn(Optional.of(new IntValue(1)));
    when(mockResult.getValue(COL2)).thenReturn(Optional.of(new TextValue("A")));
    when(mockResult.getValue(COL3)).thenReturn(Optional.of(new DoubleValue(2.0)));
    doReturn(Optional.of(mockResult)).when(dataFetcher).performGet(eq(environment), any(Get.class));

    // Act
    DataFetcherResult<Map<String, Map<String, Object>>> result = dataFetcher.get(environment);

    // Assert
    Map<String, Object> object = result.getData().get(ANY_TABLE);
    assertThat(object)
        .containsOnly(entry(COL1, 1), entry(COL2, Optional.of("A")), entry(COL3, 2.0));
  }

  @Test
  public void get_WhenGetFails_ShouldReturnNullWithErrors() throws Exception {
    // Arrange
    prepareGetInputAndExpectedGet();
    ExecutionException exception = new ExecutionException("error");
    doThrow(exception).when(dataFetcher).performGet(eq(environment), any(Get.class));

    // Act
    DataFetcherResult<Map<String, Map<String, Object>>> result = dataFetcher.get(environment);

    // Assert
    assertThat(result.getData()).isNull();
    assertThatDataFetcherResultHasErrorForException(result, exception);
  }

  @Test
  public void createGet_GetInputGiven_ShouldReturnGet() {
    // Arrange
    prepareGetInputAndExpectedGet();

    // Act
    Get actual = dataFetcher.createGet(getInput, environment);

    // Assert
    assertThat(actual).isEqualTo(expectedGet);
  }

  @Test
  public void createGet_GetInputWithConsistencyGiven_ShouldReturnGetWithConsistency() {
    // Arrange
    prepareGetInputAndExpectedGet();
    getInput.put("consistency", "EVENTUAL");
    expectedGet.withConsistency(Consistency.EVENTUAL);

    // Act
    Get actual = dataFetcher.createGet(getInput, environment);

    // Assert
    assertThat(actual).isEqualTo(expectedGet);
  }

  @Test
  public void createScan_GetInputWithFieldSelectionGiven_ShouldReturnGetWithProjections() {
    // Arrange
    prepareGetInputAndExpectedGet();
    // table1_get(get: {
    //   key: { c1: 1, c2: "A" }
    // }) {
    //   table1 {
    //     c2, c3
    //   }
    // }
    addSelectionSetToEnvironment(environment, COL2, COL3);
    expectedGet.withProjections(ImmutableList.of(COL2, COL3));

    // Act
    Get actual = dataFetcher.createGet(getInput, environment);

    // Assert
    assertThat(actual).isEqualTo(expectedGet);
  }

  private void prepareTransactionalTable() {
    tableMetadata = ConsensusCommitUtils.buildTransactionalTableMetadata(tableMetadata);
    dataFetcher =
        new QueryGetDataFetcher(
            storage,
            new DataFetcherHelper(new TableGraphQlModel(ANY_NAMESPACE, ANY_TABLE, tableMetadata)));
  }

  @Test
  public void performGet_WhenTransactionalMetadataTableIsAccessedWithStorage_ShouldThrowException()
      throws Exception {
    // Arrange
    prepareTransactionalTable();
    Get get = new Get(new Key(COL1, 1));

    // Act Assert
    assertThatThrownBy(() -> dataFetcher.performGet(environment, get))
        .isInstanceOf(AbortExecutionException.class);
    verify(storage, never()).get(get);
    verify(transaction, never()).get(get);
  }

  @Test
  public void performGet_WhenTransactionalMetadataTableIsAccessedWithTransaction_ShouldRunCommand()
      throws Exception {
    // Arrange
    prepareTransactionalTable();
    setTransactionStarted();
    Get get = new Get(new Key(COL1, 1));

    // Act
    dataFetcher.performGet(environment, get);

    // Assert
    verify(storage, never()).get(get);
    verify(transaction, times(1)).get(get);
  }
}
