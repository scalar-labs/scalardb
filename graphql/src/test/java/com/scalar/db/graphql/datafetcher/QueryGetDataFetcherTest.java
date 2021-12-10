package com.scalar.db.graphql.datafetcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Get;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.graphql.schema.TableGraphQlModel;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class QueryGetDataFetcherTest extends DataFetcherTestBase {
  private TableGraphQlModel storageTableGraphQlModel;
  private TableGraphQlModel transactionalTableGraphQlModel;
  private Get expectedGetCommand;

  @Override
  public void doSetUp() {
    // Arrange
    TableMetadata storageTableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.TEXT)
            .addColumn("c3", DataType.DOUBLE)
            .addPartitionKey("c1")
            .addClusteringKey("c2")
            .build();
    storageTableGraphQlModel =
        new TableGraphQlModel(ANY_NAMESPACE, ANY_TABLE, storageTableMetadata);
    TableMetadata transactionalTableMetadata =
        ConsensusCommitUtils.buildTransactionalTableMetadata(storageTableMetadata);
    transactionalTableGraphQlModel =
        new TableGraphQlModel(ANY_NAMESPACE, ANY_TABLE, transactionalTableMetadata);

    // table1_get(get: {
    //   key: { c1: 1, c2: "A" },
    //   consistency: EVENTUAL
    // })
    Map<String, Object> getArgument =
        ImmutableMap.of("key", ImmutableMap.of("c1", 1, "c2", "A"), "consistency", "EVENTUAL");
    when(environment.getArgument("get")).thenReturn(getArgument);

    expectedGetCommand =
        new Get(new Key("c1", 1), new Key("c2", "A"))
            .forNamespace(ANY_NAMESPACE)
            .forTable(ANY_TABLE)
            .withConsistency(Consistency.EVENTUAL);
  }

  @Test
  public void get_ForStorageTable_ShouldUseStorage() throws Exception {
    // Arrange
    QueryGetDataFetcher dataFetcher =
        spy(new QueryGetDataFetcher(storage, new DataFetcherHelper(storageTableGraphQlModel)));

    // Act
    dataFetcher.get(environment);

    // Assert
    verify(storage, times(1)).get(expectedGetCommand);
    verify(transaction, never()).get(any());
  }

  @Test
  public void get_ForTransactionalTable_ShouldUseTransaction() throws Exception {
    // Arrange
    QueryGetDataFetcher dataFetcher =
        spy(
            new QueryGetDataFetcher(
                storage, new DataFetcherHelper(transactionalTableGraphQlModel)));

    // Act
    dataFetcher.get(environment);

    // Assert
    verify(storage, never()).get(any());
    verify(transaction, times(1)).get(expectedGetCommand);
  }

  @Test
  public void get_GetArgumentGiven_ShouldRunScalarDbGet() throws Exception {
    // Arrange
    QueryGetDataFetcher dataFetcher =
        spy(new QueryGetDataFetcher(storage, new DataFetcherHelper(storageTableGraphQlModel)));

    // Act
    dataFetcher.get(environment);

    // Assert
    ArgumentCaptor<Get> argument = ArgumentCaptor.forClass(Get.class);
    verify(dataFetcher, times(1)).performGet(eq(environment), argument.capture());
    assertThat(argument.getValue()).isEqualTo(expectedGetCommand);
  }

  @Test
  public void get_GetArgumentGiven_ShouldReturnResultAsMap() throws Exception {
    // Arrange
    QueryGetDataFetcher dataFetcher =
        new QueryGetDataFetcher(storage, new DataFetcherHelper(storageTableGraphQlModel));
    Result mockResult = mock(Result.class);
    when(mockResult.getValue("c1")).thenReturn(Optional.of(new IntValue(1)));
    when(mockResult.getValue("c2")).thenReturn(Optional.of(new TextValue("A")));
    when(mockResult.getValue("c3")).thenReturn(Optional.of(new DoubleValue(2.0)));
    when(storage.get(any(Get.class))).thenReturn(Optional.of(mockResult));

    // Act
    Map<String, Map<String, Object>> result = dataFetcher.get(environment);

    // Assert
    Map<String, Object> object = result.get(storageTableGraphQlModel.getObjectType().getName());
    assertThat(object)
        .containsOnly(entry("c1", 1), entry("c2", Optional.of("A")), entry("c3", 2.0));
  }
}
