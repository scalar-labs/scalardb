package com.scalar.db.graphql.datafetcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.graphql.schema.TableGraphQlModel;
import com.scalar.db.io.DataType;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetcher;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class MutationDataFetcherFactoryDataFetcherForBulkPutTest extends DataFetcherTestBase {
  private static final String COL1 = "c1";
  private static final String COL2 = "c2";
  private static final String COL3 = "c3";

  private MutationDataFetcherFactory dataFetcherFactory;
  private DataFetcher<DataFetcherResult<Boolean>> dataFetcher;

  @Before
  public void setUp() {
    // Arrange
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(COL1, DataType.INT)
            .addColumn(COL2, DataType.TEXT)
            .addColumn(COL3, DataType.FLOAT)
            .addPartitionKey(COL1)
            .addClusteringKey(COL2)
            .build();
    dataFetcherFactory =
        spy(
            new MutationDataFetcherFactory(
                new TableGraphQlModel(ANY_NAMESPACE, ANY_TABLE, tableMetadata), storage));

    dataFetcher = dataFetcherFactory::dataFetcherForBulkPut;

    // table1_bulkPut(put: [
    //   {
    //     key: { c1: 1, c2: "A" },
    //     values: { c3: 2.0 }
    //   }, {
    //     key: { c1: 1, c2: "B" },
    //     values: { c3: 3.0 }
    //   }
    // ])
    List<Map<String, Object>> putInput =
        ImmutableList.of(
            ImmutableMap.of(
                "key", ImmutableMap.of(COL1, 1, COL2, "A"), "values", ImmutableMap.of(COL3, 2.0F)),
            ImmutableMap.of(
                "key", ImmutableMap.of(COL1, 1, COL2, "B"), "values", ImmutableMap.of(COL3, 3.0F)));
    when(dataFetchingEnvironment.getArgument("put")).thenReturn(putInput);
  }

  @Test
  public void get_WhenOperationSucceeds_ShouldReturnTrue() throws Exception {
    // Arrange
    doNothing().when(dataFetcherFactory).performPut(eq(dataFetchingEnvironment), anyList());

    // Act
    DataFetcherResult<Boolean> result = dataFetcher.get(dataFetchingEnvironment);

    // Assert
    assertThat(result.getData()).isTrue();
    assertThat(result.getErrors()).isEmpty();
  }

  @Test
  public void get_WhenOperationFails_ShouldReturnFalseWithErrors() throws Exception {
    // Arrange
    ExecutionException exception = new ExecutionException("error");
    doThrow(exception).when(dataFetcherFactory).performPut(eq(dataFetchingEnvironment), anyList());

    // Act
    DataFetcherResult<Boolean> result = dataFetcher.get(dataFetchingEnvironment);

    // Assert
    assertThat(result.getData()).isFalse();
    assertThatDataFetcherResultHasErrorForException(result, exception);
  }
}
