package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.util.TableMetadataManager;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class SelectStatementHandlerTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final String ANY_TEXT_4 = "text4";
  private static final int ANY_LIMIT = 100;

  private SelectStatementHandler handler;
  private String id;
  private PartitionKey cosmosPartitionKey;
  @Mock private CosmosClient client;
  @Mock private CosmosDatabase database;
  @Mock private CosmosContainer container;
  @Mock private TableMetadataManager metadataManager;
  @Mock private TableMetadata metadata;
  @Mock private CosmosItemResponse<Record> response;
  @Mock private CosmosPagedIterable<Record> responseIterable;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    handler = new SelectStatementHandler(client, metadataManager);
    when(client.getDatabase(anyString())).thenReturn(database);
    when(database.getContainer(anyString())).thenReturn(container);

    when(metadataManager.getTableMetadata(any(Operation.class))).thenReturn(metadata);
    when(metadata.getPartitionKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_1)));
    when(metadata.getClusteringKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_2)));
    when(metadata.getSecondaryIndexNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_3)));
    when(metadata.getClusteringOrder(ANY_NAME_2)).thenReturn(Order.ASC);
  }

  private Get prepareGet() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    id = ANY_TEXT_1 + ":" + ANY_TEXT_2;
    cosmosPartitionKey = new PartitionKey(ANY_TEXT_1);
    return new Get(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private Scan prepareScan() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    return new Scan(partitionKey).forNamespace(ANY_NAMESPACE_NAME).forTable(ANY_TABLE_NAME);
  }

  @Test
  public void handle_GetOperationGiven_ShouldCallReadItem() {
    // Arrange
    when(container.readItem(anyString(), any(PartitionKey.class), eq(Record.class)))
        .thenReturn(response);
    Record expected = new Record();
    when(response.getItem()).thenReturn(expected);
    Get get = prepareGet();

    // Act Assert
    assertThatCode(() -> handler.handle(get)).doesNotThrowAnyException();

    // Assert
    verify(container).readItem(id, cosmosPartitionKey, Record.class);
  }

  @Test
  public void handle_GetOperationWithIndexGiven_ShouldCallQueryItems() {
    // Arrange
    when(container.queryItems(anyString(), any(CosmosQueryRequestOptions.class), eq(Record.class)))
        .thenReturn(responseIterable);
    Record expected = new Record();
    when(responseIterable.iterator()).thenReturn(Collections.singletonList(expected).iterator());
    Key indexKey = new Key(ANY_NAME_3, ANY_TEXT_3);
    Get get = new Get(indexKey).forNamespace(ANY_NAMESPACE_NAME).forTable(ANY_TABLE_NAME);
    String query =
        "select * from Record r where r.values[\"" + ANY_NAME_3 + "\"]" + " = '" + ANY_TEXT_3 + "'";

    // Act Assert
    assertThatCode(() -> handler.handle(get)).doesNotThrowAnyException();

    // Assert
    verify(container).queryItems(eq(query), any(CosmosQueryRequestOptions.class), eq(Record.class));
  }

  @Test
  public void handle_CosmosExceptionWithNotFound_ShouldReturnEmptyList() throws Exception {
    // Arrange
    CosmosException toThrow = mock(CosmosException.class);
    doThrow(toThrow)
        .when(container)
        .readItem(anyString(), any(PartitionKey.class), eq(Record.class));
    when(toThrow.getStatusCode()).thenReturn(CosmosErrorCode.NOT_FOUND.get());

    Get get = prepareGet();

    // Act Assert
    List<Record> actual = handler.handle(get);

    // Assert
    assertThat(actual).isEmpty();
  }

  @Test
  public void handle_GetOperationCosmosExceptionThrown_ShouldThrowExecutionException() {
    // Arrange
    CosmosException toThrow = mock(CosmosException.class);
    doThrow(toThrow)
        .when(container)
        .readItem(anyString(), any(PartitionKey.class), eq(Record.class));

    Get get = prepareGet();

    // Act Assert
    assertThatThrownBy(() -> handler.handle(get))
        .isInstanceOf(ExecutionException.class)
        .hasCause(toThrow);
  }

  @Test
  public void handle_ScanOperationGiven_ShouldCallQueryItems() {
    // Arrange
    when(container.queryItems(anyString(), any(CosmosQueryRequestOptions.class), eq(Record.class)))
        .thenReturn(responseIterable);
    Record expected = new Record();
    when(responseIterable.iterator()).thenReturn(Collections.singletonList(expected).iterator());

    Scan scan = prepareScan();
    String query =
        "select * from Record r where r.concatenatedPartitionKey = '"
            + ANY_TEXT_1
            + "' order by r.concatenatedPartitionKey asc, r.clusteringKey[\""
            + ANY_NAME_2
            + "\"] asc";

    // Act Assert
    assertThatCode(() -> handler.handle(scan)).doesNotThrowAnyException();

    // Assert
    verify(container).queryItems(eq(query), any(CosmosQueryRequestOptions.class), eq(Record.class));
  }

  @Test
  public void handle_ScanOperationWithIndexGiven_ShouldCallQueryItems() {
    // Arrange
    when(container.queryItems(anyString(), any(CosmosQueryRequestOptions.class), eq(Record.class)))
        .thenReturn(responseIterable);
    Record expected = new Record();
    when(responseIterable.iterator()).thenReturn(Collections.singletonList(expected).iterator());

    Key indexKey = new Key(ANY_NAME_3, ANY_TEXT_3);
    Scan scan = new Scan(indexKey).forNamespace(ANY_NAMESPACE_NAME).forTable(ANY_TABLE_NAME);
    String query =
        "select * from Record r where r.values[\"" + ANY_NAME_3 + "\"]" + " = '" + ANY_TEXT_3 + "'";

    // Act Assert
    assertThatCode(() -> handler.handle(scan)).doesNotThrowAnyException();

    // Assert
    verify(container).queryItems(eq(query), any(CosmosQueryRequestOptions.class), eq(Record.class));
  }

  @Test
  public void handle_ScanOperationCosmosExceptionThrown_ShouldThrowExecutionException() {
    // Arrange
    CosmosException toThrow = mock(CosmosException.class);
    doThrow(toThrow)
        .when(container)
        .queryItems(anyString(), any(CosmosQueryRequestOptions.class), eq(Record.class));

    Scan scan = prepareScan();

    // Act Assert
    assertThatThrownBy(() -> handler.handle(scan))
        .isInstanceOf(ExecutionException.class)
        .hasCause(toThrow);
  }

  @Test
  public void handle_ScanOperationWithSingleClusteringKey_ShouldCallQueryItemsWithProperQuery() {
    // Arrange
    when(container.queryItems(anyString(), any(CosmosQueryRequestOptions.class), eq(Record.class)))
        .thenReturn(responseIterable);
    Record expected = new Record();
    when(responseIterable.iterator()).thenReturn(Collections.singletonList(expected).iterator());

    Scan scan =
        prepareScan()
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_2))
            .withEnd(new Key(ANY_NAME_2, ANY_TEXT_3));

    String query =
        "select * from Record r where (r.concatenatedPartitionKey = '"
            + ANY_TEXT_1
            + "' and r.clusteringKey[\""
            + ANY_NAME_2
            + "\"] >= '"
            + ANY_TEXT_2
            + "' and r.clusteringKey[\""
            + ANY_NAME_2
            + "\"] <= '"
            + ANY_TEXT_3
            + "') order by r.concatenatedPartitionKey asc, r.clusteringKey[\""
            + ANY_NAME_2
            + "\"] asc";

    // Act Assert
    assertThatCode(() -> handler.handle(scan)).doesNotThrowAnyException();

    // Assert
    verify(container).queryItems(eq(query), any(CosmosQueryRequestOptions.class), eq(Record.class));
  }

  @Test
  public void handle_ScanOperationWithMultipleClusteringKeys_ShouldCallQueryItemsWithProperQuery() {
    // Arrange
    when(metadata.getClusteringKeyNames())
        .thenReturn(new LinkedHashSet<>(Arrays.asList(ANY_NAME_2, ANY_NAME_3)));
    when(metadata.getClusteringOrder(ANY_NAME_3)).thenReturn(Order.DESC);

    when(container.queryItems(anyString(), any(CosmosQueryRequestOptions.class), eq(Record.class)))
        .thenReturn(responseIterable);
    Record expected = new Record();
    when(responseIterable.iterator()).thenReturn(Collections.singletonList(expected).iterator());

    Scan scan =
        prepareScan()
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_2, ANY_NAME_3, ANY_TEXT_3))
            .withEnd(new Key(ANY_NAME_2, ANY_TEXT_2, ANY_NAME_3, ANY_TEXT_4));

    String query =
        "select * from Record r where (r.concatenatedPartitionKey = '"
            + ANY_TEXT_1
            + "' and r.clusteringKey[\""
            + ANY_NAME_2
            + "\"] = '"
            + ANY_TEXT_2
            + "' and r.clusteringKey[\""
            + ANY_NAME_3
            + "\"] >= '"
            + ANY_TEXT_3
            + "' and r.clusteringKey[\""
            + ANY_NAME_2
            + "\"] = '"
            + ANY_TEXT_2
            + "' and r.clusteringKey[\""
            + ANY_NAME_3
            + "\"] <= '"
            + ANY_TEXT_4
            + "') order by r.concatenatedPartitionKey asc, r.clusteringKey[\""
            + ANY_NAME_2
            + "\"] asc, r.clusteringKey[\""
            + ANY_NAME_3
            + "\"] desc";

    // Act Assert
    assertThatCode(() -> handler.handle(scan)).doesNotThrowAnyException();

    // Assert
    verify(container).queryItems(eq(query), any(CosmosQueryRequestOptions.class), eq(Record.class));
  }

  @Test
  public void handle_ScanOperationWithNeitherInclusive_ShouldCallQueryItemsWithProperQuery() {
    // Arrange
    when(container.queryItems(anyString(), any(CosmosQueryRequestOptions.class), eq(Record.class)))
        .thenReturn(responseIterable);
    Record expected = new Record();
    when(responseIterable.iterator()).thenReturn(Collections.singletonList(expected).iterator());

    Scan scan =
        prepareScan()
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_2), false)
            .withEnd(new Key(ANY_NAME_2, ANY_TEXT_3), false);

    String query =
        "select * from Record r where (r.concatenatedPartitionKey = '"
            + ANY_TEXT_1
            + "' and r.clusteringKey[\""
            + ANY_NAME_2
            + "\"] > '"
            + ANY_TEXT_2
            + "' and r.clusteringKey[\""
            + ANY_NAME_2
            + "\"] < '"
            + ANY_TEXT_3
            + "') order by r.concatenatedPartitionKey asc, r.clusteringKey[\""
            + ANY_NAME_2
            + "\"] asc";

    // Act Assert
    assertThatCode(() -> handler.handle(scan)).doesNotThrowAnyException();

    // Assert
    verify(container).queryItems(eq(query), any(CosmosQueryRequestOptions.class), eq(Record.class));
  }

  @Test
  public void handle_ScanOperationWithOrderingAndLimit_ShouldCallQueryItemsWithProperQuery() {
    // Arrange
    when(container.queryItems(anyString(), any(CosmosQueryRequestOptions.class), eq(Record.class)))
        .thenReturn(responseIterable);
    Record expected = new Record();
    when(responseIterable.iterator()).thenReturn(Collections.singletonList(expected).iterator());

    Scan scan =
        prepareScan()
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_2))
            .withOrdering(new Scan.Ordering(ANY_NAME_2, Order.ASC))
            .withLimit(ANY_LIMIT);

    String query =
        "select * from Record r where (r.concatenatedPartitionKey = '"
            + ANY_TEXT_1
            + "' and r.clusteringKey[\""
            + ANY_NAME_2
            + "\"] >= '"
            + ANY_TEXT_2
            + "') order by r.concatenatedPartitionKey asc, r.clusteringKey[\""
            + ANY_NAME_2
            + "\"] asc offset 0 limit "
            + ANY_LIMIT;

    // Act Assert
    assertThatCode(() -> handler.handle(scan)).doesNotThrowAnyException();

    // Assert
    verify(container).queryItems(eq(query), any(CosmosQueryRequestOptions.class), eq(Record.class));
  }

  @Test
  public void
      handle_ScanOperationWithReversedOrderingAndLimit_ShouldCallQueryItemsWithProperQuery() {
    // Arrange
    when(container.queryItems(anyString(), any(CosmosQueryRequestOptions.class), eq(Record.class)))
        .thenReturn(responseIterable);
    Record expected = new Record();
    when(responseIterable.iterator()).thenReturn(Collections.singletonList(expected).iterator());

    Scan scan =
        prepareScan()
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_2))
            .withOrdering(new Scan.Ordering(ANY_NAME_2, Order.DESC))
            .withLimit(ANY_LIMIT);

    String query =
        "select * from Record r where (r.concatenatedPartitionKey = '"
            + ANY_TEXT_1
            + "' and r.clusteringKey[\""
            + ANY_NAME_2
            + "\"] >= '"
            + ANY_TEXT_2
            + "') order by r.concatenatedPartitionKey desc, r.clusteringKey[\""
            + ANY_NAME_2
            + "\"] desc offset 0 limit "
            + ANY_LIMIT;

    // Act Assert
    assertThatCode(() -> handler.handle(scan)).doesNotThrowAnyException();

    // Assert
    verify(container).queryItems(eq(query), any(CosmosQueryRequestOptions.class), eq(Record.class));
  }

  @Test
  public void
      handle_ScanOperationWithMultipleOrderingsAndLimit_ShouldCallQueryItemsWithProperQuery() {
    // Arrange
    when(metadata.getClusteringKeyNames())
        .thenReturn(new LinkedHashSet<>(Arrays.asList(ANY_NAME_2, ANY_NAME_3)));
    when(metadata.getClusteringOrder(ANY_NAME_3)).thenReturn(Order.DESC);

    when(container.queryItems(anyString(), any(CosmosQueryRequestOptions.class), eq(Record.class)))
        .thenReturn(responseIterable);
    Record expected = new Record();
    when(responseIterable.iterator()).thenReturn(Collections.singletonList(expected).iterator());

    Scan scan =
        prepareScan()
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_2))
            .withOrdering(new Scan.Ordering(ANY_NAME_2, Order.ASC))
            .withOrdering(new Scan.Ordering(ANY_NAME_3, Order.DESC))
            .withLimit(ANY_LIMIT);

    String query =
        "select * from Record r where (r.concatenatedPartitionKey = '"
            + ANY_TEXT_1
            + "' and r.clusteringKey[\""
            + ANY_NAME_2
            + "\"] >= '"
            + ANY_TEXT_2
            + "') order by r.concatenatedPartitionKey asc, r.clusteringKey[\""
            + ANY_NAME_2
            + "\"] asc, r.clusteringKey[\""
            + ANY_NAME_3
            + "\"] desc offset 0 limit "
            + ANY_LIMIT;

    // Act Assert
    assertThatCode(() -> handler.handle(scan)).doesNotThrowAnyException();

    // Assert
    verify(container).queryItems(eq(query), any(CosmosQueryRequestOptions.class), eq(Record.class));
  }

  @Test
  public void
      handle_ScanOperationWithMultipleReversedOrderingsAndLimit_ShouldCallQueryItemsWithProperQuery() {
    // Arrange
    when(metadata.getClusteringKeyNames())
        .thenReturn(new LinkedHashSet<>(Arrays.asList(ANY_NAME_2, ANY_NAME_3)));
    when(metadata.getClusteringOrder(ANY_NAME_3)).thenReturn(Order.DESC);

    when(container.queryItems(anyString(), any(CosmosQueryRequestOptions.class), eq(Record.class)))
        .thenReturn(responseIterable);
    Record expected = new Record();
    when(responseIterable.iterator()).thenReturn(Collections.singletonList(expected).iterator());

    Scan scan =
        prepareScan()
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_2))
            .withOrdering(new Scan.Ordering(ANY_NAME_2, Order.DESC))
            .withOrdering(new Scan.Ordering(ANY_NAME_3, Order.ASC))
            .withLimit(ANY_LIMIT);

    String query =
        "select * from Record r where (r.concatenatedPartitionKey = '"
            + ANY_TEXT_1
            + "' and r.clusteringKey[\""
            + ANY_NAME_2
            + "\"] >= '"
            + ANY_TEXT_2
            + "') order by r.concatenatedPartitionKey desc, r.clusteringKey[\""
            + ANY_NAME_2
            + "\"] desc, r.clusteringKey[\""
            + ANY_NAME_3
            + "\"] asc offset 0 limit "
            + ANY_LIMIT;

    // Act Assert
    assertThatCode(() -> handler.handle(scan)).doesNotThrowAnyException();

    // Assert
    verify(container).queryItems(eq(query), any(CosmosQueryRequestOptions.class), eq(Record.class));
  }
}
