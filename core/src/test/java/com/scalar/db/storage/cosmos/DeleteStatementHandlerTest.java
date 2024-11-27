package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
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
import com.azure.cosmos.CosmosScripts;
import com.azure.cosmos.CosmosStoredProcedure;
import com.azure.cosmos.implementation.NotFoundException;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosStoredProcedureRequestOptions;
import com.azure.cosmos.models.CosmosStoredProcedureResponse;
import com.azure.cosmos.models.PartitionKey;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Operation;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.Key;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DeleteStatementHandlerTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";

  private DeleteStatementHandler handler;
  private String id;
  private PartitionKey cosmosPartitionKey;
  @Mock private CosmosClient client;
  @Mock private CosmosDatabase database;
  @Mock private CosmosContainer container;
  @Mock private TableMetadataManager metadataManager;
  @Mock private TableMetadata metadata;
  @Mock private CosmosItemResponse<Object> response;
  @Mock private CosmosScripts cosmosScripts;
  @Mock private CosmosStoredProcedure storedProcedure;
  @Mock private CosmosStoredProcedureResponse spResponse;

  @Captor ArgumentCaptor<List<Object>> captor;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    handler = new DeleteStatementHandler(client, metadataManager);
    when(client.getDatabase(anyString())).thenReturn(database);
    when(database.getContainer(anyString())).thenReturn(container);

    when(metadataManager.getTableMetadata(any(Operation.class))).thenReturn(metadata);
    when(metadata.getPartitionKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_1)));
    when(metadata.getClusteringKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_2)));
  }

  private Delete prepareDelete() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    id = ANY_TEXT_1 + ":" + ANY_TEXT_2;
    cosmosPartitionKey = new PartitionKey(ANY_TEXT_1);
    return new Delete(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  @Test
  public void handle_DeleteWithoutConditionsGiven_ShouldCallDeleteItem() {
    // Arrange
    when(container.deleteItem(
            anyString(), any(PartitionKey.class), any(CosmosItemRequestOptions.class)))
        .thenReturn(response);
    Delete delete = prepareDelete();

    // Act Assert
    assertThatCode(() -> handler.handle(delete)).doesNotThrowAnyException();

    // Assert
    verify(container)
        .deleteItem(eq(id), eq(cosmosPartitionKey), any(CosmosItemRequestOptions.class));
  }

  @Test
  public void handle_DeleteWithoutConditionsCosmosExceptionThrown_ShouldThrowExecutionException() {
    // Arrange
    CosmosException toThrow = mock(CosmosException.class);
    doThrow(toThrow)
        .when(container)
        .deleteItem(anyString(), any(PartitionKey.class), any(CosmosItemRequestOptions.class));

    Delete delete = prepareDelete();

    // Act Assert
    assertThatThrownBy(() -> handler.handle(delete))
        .isInstanceOf(ExecutionException.class)
        .hasCause(toThrow);
  }

  @Test
  public void handle_DeleteWithoutConditionsNotFoundExceptionThrown_ShouldNotThrowAnyException() {
    // Arrange
    doThrow(NotFoundException.class)
        .when(container)
        .deleteItem(anyString(), any(PartitionKey.class), any(CosmosItemRequestOptions.class));

    Delete delete = prepareDelete();

    // Act Assert
    assertThatCode(() -> handler.handle(delete)).doesNotThrowAnyException();
  }

  @Test
  public void handle_DeleteWithoutClusteringKeyGiven_ShouldCallStoredProcedure() {
    // Arrange
    when(metadata.getClusteringKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_2)));
    when(container.getScripts()).thenReturn(cosmosScripts);
    when(cosmosScripts.getStoredProcedure(anyString())).thenReturn(storedProcedure);
    when(storedProcedure.execute(anyList(), any(CosmosStoredProcedureRequestOptions.class)))
        .thenReturn(spResponse);

    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    cosmosPartitionKey = new PartitionKey(ANY_TEXT_1);
    Delete delete =
        new Delete(partitionKey).forNamespace(ANY_NAMESPACE_NAME).forTable(ANY_TABLE_NAME);

    CosmosMutation cosmosMutation = new CosmosMutation(delete, metadata);
    String query = cosmosMutation.makeConditionalQuery();

    // Act Assert
    assertThatCode(() -> handler.handle(delete)).doesNotThrowAnyException();

    // Assert
    verify(cosmosScripts).getStoredProcedure("mutate.js");
    verify(storedProcedure)
        .execute(captor.capture(), any(CosmosStoredProcedureRequestOptions.class));
    assertThat(captor.getValue().get(0)).isEqualTo(1);
    assertThat(captor.getValue().get(1)).isEqualTo(CosmosMutation.MutationType.DELETE_IF.ordinal());
    assertThat(captor.getValue().get(2)).isEqualTo(new Record());
    assertThat(captor.getValue().get(3)).isEqualTo(query);
  }

  @Test
  public void handle_DeleteWithConditionsGiven_ShouldCallStoredProcedure() {
    // Arrange
    when(container.getScripts()).thenReturn(cosmosScripts);
    when(cosmosScripts.getStoredProcedure(anyString())).thenReturn(storedProcedure);
    when(storedProcedure.execute(anyList(), any(CosmosStoredProcedureRequestOptions.class)))
        .thenReturn(spResponse);

    Delete delete = prepareDelete().withCondition(new DeleteIfExists());
    CosmosMutation cosmosMutation = new CosmosMutation(delete, metadata);
    String query = cosmosMutation.makeConditionalQuery();

    // Act Assert
    assertThatCode(() -> handler.handle(delete)).doesNotThrowAnyException();

    // Assert
    verify(cosmosScripts).getStoredProcedure("mutate.js");
    verify(storedProcedure)
        .execute(captor.capture(), any(CosmosStoredProcedureRequestOptions.class));
    assertThat(captor.getValue().get(0)).isEqualTo(1);
    assertThat(captor.getValue().get(1)).isEqualTo(CosmosMutation.MutationType.DELETE_IF.ordinal());
    assertThat(captor.getValue().get(2)).isEqualTo(new Record());
    assertThat(captor.getValue().get(3)).isEqualTo(query);
  }

  @Test
  public void handle_CosmosExceptionWithPreconditionFailed_ShouldThrowNoMutationException() {
    // Arrange
    when(container.getScripts()).thenReturn(cosmosScripts);
    when(cosmosScripts.getStoredProcedure(anyString())).thenReturn(storedProcedure);
    CosmosException toThrow = mock(CosmosException.class);
    doThrow(toThrow)
        .when(storedProcedure)
        .execute(anyList(), any(CosmosStoredProcedureRequestOptions.class));
    when(toThrow.getSubStatusCode()).thenReturn(CosmosErrorCode.PRECONDITION_FAILED.get());

    Delete delete = prepareDelete().withCondition(new DeleteIfExists());

    // Act Assert
    assertThatThrownBy(() -> handler.handle(delete)).isInstanceOf(NoMutationException.class);
  }

  @Test
  public void handle_DeleteWithConditionCosmosExceptionThrown_ShouldThrowExecutionException() {
    // Arrange
    when(container.getScripts()).thenReturn(cosmosScripts);
    when(cosmosScripts.getStoredProcedure(anyString())).thenReturn(storedProcedure);
    CosmosException toThrow = mock(CosmosException.class);
    doThrow(toThrow)
        .when(storedProcedure)
        .execute(anyList(), any(CosmosStoredProcedureRequestOptions.class));

    Delete delete = prepareDelete().withCondition(new DeleteIfExists());

    // Act Assert
    assertThatThrownBy(() -> handler.handle(delete))
        .isInstanceOf(ExecutionException.class)
        .hasCause(toThrow);
  }
}
