package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
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
import com.azure.cosmos.models.CosmosStoredProcedureRequestOptions;
import com.azure.cosmos.models.CosmosStoredProcedureResponse;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.util.TableMetadataManager;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class PutStatementHandlerTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_NAME_4 = "name4";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final int ANY_INT_1 = 1;
  private static final int ANY_INT_2 = 2;

  private PutStatementHandler handler;
  @Mock private CosmosClient client;
  @Mock private CosmosDatabase database;
  @Mock private CosmosContainer container;
  @Mock private TableMetadataManager metadataManager;
  @Mock private TableMetadata metadata;
  @Mock private CosmosScripts cosmosScripts;
  @Mock private CosmosStoredProcedure storedProcedure;
  @Mock private CosmosStoredProcedureResponse spResponse;

  @Captor ArgumentCaptor<List<Object>> captor;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    handler = new PutStatementHandler(client, metadataManager);
    when(client.getDatabase(anyString())).thenReturn(database);
    when(database.getContainer(anyString())).thenReturn(container);

    when(metadataManager.getTableMetadata(any(Operation.class))).thenReturn(metadata);
    when(metadata.getPartitionKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_1)));
  }

  private Put preparePut() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Put(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME)
        .withValue(ANY_NAME_3, ANY_INT_1)
        .withValue(ANY_NAME_4, ANY_INT_2);
  }

  @Test
  public void handle_PutWithoutConditionsGiven_ShouldCallStoredProcedure() {
    // Arrange
    when(container.getScripts()).thenReturn(cosmosScripts);
    when(cosmosScripts.getStoredProcedure(anyString())).thenReturn(storedProcedure);
    when(storedProcedure.execute(anyList(), any(CosmosStoredProcedureRequestOptions.class)))
        .thenReturn(spResponse);
    when(spResponse.getResponseAsString()).thenReturn("true");

    Put put = preparePut();
    CosmosMutation cosmosMutation = new CosmosMutation(put, metadata);
    Record record = cosmosMutation.makeRecord();
    String query = cosmosMutation.makeConditionalQuery();

    // Act Assert
    assertThatCode(() -> handler.handle(put)).doesNotThrowAnyException();

    // Assert
    verify(cosmosScripts).getStoredProcedure("mutate.js");
    verify(storedProcedure)
        .execute(captor.capture(), any(CosmosStoredProcedureRequestOptions.class));
    assertThat(captor.getValue().get(0)).isEqualTo(1);
    assertThat(captor.getValue().get(1)).isEqualTo(CosmosMutation.MutationType.PUT.ordinal());
    assertThat(captor.getValue().get(2)).isEqualTo(record);
    assertThat(captor.getValue().get(3)).isEqualTo(query);
  }

  @Test
  public void handle_PutWithoutClusteringKeyGiven_ShouldCallStoredProcedure() {
    // Arrange
    when(container.getScripts()).thenReturn(cosmosScripts);
    when(cosmosScripts.getStoredProcedure(anyString())).thenReturn(storedProcedure);
    when(storedProcedure.execute(anyList(), any(CosmosStoredProcedureRequestOptions.class)))
        .thenReturn(spResponse);
    when(spResponse.getResponseAsString()).thenReturn("true");

    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Put put =
        new Put(partitionKey)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME)
            .withValue(ANY_NAME_3, ANY_INT_1)
            .withValue(ANY_NAME_4, ANY_INT_2);
    CosmosMutation cosmosMutation = new CosmosMutation(put, metadata);
    Record record = cosmosMutation.makeRecord();
    String query = cosmosMutation.makeConditionalQuery();

    // Act Assert
    assertThatCode(() -> handler.handle(put)).doesNotThrowAnyException();

    // Assert
    verify(cosmosScripts).getStoredProcedure("mutate.js");
    verify(storedProcedure)
        .execute(captor.capture(), any(CosmosStoredProcedureRequestOptions.class));
    assertThat(captor.getValue().get(0)).isEqualTo(1);
    assertThat(captor.getValue().get(1)).isEqualTo(CosmosMutation.MutationType.PUT.ordinal());
    assertThat(captor.getValue().get(2)).isEqualTo(record);
    assertThat(captor.getValue().get(3)).isEqualTo(query);
  }

  @Test
  public void handle_PutWithoutConditionsCosmosExceptionThrown_ShouldThrowExecutionException() {
    // Arrange
    when(container.getScripts()).thenReturn(cosmosScripts);
    when(cosmosScripts.getStoredProcedure(anyString())).thenReturn(storedProcedure);
    CosmosException toThrow = mock(CosmosException.class);
    doThrow(toThrow)
        .when(storedProcedure)
        .execute(anyList(), any(CosmosStoredProcedureRequestOptions.class));
    when(toThrow.getSubStatusCode()).thenReturn(CosmosErrorCode.PRECONDITION_FAILED.get());

    Put put = preparePut();

    // Act Assert
    assertThatThrownBy(() -> handler.handle(put)).isInstanceOf(NoMutationException.class);
  }

  @Test
  public void handle_PutIfNotExistsGiven_ShouldCallStoredProcedure() {
    // Arrange
    when(container.getScripts()).thenReturn(cosmosScripts);
    when(cosmosScripts.getStoredProcedure(anyString())).thenReturn(storedProcedure);
    when(storedProcedure.execute(anyList(), any(CosmosStoredProcedureRequestOptions.class)))
        .thenReturn(spResponse);
    when(spResponse.getResponseAsString()).thenReturn("true");

    Put put = preparePut().withCondition(new PutIfNotExists());
    CosmosMutation cosmosMutation = new CosmosMutation(put, metadata);
    Record record = cosmosMutation.makeRecord();
    String query = cosmosMutation.makeConditionalQuery();

    // Act Assert
    assertThatCode(() -> handler.handle(put)).doesNotThrowAnyException();

    // Assert
    verify(cosmosScripts).getStoredProcedure("mutate.js");
    verify(storedProcedure)
        .execute(captor.capture(), any(CosmosStoredProcedureRequestOptions.class));
    assertThat(captor.getValue().get(0)).isEqualTo(1);
    assertThat(captor.getValue().get(1))
        .isEqualTo(CosmosMutation.MutationType.PUT_IF_NOT_EXISTS.ordinal());
    assertThat(captor.getValue().get(2)).isEqualTo(record);
    assertThat(captor.getValue().get(3)).isEqualTo(query);
  }

  @Test
  public void handle_PutIfExistsGiven_ShouldCallStoredProcedure() {
    // Arrange
    when(container.getScripts()).thenReturn(cosmosScripts);
    when(cosmosScripts.getStoredProcedure(anyString())).thenReturn(storedProcedure);
    when(storedProcedure.execute(anyList(), any(CosmosStoredProcedureRequestOptions.class)))
        .thenReturn(spResponse);

    Put put = preparePut().withCondition(new PutIfExists());
    CosmosMutation cosmosMutation = new CosmosMutation(put, metadata);
    Record record = cosmosMutation.makeRecord();
    String query = cosmosMutation.makeConditionalQuery();

    // Act Assert
    assertThatCode(() -> handler.handle(put)).doesNotThrowAnyException();

    // Assert
    verify(cosmosScripts).getStoredProcedure("mutate.js");
    verify(storedProcedure)
        .execute(captor.capture(), any(CosmosStoredProcedureRequestOptions.class));
    assertThat(captor.getValue().get(0)).isEqualTo(1);
    assertThat(captor.getValue().get(1)).isEqualTo(CosmosMutation.MutationType.PUT_IF.ordinal());
    assertThat(captor.getValue().get(2)).isEqualTo(record);
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

    Put put = preparePut().withCondition(new PutIfExists());

    // Act Assert
    assertThatThrownBy(() -> handler.handle(put)).isInstanceOf(NoMutationException.class);
  }

  @Test
  public void handle_PutWithConditionCosmosExceptionThrown_ShouldThrowExecutionException() {
    // Arrange
    when(container.getScripts()).thenReturn(cosmosScripts);
    when(cosmosScripts.getStoredProcedure(anyString())).thenReturn(storedProcedure);
    CosmosException toThrow = mock(CosmosException.class);
    doThrow(toThrow)
        .when(storedProcedure)
        .execute(anyList(), any(CosmosStoredProcedureRequestOptions.class));
    when(toThrow.getSubStatusCode()).thenReturn(CosmosErrorCode.RETRY_WITH.get());

    Put put = preparePut().withCondition(new PutIfExists());

    // Act Assert
    assertThatThrownBy(() -> handler.handle(put))
        .isInstanceOf(RetriableExecutionException.class)
        .hasCause(toThrow);
  }
}
