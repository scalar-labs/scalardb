package com.scalar.db.storage.cosmos;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.CosmosScripts;
import com.azure.cosmos.CosmosStoredProcedure;
import com.azure.cosmos.models.CosmosStoredProcedureRequestOptions;
import com.azure.cosmos.models.CosmosStoredProcedureResponse;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BatchHandlerTest {
  private static final String ANY_KEYSPACE_NAME = "keyspace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANOTHER_TABLE_NAME = "another_table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_NAME_4 = "name4";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text1";
  private static final String ANY_TEXT_4 = "text2";
  private static final int ANY_INT_1 = 1;
  private static final int ANY_INT_2 = 2;

  private BatchHandler handler;
  @Mock private CosmosClient client;
  @Mock private CosmosDatabase database;
  @Mock private CosmosContainer container;
  @Mock private TableMetadataManager metadataManager;
  @Mock private CosmosTableMetadata metadata;
  @Mock private CosmosScripts cosmosScripts;
  @Mock private CosmosStoredProcedure storedProcedure;
  @Mock private CosmosStoredProcedureResponse spResponse;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    handler = new BatchHandler(client, metadataManager);
    when(client.getDatabase(anyString())).thenReturn(database);
    when(database.getContainer(anyString())).thenReturn(container);

    when(metadataManager.getTableMetadata(any(Operation.class))).thenReturn(metadata);
    when(metadata.getPartitionKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_1)));
    when(metadata.getKeyNames()).thenReturn(Arrays.asList(ANY_NAME_1, ANY_NAME_2));
  }

  private Put preparePut() {
    Key partitionKey = new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    Key clusteringKey = new Key(new TextValue(ANY_NAME_2, ANY_TEXT_2));
    Put put =
        new Put(partitionKey, clusteringKey)
            .forNamespace(ANY_KEYSPACE_NAME)
            .forTable(ANY_TABLE_NAME)
            .withValue(new IntValue(ANY_NAME_3, ANY_INT_1))
            .withValue(new IntValue(ANY_NAME_4, ANY_INT_2));

    return put;
  }

  private Delete prepareDelete() {
    Key partitionKey = new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    Key clusteringKey = new Key(new TextValue(ANY_NAME_2, ANY_TEXT_2));
    Delete del =
        new Delete(partitionKey, clusteringKey)
            .forNamespace(ANY_KEYSPACE_NAME)
            .forTable(ANY_TABLE_NAME);
    return del;
  }

  @Test
  public void handle_MultipleMutationsGiven_ShouldCallStoredProcedure() {
    // Arrange
    when(container.getScripts()).thenReturn(cosmosScripts);
    when(cosmosScripts.getStoredProcedure(anyString())).thenReturn(storedProcedure);
    when(storedProcedure.execute(any(List.class), any(CosmosStoredProcedureRequestOptions.class)))
        .thenReturn(spResponse);

    Put put1 = preparePut();
    Put put2 = preparePut().withCondition(new PutIfNotExists());
    Delete delete1 = prepareDelete();
    Delete delete2 = prepareDelete().withCondition(new DeleteIfExists());
    CosmosMutation cosmosMutation1 = new CosmosMutation(put1, metadataManager);
    CosmosMutation cosmosMutation2 = new CosmosMutation(put2, metadataManager);
    CosmosMutation cosmosMutation3 = new CosmosMutation(delete1, metadataManager);
    CosmosMutation cosmosMutation4 = new CosmosMutation(delete2, metadataManager);
    Record record1 = cosmosMutation1.makeRecord();
    Record record2 = cosmosMutation2.makeRecord();
    Record emptyRecord = new Record();
    String query1 = cosmosMutation1.makeConditionalQuery();
    String query2 = cosmosMutation2.makeConditionalQuery();
    String query3 = cosmosMutation3.makeConditionalQuery();
    String query4 = cosmosMutation4.makeConditionalQuery();

    // Act Assert
    assertThatCode(
            () -> {
              handler.handle(Arrays.asList(put1, put2, delete1, delete2));
            })
        .doesNotThrowAnyException();

    // Assert
    verify(cosmosScripts).getStoredProcedure("mutate.js");
    ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
    verify(storedProcedure)
        .execute(captor.capture(), any(CosmosStoredProcedureRequestOptions.class));
    assertThat(captor.getValue().get(0)).isEqualTo(4);

    assertThat(captor.getValue().get(1)).isEqualTo(CosmosMutation.MutationType.PUT.ordinal());
    assertThat(captor.getValue().get(2))
        .isEqualTo(CosmosMutation.MutationType.PUT_IF_NOT_EXISTS.ordinal());
    assertThat(captor.getValue().get(3)).isEqualTo(CosmosMutation.MutationType.DELETE_IF.ordinal());
    assertThat(captor.getValue().get(4)).isEqualTo(CosmosMutation.MutationType.DELETE_IF.ordinal());

    assertThat(captor.getValue().get(5)).isEqualTo(record1);
    assertThat(captor.getValue().get(6)).isEqualTo(record2);
    assertThat(captor.getValue().get(7)).isEqualTo(emptyRecord);
    assertThat(captor.getValue().get(8)).isEqualTo(emptyRecord);

    assertThat(captor.getValue().get(9)).isEqualTo(query1);
    assertThat(captor.getValue().get(10)).isEqualTo(query2);
    assertThat(captor.getValue().get(11)).isEqualTo(query3);
    assertThat(captor.getValue().get(12)).isEqualTo(query4);
  }

  @Test
  public void handle_CosmosExceptionWithPreconditionFailed_ShouldThrowNoMutationException() {
    when(container.getScripts()).thenReturn(cosmosScripts);
    when(cosmosScripts.getStoredProcedure(anyString())).thenReturn(storedProcedure);
    CosmosException toThrow = mock(CosmosException.class);
    doThrow(toThrow)
        .when(storedProcedure)
        .execute(any(List.class), any(CosmosStoredProcedureRequestOptions.class));
    when(toThrow.getSubStatusCode()).thenReturn(CosmosErrorCode.PRECONDITION_FAILED.get());

    Put put = preparePut().withCondition(new PutIfNotExists());
    Delete delete = prepareDelete().withCondition(new DeleteIfExists());

    // Act Assert
    assertThatThrownBy(
            () -> {
              handler.handle(Arrays.asList(put, delete));
            })
        .isInstanceOf(NoMutationException.class);
  }

  @Test
  public void handle_CosmosExceptionThrown_ShouldThrowExecutionException() {
    // Arrange
    when(container.getScripts()).thenReturn(cosmosScripts);
    when(cosmosScripts.getStoredProcedure(anyString())).thenReturn(storedProcedure);
    CosmosException toThrow = mock(CosmosException.class);
    doThrow(toThrow)
        .when(storedProcedure)
        .execute(any(List.class), any(CosmosStoredProcedureRequestOptions.class));

    Put put1 = preparePut();
    Put put2 = preparePut();

    // Act Assert
    assertThatThrownBy(
            () -> {
              handler.handle(Arrays.asList(put1, put2));
            })
        .isInstanceOf(ExecutionException.class)
        .hasCause(toThrow);
  }
}
