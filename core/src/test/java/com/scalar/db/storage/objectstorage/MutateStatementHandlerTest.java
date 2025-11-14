package com.scalar.db.storage.objectstorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.type.TypeReference;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class MutateStatementHandlerTest {
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
  private static final String VERSION = "version1";

  private MutateStatementHandler handler;
  @Mock private ObjectStorageWrapper wrapper;
  @Mock private TableMetadataManager metadataManager;
  @Mock private TableMetadata metadata;

  @Captor private ArgumentCaptor<String> objectKeyCaptor;
  @Captor private ArgumentCaptor<String> payloadCaptor;
  @Captor private ArgumentCaptor<String> versionCaptor;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    handler = new MutateStatementHandler(wrapper, metadataManager);

    when(metadataManager.getTableMetadata(any(Operation.class))).thenReturn(metadata);
    when(metadata.getPartitionKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_1)));
    when(metadata.getClusteringKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_2)));
    when(metadata.getColumnDataType(ANY_NAME_3)).thenReturn(DataType.INT);
    when(metadata.getColumnDataType(ANY_NAME_4)).thenReturn(DataType.INT);
  }

  private Put preparePut() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_2);
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .intValue(ANY_NAME_3, ANY_INT_1)
        .intValue(ANY_NAME_4, ANY_INT_2)
        .build();
  }

  private Put preparePutWithoutClusteringKey() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .intValue(ANY_NAME_3, ANY_INT_1)
        .intValue(ANY_NAME_4, ANY_INT_2)
        .build();
  }

  private Delete prepareDelete() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_2);
    return Delete.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .build();
  }

  private Delete prepareDeleteWithoutClusteringKey() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    return Delete.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .build();
  }

  private ObjectStorageRecord prepareExistingRecord() {
    Map<String, Object> values = new HashMap<>();
    values.put(ANY_NAME_3, ANY_INT_1);
    values.put(ANY_NAME_4, ANY_INT_2);
    return ObjectStorageRecord.newBuilder().id("concat_key").values(values).build();
  }

  private void setupNonExistentPartition() throws ObjectStorageWrapperException {
    when(wrapper.get(anyString())).thenReturn(Optional.empty());
  }

  private void setupPartitionWithRecord(String recordId) throws ObjectStorageWrapperException {
    Map<String, ObjectStorageRecord> records = new HashMap<>();
    records.put(recordId, prepareExistingRecord());
    ObjectStoragePartition partition = new ObjectStoragePartition(records);
    String serializedPartition = Serializer.serialize(partition);
    ObjectStorageWrapperResponse response =
        new ObjectStorageWrapperResponse(serializedPartition, VERSION);
    when(wrapper.get(anyString())).thenReturn(Optional.of(response));
  }

  private void setupPartitionWithRecords(String recordId, String... additionalRecordIds)
      throws ObjectStorageWrapperException {
    Map<String, ObjectStorageRecord> records = new HashMap<>();
    records.put(recordId, prepareExistingRecord());
    for (String additionalRecordId : additionalRecordIds) {
      records.put(additionalRecordId, prepareExistingRecord());
    }
    ObjectStoragePartition partition = new ObjectStoragePartition(records);
    String serializedPartition = Serializer.serialize(partition);
    ObjectStorageWrapperResponse response =
        new ObjectStorageWrapperResponse(serializedPartition, VERSION);
    when(wrapper.get(anyString())).thenReturn(Optional.of(response));
  }

  @Test
  public void handle_PutWithoutConditionsGiven_WhenPartitionDoesNotExist_ShouldCallWrapperInsert()
      throws Exception {
    // Arrange
    Put put = preparePut();
    ObjectStorageMutation mutation = new ObjectStorageMutation(put, metadata);
    String expectedObjectKey =
        ObjectStorageUtils.getObjectKey(
            ANY_NAMESPACE_NAME, ANY_TABLE_NAME, mutation.getConcatenatedPartitionKey());
    setupNonExistentPartition();

    // Act
    handler.handle(put);

    // Assert
    assert_Put_WhenPartitionDoesNotExist_ShouldCallWrapperInsert(
        expectedObjectKey, mutation.getRecordId());
  }

  @Test
  public void handle_PutWithoutConditionsGiven_WhenPartitionExists_ShouldCallWrapperUpdate()
      throws Exception {
    // Arrange
    Put put = preparePut();
    ObjectStorageMutation mutation = new ObjectStorageMutation(put, metadata);
    String expectedObjectKey =
        ObjectStorageUtils.getObjectKey(
            ANY_NAMESPACE_NAME, ANY_TABLE_NAME, mutation.getConcatenatedPartitionKey());
    setupPartitionWithRecord(mutation.getRecordId());

    // Act
    handler.handle(put);

    // Assert
    assert_Put_WhenPartitionExists_ShouldCallWrapperUpdate(
        expectedObjectKey, mutation.getRecordId());
  }

  @Test
  public void
      handle_PutWithoutClusteringKeyGiven_WhenPartitionDoesNotExist_ShouldCallWrapperInsert()
          throws Exception {
    // Arrange
    Put put = preparePutWithoutClusteringKey();
    ObjectStorageMutation mutation = new ObjectStorageMutation(put, metadata);
    String expectedObjectKey =
        ObjectStorageUtils.getObjectKey(
            ANY_NAMESPACE_NAME, ANY_TABLE_NAME, mutation.getConcatenatedPartitionKey());
    setupNonExistentPartition();

    // Act
    handler.handle(put);

    // Assert
    assert_Put_WhenPartitionDoesNotExist_ShouldCallWrapperInsert(
        expectedObjectKey, mutation.getRecordId());
  }

  @Test
  public void handle_PutWithoutClusteringKeyGiven_WhenPartitionExists_ShouldCallWrapperUpdate()
      throws Exception {
    // Arrange
    Put put = preparePutWithoutClusteringKey();
    ObjectStorageMutation mutation = new ObjectStorageMutation(put, metadata);
    String expectedObjectKey =
        ObjectStorageUtils.getObjectKey(
            ANY_NAMESPACE_NAME, ANY_TABLE_NAME, mutation.getConcatenatedPartitionKey());
    setupPartitionWithRecord(mutation.getRecordId());

    // Act
    handler.handle(put);

    // Assert
    verify(wrapper).get(objectKeyCaptor.capture());
    assertThat(objectKeyCaptor.getValue()).isEqualTo(expectedObjectKey);

    assert_Put_WhenPartitionExists_ShouldCallWrapperUpdate(
        expectedObjectKey, mutation.getRecordId());
  }

  @Test
  public void handle_PutWithoutConditionsWrapperExceptionThrown_ShouldThrowExecutionException()
      throws Exception {
    // Arrange
    Put put = preparePut();
    ObjectStorageWrapperException exception =
        new ObjectStorageWrapperException("Test error", new RuntimeException());
    when(wrapper.get(anyString())).thenThrow(exception);

    // Act & Assert
    assertThatThrownBy(() -> handler.handle(put))
        .isInstanceOf(ExecutionException.class)
        .hasCause(exception);
  }

  @Test
  public void handle_PutIfNotExistsGiven_WhenPartitionDoesNotExist_ShouldCallWrapperInsert()
      throws Exception {
    // Arrange
    Put put = Put.newBuilder(preparePut()).condition(ConditionBuilder.putIfNotExists()).build();
    ObjectStorageMutation mutation = new ObjectStorageMutation(put, metadata);
    String expectedObjectKey =
        ObjectStorageUtils.getObjectKey(
            ANY_NAMESPACE_NAME, ANY_TABLE_NAME, mutation.getConcatenatedPartitionKey());
    setupNonExistentPartition();

    // Act
    handler.handle(put);

    // Assert
    assert_Put_WhenPartitionDoesNotExist_ShouldCallWrapperInsert(
        expectedObjectKey, mutation.getRecordId());
  }

  @Test
  public void
      handle_PutIfNotExistsGiven_WhenPartitionExistsButRecordDoesNotExist_ShouldCallWrapperUpdate()
          throws Exception {
    // Arrange
    Put put = Put.newBuilder(preparePut()).condition(ConditionBuilder.putIfNotExists()).build();
    ObjectStorageMutation mutation = new ObjectStorageMutation(put, metadata);
    String expectedObjectKey =
        ObjectStorageUtils.getObjectKey(
            ANY_NAMESPACE_NAME, ANY_TABLE_NAME, mutation.getConcatenatedPartitionKey());
    setupPartitionWithRecord("another_record_key");

    // Act
    handler.handle(put);

    // Assert
    assert_Put_WhenPartitionExists_ShouldCallWrapperUpdate(
        expectedObjectKey, mutation.getRecordId());
  }

  @Test
  public void
      handle_PutIfNotExistsGiven_WhenPartitionAndRecordExist_ShouldThrowNoMutationException()
          throws Exception {
    // Arrange
    Put put = Put.newBuilder(preparePut()).condition(ConditionBuilder.putIfNotExists()).build();
    ObjectStorageMutation mutation = new ObjectStorageMutation(put, metadata);
    setupPartitionWithRecord(mutation.getRecordId());

    // Act & Assert
    assertThatThrownBy(() -> handler.handle(put)).isInstanceOf(NoMutationException.class);
  }

  @Test
  public void handle_PutIfExistsGiven_WhenPartitionDoesNotExist_ShouldThrowNoMutationException()
      throws Exception {
    // Arrange
    Put put = Put.newBuilder(preparePut()).condition(ConditionBuilder.putIfExists()).build();
    setupNonExistentPartition();

    // Act & Assert
    assertThatThrownBy(() -> handler.handle(put)).isInstanceOf(NoMutationException.class);
  }

  @Test
  public void
      handle_PutIfExistsGiven_WhenPartitionExistsButRecordDoesNotExist_ShouldThrowNoMutationException()
          throws Exception {
    // Arrange
    Put put = Put.newBuilder(preparePut()).condition(ConditionBuilder.putIfExists()).build();
    setupPartitionWithRecord("another_record_key");

    // Act & Assert
    assertThatThrownBy(() -> handler.handle(put)).isInstanceOf(NoMutationException.class);
  }

  @Test
  public void handle_PutIfExistsGiven_WhenPartitionAndRecordExist_ShouldCallWrapperUpdate()
      throws Exception {
    // Arrange
    Put put = Put.newBuilder(preparePut()).condition(ConditionBuilder.putIfExists()).build();
    ObjectStorageMutation mutation = new ObjectStorageMutation(put, metadata);
    String expectedObjectKey =
        ObjectStorageUtils.getObjectKey(
            ANY_NAMESPACE_NAME, ANY_TABLE_NAME, mutation.getConcatenatedPartitionKey());
    setupPartitionWithRecord(mutation.getRecordId());

    // Act
    handler.handle(put);

    // Assert
    assert_Put_WhenPartitionExists_ShouldCallWrapperUpdate(
        expectedObjectKey, mutation.getRecordId());
  }

  @Test
  public void
      handle_PutIfGiven_WhenConditionMatchesAndPartitionDoesNotExist_ShouldThrowNoMutationException()
          throws Exception {
    // Arrange
    Put put =
        Put.newBuilder(preparePut())
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column(ANY_NAME_3).isEqualToInt(ANY_INT_1))
                    .build())
            .build();
    setupNonExistentPartition();

    // Act & Assert
    assertThatThrownBy(() -> handler.handle(put)).isInstanceOf(NoMutationException.class);
  }

  @Test
  public void
      handle_PutIfGiven_WhenConditionMatchesAndPartitionExistsButRecordDoesNotExist_ShouldThrowNoMutationException()
          throws Exception {
    // Arrange
    Put put =
        Put.newBuilder(preparePut())
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column(ANY_NAME_3).isEqualToInt(ANY_INT_1))
                    .build())
            .build();
    setupPartitionWithRecord("another_record_key");

    // Act & Assert
    assertThatThrownBy(() -> handler.handle(put)).isInstanceOf(NoMutationException.class);
  }

  @Test
  public void
      handle_PutIfGiven_WhenConditionMatchesAndPartitionAndRecordExist_ShouldCallWrapperUpdate()
          throws Exception {
    // Arrange
    Put put =
        Put.newBuilder(preparePut())
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column(ANY_NAME_3).isEqualToInt(ANY_INT_1))
                    .build())
            .build();
    ObjectStorageMutation mutation = new ObjectStorageMutation(put, metadata);
    String expectedObjectKey =
        ObjectStorageUtils.getObjectKey(
            ANY_NAMESPACE_NAME, ANY_TABLE_NAME, mutation.getConcatenatedPartitionKey());
    setupPartitionWithRecord(mutation.getRecordId());

    // Act
    handler.handle(put);

    // Assert
    assert_Put_WhenPartitionExists_ShouldCallWrapperUpdate(
        expectedObjectKey, mutation.getRecordId());
  }

  @Test
  public void handle_PutIfGiven_WhenConditionDoesNotMatch_ShouldThrowNoMutationException()
      throws Exception {
    // Arrange
    Put put =
        Put.newBuilder(preparePut())
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column(ANY_NAME_3).isEqualToInt(999))
                    .build())
            .build();
    ObjectStorageMutation mutation = new ObjectStorageMutation(put, metadata);
    setupPartitionWithRecord(mutation.getRecordId());

    // Act & Assert
    assertThatThrownBy(() -> handler.handle(put)).isInstanceOf(NoMutationException.class);
  }

  private void assert_Put_WhenPartitionDoesNotExist_ShouldCallWrapperInsert(
      String expectedObjectKey, String expectedConcatenatedKey)
      throws ObjectStorageWrapperException {
    verify(wrapper).get(objectKeyCaptor.capture());
    assertThat(objectKeyCaptor.getValue()).isEqualTo(expectedObjectKey);

    verify(wrapper).insert(objectKeyCaptor.capture(), payloadCaptor.capture());
    assertThat(objectKeyCaptor.getValue()).isEqualTo(expectedObjectKey);

    ObjectStoragePartition insertedPartition =
        Serializer.deserialize(
            payloadCaptor.getValue(), new TypeReference<ObjectStoragePartition>() {});
    Optional<ObjectStorageRecord> record = insertedPartition.getRecord(expectedConcatenatedKey);
    assertThat(record).isPresent();
    assertThat(record.get().getValues())
        .containsEntry(ANY_NAME_3, ANY_INT_1)
        .containsEntry(ANY_NAME_4, ANY_INT_2);
  }

  private void assert_Put_WhenPartitionExists_ShouldCallWrapperUpdate(
      String expectedObjectKey, String expectedConcatenatedKey)
      throws ObjectStorageWrapperException {
    verify(wrapper)
        .update(objectKeyCaptor.capture(), payloadCaptor.capture(), versionCaptor.capture());
    assertThat(objectKeyCaptor.getValue()).isEqualTo(expectedObjectKey);

    ObjectStoragePartition updatedPartition =
        Serializer.deserialize(
            payloadCaptor.getValue(), new TypeReference<ObjectStoragePartition>() {});
    Optional<ObjectStorageRecord> record = updatedPartition.getRecord(expectedConcatenatedKey);
    assertThat(record).isPresent();
    assertThat(record.get().getValues())
        .containsEntry(ANY_NAME_3, ANY_INT_1)
        .containsEntry(ANY_NAME_4, ANY_INT_2);
    assertThat(versionCaptor.getValue()).isEqualTo(VERSION);
  }

  @Test
  public void
      handle_DeleteWithoutConditionsGiven_WhenNewPartitionIsNotEmpty_ShouldCallWrapperUpdate()
          throws Exception {
    // Arrange
    Delete delete = prepareDelete();
    ObjectStorageMutation mutation = new ObjectStorageMutation(delete, metadata);
    String expectedObjectKey =
        ObjectStorageUtils.getObjectKey(
            ANY_NAMESPACE_NAME, ANY_TABLE_NAME, mutation.getConcatenatedPartitionKey());
    String expectedExistingRecordKey = "existing_record_key";
    setupPartitionWithRecords(mutation.getRecordId(), expectedExistingRecordKey);

    // Act
    handler.handle(delete);

    // Assert
    assert_Delete_WhenNewPartitionIsNotEmpty_ShouldCallWrapperUpdate(
        expectedObjectKey, mutation.getConcatenatedPartitionKey(), expectedExistingRecordKey);
  }

  @Test
  public void handle_DeleteWithoutConditionsGiven_WhenNewPartitionIsEmpty_ShouldCallWrapperDelete()
      throws Exception {
    // Arrange
    Delete delete = prepareDelete();
    ObjectStorageMutation mutation = new ObjectStorageMutation(delete, metadata);
    String concatenatedPartitionKey = mutation.getConcatenatedPartitionKey();
    String expectedObjectKey =
        ObjectStorageUtils.getObjectKey(
            ANY_NAMESPACE_NAME, ANY_TABLE_NAME, concatenatedPartitionKey);
    setupPartitionWithRecord(mutation.getRecordId());

    // Act
    handler.handle(delete);

    // Assert
    assert_Delete_WhenNewPartitionIsEmpty_ShouldCallWrapperDelete(expectedObjectKey);
  }

  @Test
  public void
      handle_DeleteWithoutClusteringKeyGiven_WhenNewPartitionIsNotEmpty_ShouldCallWrapperUpdate()
          throws Exception {
    // Arrange
    Delete delete = prepareDeleteWithoutClusteringKey();
    ObjectStorageMutation mutation = new ObjectStorageMutation(delete, metadata);
    String concatenatedPartitionKey = mutation.getConcatenatedPartitionKey();
    String expectedObjectKey =
        ObjectStorageUtils.getObjectKey(
            ANY_NAMESPACE_NAME, ANY_TABLE_NAME, concatenatedPartitionKey);
    String expectedExistingRecordKey = "existing_record_key";
    setupPartitionWithRecords(mutation.getRecordId(), expectedExistingRecordKey);

    // Act
    handler.handle(delete);

    // Assert
    assert_Delete_WhenNewPartitionIsNotEmpty_ShouldCallWrapperUpdate(
        expectedObjectKey, mutation.getRecordId(), expectedExistingRecordKey);
  }

  @Test
  public void
      handle_DeleteWithoutClusteringKeyGiven_WhenNewPartitionIsEmpty_ShouldCallWrapperDelete()
          throws Exception {
    // Arrange
    Delete delete = prepareDeleteWithoutClusteringKey();
    ObjectStorageMutation mutation = new ObjectStorageMutation(delete, metadata);
    String concatenatedPartitionKey = mutation.getConcatenatedPartitionKey();
    String expectedObjectKey =
        ObjectStorageUtils.getObjectKey(
            ANY_NAMESPACE_NAME, ANY_TABLE_NAME, concatenatedPartitionKey);
    setupPartitionWithRecord(mutation.getRecordId());

    // Act
    handler.handle(delete);

    // Assert
    assert_Delete_WhenNewPartitionIsEmpty_ShouldCallWrapperDelete(expectedObjectKey);
  }

  @Test
  public void handle_DeleteWithoutConditionsWrapperExceptionThrown_ShouldThrowExecutionException()
      throws Exception {
    // Arrange
    Delete delete = prepareDelete();
    ObjectStorageWrapperException exception =
        new ObjectStorageWrapperException("Test error", new RuntimeException());
    when(wrapper.get(anyString())).thenThrow(exception);

    // Act & Assert
    assertThatThrownBy(() -> handler.handle(delete))
        .isInstanceOf(ExecutionException.class)
        .hasCause(exception);
  }

  @Test
  public void handle_DeleteIfExistsGiven_WhenNewPartitionIsNotEmpty_ShouldCallWrapperUpdate()
      throws Exception {
    // Arrange
    Delete delete =
        Delete.newBuilder(prepareDelete()).condition(ConditionBuilder.deleteIfExists()).build();
    ObjectStorageMutation mutation = new ObjectStorageMutation(delete, metadata);
    String concatenatedPartitionKey = mutation.getConcatenatedPartitionKey();
    String expectedObjectKey =
        ObjectStorageUtils.getObjectKey(
            ANY_NAMESPACE_NAME, ANY_TABLE_NAME, concatenatedPartitionKey);
    String expectedExistingRecordKey = "existing_record_key";
    setupPartitionWithRecords(mutation.getRecordId(), expectedExistingRecordKey);

    // Act
    handler.handle(delete);

    // Assert
    assert_Delete_WhenNewPartitionIsNotEmpty_ShouldCallWrapperUpdate(
        expectedObjectKey, mutation.getRecordId(), expectedExistingRecordKey);
  }

  @Test
  public void handle_DeleteIfExistsGiven_WhenNewPartitionIsEmpty_ShouldCallWrapperDelete()
      throws Exception {
    // Arrange
    Delete delete =
        Delete.newBuilder(prepareDelete()).condition(ConditionBuilder.deleteIfExists()).build();
    ObjectStorageMutation mutation = new ObjectStorageMutation(delete, metadata);
    String concatenatedPartitionKey = mutation.getConcatenatedPartitionKey();
    String expectedObjectKey =
        ObjectStorageUtils.getObjectKey(
            ANY_NAMESPACE_NAME, ANY_TABLE_NAME, concatenatedPartitionKey);
    setupPartitionWithRecord(mutation.getRecordId());

    // Act
    handler.handle(delete);

    // Assert
    assert_Delete_WhenNewPartitionIsEmpty_ShouldCallWrapperDelete(expectedObjectKey);
  }

  @Test
  public void handle_DeleteIfExistsGiven_WhenPartitionDoesNotExist_ShouldThrowNoMutationException()
      throws Exception {
    // Arrange
    Delete delete =
        Delete.newBuilder(prepareDelete()).condition(ConditionBuilder.deleteIfExists()).build();
    setupNonExistentPartition();

    // Act & Assert
    assertThatThrownBy(() -> handler.handle(delete)).isInstanceOf(NoMutationException.class);
  }

  @Test
  public void
      handle_DeleteIfExistsGiven_WhenPartitionExistsButRecordDoesNotExist_ShouldThrowNoMutationException()
          throws Exception {
    // Arrange
    Delete delete =
        Delete.newBuilder(prepareDelete()).condition(ConditionBuilder.deleteIfExists()).build();
    setupPartitionWithRecord("another_record_key");

    // Act & Assert
    assertThatThrownBy(() -> handler.handle(delete)).isInstanceOf(NoMutationException.class);
  }

  @Test
  public void
      handle_DeleteIfGiven_WhenConditionMatchesAndPartitionAndRecordExistAndNewPartitionIsNotEmpty_ShouldCallWrapperUpdate()
          throws Exception {
    // Arrange
    Delete delete =
        Delete.newBuilder(prepareDelete())
            .condition(
                ConditionBuilder.deleteIf(
                        ConditionBuilder.column(ANY_NAME_3).isEqualToInt(ANY_INT_1))
                    .build())
            .build();
    ObjectStorageMutation mutation = new ObjectStorageMutation(delete, metadata);
    String concatenatedPartitionKey = mutation.getConcatenatedPartitionKey();
    String expectedObjectKey =
        ObjectStorageUtils.getObjectKey(
            ANY_NAMESPACE_NAME, ANY_TABLE_NAME, concatenatedPartitionKey);
    String expectedExistingRecordKey = "existing_record_key";
    setupPartitionWithRecords(mutation.getRecordId(), expectedExistingRecordKey);

    // Act
    handler.handle(delete);

    // Assert
    assert_Delete_WhenNewPartitionIsNotEmpty_ShouldCallWrapperUpdate(
        expectedObjectKey, mutation.getRecordId(), expectedExistingRecordKey);
  }

  @Test
  public void
      handle_DeleteIfGiven_WhenConditionMatchesAndPartitionAndRecordExistAndPartitionIsEmpty_ShouldCallWrapperDelete()
          throws Exception {
    // Arrange
    Delete delete =
        Delete.newBuilder(prepareDelete())
            .condition(
                ConditionBuilder.deleteIf(
                        ConditionBuilder.column(ANY_NAME_3).isEqualToInt(ANY_INT_1))
                    .build())
            .build();
    ObjectStorageMutation mutation = new ObjectStorageMutation(delete, metadata);
    String concatenatedPartitionKey = mutation.getConcatenatedPartitionKey();
    String expectedObjectKey =
        ObjectStorageUtils.getObjectKey(
            ANY_NAMESPACE_NAME, ANY_TABLE_NAME, concatenatedPartitionKey);
    setupPartitionWithRecord(mutation.getRecordId());

    // Act
    handler.handle(delete);

    // Assert
    assert_Delete_WhenNewPartitionIsEmpty_ShouldCallWrapperDelete(expectedObjectKey);
  }

  @Test
  public void
      handle_DeleteIfGiven_WhenConditionMatchesAndPartitionExistsButRecordDoesNotExist_ShouldThrowNoMutationException()
          throws Exception {
    // Arrange
    Delete delete =
        Delete.newBuilder(prepareDelete())
            .condition(
                ConditionBuilder.deleteIf(
                        ConditionBuilder.column(ANY_NAME_3).isEqualToInt(ANY_INT_1))
                    .build())
            .build();
    setupPartitionWithRecord("another_record_key");

    // Act & Assert
    assertThatThrownBy(() -> handler.handle(delete)).isInstanceOf(NoMutationException.class);
  }

  @Test
  public void
      handle_DeleteIfGiven_WhenConditionMatchesAndPartitionDoesNotExist_ShouldThrowNoMutationException()
          throws Exception {
    // Arrange
    Delete delete =
        Delete.newBuilder(prepareDelete())
            .condition(
                ConditionBuilder.deleteIf(
                        ConditionBuilder.column(ANY_NAME_3).isEqualToInt(ANY_INT_1))
                    .build())
            .build();
    setupNonExistentPartition();

    // Act & Assert
    assertThatThrownBy(() -> handler.handle(delete)).isInstanceOf(NoMutationException.class);
  }

  @Test
  public void handle_DeleteIfGiven_WhenConditionDoesNotMatch_ShouldThrowNoMutationException()
      throws Exception {
    // Arrange
    Delete delete =
        Delete.newBuilder(prepareDelete())
            .condition(
                ConditionBuilder.deleteIf(ConditionBuilder.column(ANY_NAME_3).isEqualToInt(999))
                    .build())
            .build();
    ObjectStorageMutation mutation = new ObjectStorageMutation(delete, metadata);
    setupPartitionWithRecord(mutation.getRecordId());

    // Act & Assert
    assertThatThrownBy(() -> handler.handle(delete)).isInstanceOf(NoMutationException.class);
  }

  private void assert_Delete_WhenNewPartitionIsNotEmpty_ShouldCallWrapperUpdate(
      String expectedObjectKey, String expectedConcatenatedKey, String expectedExistingRecordKey)
      throws ObjectStorageWrapperException {
    verify(wrapper)
        .update(objectKeyCaptor.capture(), payloadCaptor.capture(), versionCaptor.capture());
    assertThat(objectKeyCaptor.getValue()).isEqualTo(expectedObjectKey);

    ObjectStoragePartition updatedPartition =
        Serializer.deserialize(
            payloadCaptor.getValue(), new TypeReference<ObjectStoragePartition>() {});
    assertThat(updatedPartition.getRecord(expectedConcatenatedKey)).isEmpty();
    assertThat(updatedPartition.getRecord(expectedExistingRecordKey)).isPresent();
  }

  private void assert_Delete_WhenNewPartitionIsEmpty_ShouldCallWrapperDelete(
      String expectedObjectKey) throws ObjectStorageWrapperException {
    verify(wrapper).delete(objectKeyCaptor.capture(), versionCaptor.capture());
    assertThat(objectKeyCaptor.getValue()).isEqualTo(expectedObjectKey);
    assertThat(versionCaptor.getValue()).isEqualTo(VERSION);
  }

  @Test
  public void handle_MultipleMutationsGiven_WhenPartitionDoesNotExist_ShouldCallWrapperInsert()
      throws Exception {
    // Arrange
    Put put1 = preparePut();
    Put put2 = Put.newBuilder(preparePut()).clusteringKey(Key.ofText(ANY_NAME_2, "put2")).build();
    Put put3 = Put.newBuilder(preparePut()).clusteringKey(Key.ofText(ANY_NAME_2, "put3")).build();
    Put put4 = Put.newBuilder(preparePut()).clusteringKey(Key.ofText(ANY_NAME_2, "put4")).build();
    ObjectStorageMutation mutation1 = new ObjectStorageMutation(put1, metadata);
    ObjectStorageMutation mutation2 = new ObjectStorageMutation(put2, metadata);
    ObjectStorageMutation mutation3 = new ObjectStorageMutation(put3, metadata);
    ObjectStorageMutation mutation4 = new ObjectStorageMutation(put4, metadata);
    String expectedObjectKey =
        ObjectStorageUtils.getObjectKey(
            ANY_NAMESPACE_NAME, ANY_TABLE_NAME, mutation1.getConcatenatedPartitionKey());
    setupNonExistentPartition();

    // Act
    handler.handle(Arrays.asList(put1, put2, put3, put4));

    // Assert
    verify(wrapper).get(objectKeyCaptor.capture());
    assertThat(objectKeyCaptor.getValue()).isEqualTo(expectedObjectKey);

    verify(wrapper).insert(objectKeyCaptor.capture(), payloadCaptor.capture());
    assertThat(objectKeyCaptor.getValue()).isEqualTo(expectedObjectKey);

    ObjectStoragePartition insertedPartition =
        Serializer.deserialize(
            payloadCaptor.getValue(), new TypeReference<ObjectStoragePartition>() {});
    Optional<ObjectStorageRecord> record1 = insertedPartition.getRecord(mutation1.getRecordId());
    assertThat(record1).isPresent();
    assertThat(record1.get().getValues())
        .containsEntry(ANY_NAME_3, ANY_INT_1)
        .containsEntry(ANY_NAME_4, ANY_INT_2);
    Optional<ObjectStorageRecord> record2 = insertedPartition.getRecord(mutation2.getRecordId());
    assertThat(record2).isPresent();
    assertThat(record2.get().getValues())
        .containsEntry(ANY_NAME_3, ANY_INT_1)
        .containsEntry(ANY_NAME_4, ANY_INT_2);
    Optional<ObjectStorageRecord> record3 = insertedPartition.getRecord(mutation3.getRecordId());
    assertThat(record3).isPresent();
    assertThat(record3.get().getValues())
        .containsEntry(ANY_NAME_3, ANY_INT_1)
        .containsEntry(ANY_NAME_4, ANY_INT_2);
    Optional<ObjectStorageRecord> record4 = insertedPartition.getRecord(mutation4.getRecordId());
    assertThat(record4).isPresent();
    assertThat(record4.get().getValues())
        .containsEntry(ANY_NAME_3, ANY_INT_1)
        .containsEntry(ANY_NAME_4, ANY_INT_2);
  }

  @Test
  public void handle_MultipleMutationsGiven_WhenPartitionExists_ShouldCallWrapperUpdate()
      throws Exception {
    // Arrange
    Put put1 = preparePut();
    Put put2 = Put.newBuilder(preparePut()).clusteringKey(Key.ofText(ANY_NAME_2, "put2")).build();
    Put put3 = Put.newBuilder(preparePut()).clusteringKey(Key.ofText(ANY_NAME_2, "put3")).build();
    Put put4 = Put.newBuilder(preparePut()).clusteringKey(Key.ofText(ANY_NAME_2, "put4")).build();
    ObjectStorageMutation mutation1 = new ObjectStorageMutation(put1, metadata);
    ObjectStorageMutation mutation2 = new ObjectStorageMutation(put2, metadata);
    ObjectStorageMutation mutation3 = new ObjectStorageMutation(put3, metadata);
    ObjectStorageMutation mutation4 = new ObjectStorageMutation(put4, metadata);
    String expectedObjectKey =
        ObjectStorageUtils.getObjectKey(
            ANY_NAMESPACE_NAME, ANY_TABLE_NAME, mutation1.getConcatenatedPartitionKey());
    setupPartitionWithRecords(
        mutation1.getRecordId(),
        mutation2.getRecordId(),
        mutation3.getRecordId(),
        mutation4.getRecordId());

    // Act
    handler.handle(Arrays.asList(put1, put2, put3, put4));

    // Assert
    verify(wrapper)
        .update(objectKeyCaptor.capture(), payloadCaptor.capture(), versionCaptor.capture());
    assertThat(objectKeyCaptor.getValue()).isEqualTo(expectedObjectKey);
    ObjectStoragePartition updatedPartition =
        Serializer.deserialize(
            payloadCaptor.getValue(), new TypeReference<ObjectStoragePartition>() {});
    Optional<ObjectStorageRecord> record1 = updatedPartition.getRecord(mutation1.getRecordId());
    assertThat(record1).isPresent();
    assertThat(record1.get().getValues())
        .containsEntry(ANY_NAME_3, ANY_INT_1)
        .containsEntry(ANY_NAME_4, ANY_INT_2);
    Optional<ObjectStorageRecord> record2 = updatedPartition.getRecord(mutation2.getRecordId());
    assertThat(record2).isPresent();
    assertThat(record2.get().getValues())
        .containsEntry(ANY_NAME_3, ANY_INT_1)
        .containsEntry(ANY_NAME_4, ANY_INT_2);
    Optional<ObjectStorageRecord> record3 = updatedPartition.getRecord(mutation3.getRecordId());
    assertThat(record3).isPresent();
    assertThat(record3.get().getValues())
        .containsEntry(ANY_NAME_3, ANY_INT_1)
        .containsEntry(ANY_NAME_4, ANY_INT_2);
    Optional<ObjectStorageRecord> record4 = updatedPartition.getRecord(mutation4.getRecordId());
    assertThat(record4).isPresent();
    assertThat(record4.get().getValues())
        .containsEntry(ANY_NAME_3, ANY_INT_1)
        .containsEntry(ANY_NAME_4, ANY_INT_2);
    assertThat(versionCaptor.getValue()).isEqualTo(VERSION);
  }
}
