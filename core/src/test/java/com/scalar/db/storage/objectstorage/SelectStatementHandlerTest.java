package com.scalar.db.storage.objectstorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

  private SelectStatementHandler handler;
  @Mock private ObjectStorageWrapper wrapper;
  @Mock private TableMetadataManager metadataManager;
  @Mock private TableMetadata metadata;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    handler = new SelectStatementHandler(wrapper, metadataManager);

    when(metadataManager.getTableMetadata(any(Operation.class))).thenReturn(metadata);
    when(metadata.getPartitionKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_1)));
    when(metadata.getClusteringKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_2)));
    when(metadata.getSecondaryIndexNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_3)));
    when(metadata.getClusteringOrder(ANY_NAME_2)).thenReturn(Scan.Ordering.Order.ASC);
    when(metadata.getColumnDataType(anyString())).thenReturn(DataType.TEXT);
  }

  private Get prepareGet() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_2);
    return Get.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .build();
  }

  private Scan prepareScan() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    return Scan.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .build();
  }

  private Scan prepareScanAll() {
    return Scan.newBuilder().namespace(ANY_NAMESPACE_NAME).table(ANY_TABLE_NAME).all().build();
  }

  private ObjectStoragePartition createPartitionWithRecord() {
    Map<String, Object> partitionKey = Collections.singletonMap(ANY_NAME_1, ANY_TEXT_1);
    Map<String, Object> clusteringKey = Collections.singletonMap(ANY_NAME_2, ANY_TEXT_2);
    Map<String, Object> values = Collections.singletonMap(ANY_NAME_3, ANY_TEXT_3);
    ObjectStoragePartition partition = new ObjectStoragePartition(new HashMap<>());
    addRecordToPartition(partition, partitionKey, clusteringKey, values);
    return partition;
  }

  private ObjectStorageRecord createRecord(
      Map<String, Object> partitionKey,
      Map<String, Object> clusteringKey,
      Map<String, Object> values) {
    String recordId = buildRecordId(partitionKey, clusteringKey);
    return ObjectStorageRecord.newBuilder()
        .id(recordId)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .values(values)
        .build();
  }

  private void addRecordToPartition(
      ObjectStoragePartition partition,
      Map<String, Object> partitionKey,
      Map<String, Object> clusteringKey,
      Map<String, Object> values) {
    ObjectStorageRecord record = createRecord(partitionKey, clusteringKey, values);
    String recordId = buildRecordId(partitionKey, clusteringKey);
    partition.putRecord(recordId, record);
  }

  private String buildRecordId(
      Map<String, Object> partitionKey, Map<String, Object> clusteringKey) {
    String partitionKeyValue = (String) partitionKey.get(ANY_NAME_1);
    String clusteringKeyValue = (String) clusteringKey.get(ANY_NAME_2);
    return partitionKeyValue + ObjectStorageUtils.CONCATENATED_KEY_DELIMITER + clusteringKeyValue;
  }

  @Test
  public void handle_GetOperationGiven_ShouldReturnScanner() throws Exception {
    // Arrange
    Get get = prepareGet();
    ObjectStoragePartition partition = createPartitionWithRecord();
    String serialized = Serializer.serialize(partition);
    ObjectStorageWrapperResponse response =
        new ObjectStorageWrapperResponse(serialized, "version1");
    when(wrapper.get(anyString())).thenReturn(Optional.of(response));

    // Act
    Scanner scanner = handler.handle(get);

    // Assert
    assertThat(scanner).isNotNull();
    verify(wrapper)
        .get(ObjectStorageUtils.getObjectKey(ANY_NAMESPACE_NAME, ANY_TABLE_NAME, ANY_TEXT_1));
  }

  @Test
  public void handle_GetOperationWhenRecordNotFound_ShouldReturnEmptyScanner() throws Exception {
    // Arrange
    Get get = prepareGet();
    ObjectStoragePartition partition = new ObjectStoragePartition(new HashMap<>());
    String serialized = Serializer.serialize(partition);
    ObjectStorageWrapperResponse response =
        new ObjectStorageWrapperResponse(serialized, "version1");
    when(wrapper.get(anyString())).thenReturn(Optional.of(response));

    // Act
    Scanner scanner = handler.handle(get);

    // Assert
    assertThat(scanner).isNotNull();
    assertThat(scanner.all()).isEmpty();
  }

  @Test
  public void handle_GetOperationWhenPartitionNotFound_ShouldReturnEmptyScanner() throws Exception {
    // Arrange
    Get get = prepareGet();
    when(wrapper.get(anyString())).thenReturn(Optional.empty());

    // Act
    Scanner scanner = handler.handle(get);

    // Assert
    assertThat(scanner).isNotNull();
    assertThat(scanner.all()).isEmpty();
  }

  @Test
  public void handle_GetOperationWithSecondaryIndex_ShouldThrowUnsupportedOperationException() {
    // Arrange
    Key indexKey = Key.ofText(ANY_NAME_3, ANY_TEXT_3);
    Get get =
        Get.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(indexKey)
            .build();

    // Act Assert
    assertThatThrownBy(() -> handler.handle(get)).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void handle_GetOperationWhenExceptionThrown_ShouldThrowExecutionException()
      throws Exception {
    // Arrange
    Get get = prepareGet();
    when(wrapper.get(anyString()))
        .thenThrow(new ObjectStorageWrapperException("error", new RuntimeException()));

    // Act Assert
    assertThatThrownBy(() -> handler.handle(get)).isInstanceOf(ExecutionException.class);
  }

  @Test
  public void handle_ScanOperationGiven_ShouldReturnScanner() throws Exception {
    // Arrange
    Scan scan = prepareScan();
    ObjectStoragePartition partition = createPartitionWithRecord();
    String serialized = Serializer.serialize(partition);
    ObjectStorageWrapperResponse response =
        new ObjectStorageWrapperResponse(serialized, "version1");
    when(wrapper.get(anyString())).thenReturn(Optional.of(response));

    // Act
    Scanner scanner = handler.handle(scan);

    // Assert
    assertThat(scanner).isNotNull();
    verify(wrapper)
        .get(ObjectStorageUtils.getObjectKey(ANY_NAMESPACE_NAME, ANY_TABLE_NAME, ANY_TEXT_1));
  }

  @Test
  public void handle_ScanOperationWithSecondaryIndex_ShouldThrowUnsupportedOperationException() {
    // Arrange
    Key indexKey = Key.ofText(ANY_NAME_3, ANY_TEXT_3);
    Scan scan =
        Scan.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(indexKey)
            .build();

    // Act Assert
    assertThatThrownBy(() -> handler.handle(scan))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void handle_ScanOperationWhenExceptionThrown_ShouldThrowExecutionException()
      throws Exception {
    // Arrange
    Scan scan = prepareScan();
    when(wrapper.get(anyString()))
        .thenThrow(new ObjectStorageWrapperException("error", new RuntimeException()));

    // Act Assert
    assertThatThrownBy(() -> handler.handle(scan)).isInstanceOf(ExecutionException.class);
  }

  @Test
  public void handle_ScanOperationWithLimit_ShouldReturnLimitedResults() throws Exception {
    // Arrange
    Scan scan = Scan.newBuilder(prepareScan()).limit(1).build();
    ObjectStoragePartition partition = new ObjectStoragePartition(new HashMap<>());

    // Create multiple records
    for (int i = 0; i < 5; i++) {
      Map<String, Object> partitionKey = Collections.singletonMap(ANY_NAME_1, ANY_TEXT_1);
      Map<String, Object> clusteringKey = Collections.singletonMap(ANY_NAME_2, ANY_TEXT_2 + i);
      addRecordToPartition(partition, partitionKey, clusteringKey, new HashMap<>());
    }

    String serialized = Serializer.serialize(partition);
    ObjectStorageWrapperResponse response =
        new ObjectStorageWrapperResponse(serialized, "version1");
    when(wrapper.get(anyString())).thenReturn(Optional.of(response));

    // Act
    Scanner scanner = handler.handle(scan);

    // Assert
    assertThat(scanner).isNotNull();
    assertThat(scanner.all()).hasSize(1);
  }

  @Test
  public void handle_ScanAllOperationGiven_ShouldReturnScanner() throws Exception {
    // Arrange
    Scan scanAll = prepareScanAll();
    when(wrapper.getKeys(anyString()))
        .thenReturn(
            new HashSet<>(
                Arrays.asList(
                    ObjectStorageUtils.getObjectKey(ANY_NAMESPACE_NAME, ANY_TABLE_NAME, ANY_TEXT_1),
                    ObjectStorageUtils.getObjectKey(
                        ANY_NAMESPACE_NAME, ANY_TABLE_NAME, ANY_TEXT_2))));

    ObjectStoragePartition partition1 = createPartitionWithRecord();
    String serialized1 = Serializer.serialize(partition1);
    ObjectStorageWrapperResponse response1 =
        new ObjectStorageWrapperResponse(serialized1, "version1");

    ObjectStoragePartition partition2 = new ObjectStoragePartition(new HashMap<>());
    Map<String, Object> partitionKey2 = Collections.singletonMap(ANY_NAME_1, ANY_TEXT_2);
    Map<String, Object> clusteringKey2 = Collections.singletonMap(ANY_NAME_2, ANY_TEXT_3);
    addRecordToPartition(partition2, partitionKey2, clusteringKey2, new HashMap<>());
    String serialized2 = Serializer.serialize(partition2);
    ObjectStorageWrapperResponse response2 =
        new ObjectStorageWrapperResponse(serialized2, "version2");

    when(wrapper.get(
            ObjectStorageUtils.getObjectKey(ANY_NAMESPACE_NAME, ANY_TABLE_NAME, ANY_TEXT_1)))
        .thenReturn(Optional.of(response1));
    when(wrapper.get(
            ObjectStorageUtils.getObjectKey(ANY_NAMESPACE_NAME, ANY_TABLE_NAME, ANY_TEXT_2)))
        .thenReturn(Optional.of(response2));

    // Act
    Scanner scanner = handler.handle(scanAll);

    // Assert
    assertThat(scanner).isNotNull();
    assertThat(scanner.all()).hasSize(2);
  }

  @Test
  public void handle_ScanAllOperationWithLimit_ShouldReturnLimitedResults() throws Exception {
    // Arrange
    Scan scanAll = Scan.newBuilder(prepareScanAll()).limit(1).build();
    String objectKey1 =
        ObjectStorageUtils.getObjectKey(ANY_NAMESPACE_NAME, ANY_TABLE_NAME, ANY_TEXT_1);
    String objectKey2 =
        ObjectStorageUtils.getObjectKey(ANY_NAMESPACE_NAME, ANY_TABLE_NAME, ANY_TEXT_2);
    when(wrapper.getKeys(anyString()))
        .thenReturn(new HashSet<>(Arrays.asList(objectKey1, objectKey2)));

    ObjectStoragePartition partition1 = createPartitionWithRecord();
    String serialized1 = Serializer.serialize(partition1);
    ObjectStorageWrapperResponse response1 =
        new ObjectStorageWrapperResponse(serialized1, "version1");

    ObjectStoragePartition partition2 = new ObjectStoragePartition(new HashMap<>());
    Map<String, Object> partitionKey2 = Collections.singletonMap(ANY_NAME_1, ANY_TEXT_2);
    Map<String, Object> clusteringKey2 = Collections.singletonMap(ANY_NAME_2, ANY_TEXT_3);
    addRecordToPartition(partition2, partitionKey2, clusteringKey2, new HashMap<>());
    String serialized2 = Serializer.serialize(partition2);
    ObjectStorageWrapperResponse response2 =
        new ObjectStorageWrapperResponse(serialized2, "version2");

    when(wrapper.get(objectKey1)).thenReturn(Optional.of(response1));
    when(wrapper.get(objectKey2)).thenReturn(Optional.of(response2));

    // Act
    Scanner scanner = handler.handle(scanAll);

    // Assert
    assertThat(scanner).isNotNull();
    assertThat(scanner.all()).hasSize(1);
  }

  @Test
  public void handle_ScanAllOperationWhenExceptionThrown_ShouldThrowExecutionException()
      throws Exception {
    // Arrange
    Scan scanAll = prepareScanAll();
    when(wrapper.getKeys(anyString()))
        .thenThrow(new ObjectStorageWrapperException("error", new RuntimeException()));

    // Act Assert
    assertThatThrownBy(() -> handler.handle(scanAll)).isInstanceOf(ExecutionException.class);
  }

  @Test
  public void handle_ScanOperationWithStartClusteringKey_ShouldFilterResults() throws Exception {
    // Arrange
    Scan scan =
        Scan.newBuilder(prepareScan()).start(Key.ofText(ANY_NAME_2, ANY_TEXT_2 + "2")).build();
    ObjectStoragePartition partition = new ObjectStoragePartition(new HashMap<>());

    // Create multiple records with different clustering keys
    for (int i = 0; i < 5; i++) {
      Map<String, Object> partitionKey = Collections.singletonMap(ANY_NAME_1, ANY_TEXT_1);
      Map<String, Object> clusteringKey = Collections.singletonMap(ANY_NAME_2, ANY_TEXT_2 + i);
      addRecordToPartition(partition, partitionKey, clusteringKey, new HashMap<>());
    }

    String serialized = Serializer.serialize(partition);
    ObjectStorageWrapperResponse response =
        new ObjectStorageWrapperResponse(serialized, "version1");
    when(wrapper.get(anyString())).thenReturn(Optional.of(response));

    // Act
    Scanner scanner = handler.handle(scan);

    // Assert
    assertThat(scanner).isNotNull();
    // Should filter records with clustering key >= "text22"
    assertThat(scanner.all()).hasSizeGreaterThanOrEqualTo(1);
  }

  @Test
  public void handle_ScanOperationWithEndClusteringKey_ShouldFilterResults() throws Exception {
    // Arrange
    Scan scan =
        Scan.newBuilder(prepareScan()).end(Key.ofText(ANY_NAME_2, ANY_TEXT_2 + "2")).build();
    ObjectStoragePartition partition = new ObjectStoragePartition(new HashMap<>());

    // Create multiple records with different clustering keys
    for (int i = 0; i < 5; i++) {
      Map<String, Object> partitionKey = Collections.singletonMap(ANY_NAME_1, ANY_TEXT_1);
      Map<String, Object> clusteringKey = Collections.singletonMap(ANY_NAME_2, ANY_TEXT_2 + i);
      addRecordToPartition(partition, partitionKey, clusteringKey, new HashMap<>());
    }

    String serialized = Serializer.serialize(partition);
    ObjectStorageWrapperResponse response =
        new ObjectStorageWrapperResponse(serialized, "version1");
    when(wrapper.get(anyString())).thenReturn(Optional.of(response));

    // Act
    Scanner scanner = handler.handle(scan);

    // Assert
    assertThat(scanner).isNotNull();
    // Should filter records with clustering key <= "text22"
    assertThat(scanner.all()).hasSizeGreaterThanOrEqualTo(1);
  }

  @Test
  public void handle_ScanOperationWithDescOrdering_ShouldReverseResults() throws Exception {
    // Arrange
    when(metadata.getClusteringOrder(ANY_NAME_2)).thenReturn(Scan.Ordering.Order.ASC);
    Scan scan = Scan.newBuilder(prepareScan()).ordering(Scan.Ordering.desc(ANY_NAME_2)).build();
    ObjectStoragePartition partition = new ObjectStoragePartition(new HashMap<>());

    // Create multiple records
    for (int i = 0; i < 3; i++) {
      Map<String, Object> partitionKey = Collections.singletonMap(ANY_NAME_1, ANY_TEXT_1);
      Map<String, Object> clusteringKey = Collections.singletonMap(ANY_NAME_2, ANY_TEXT_2 + i);
      addRecordToPartition(partition, partitionKey, clusteringKey, new HashMap<>());
    }

    String serialized = Serializer.serialize(partition);
    ObjectStorageWrapperResponse response =
        new ObjectStorageWrapperResponse(serialized, "version1");
    when(wrapper.get(anyString())).thenReturn(Optional.of(response));

    // Act
    Scanner scanner = handler.handle(scan);

    // Assert
    assertThat(scanner).isNotNull();
    assertThat(scanner.all()).hasSize(3);
  }
}
