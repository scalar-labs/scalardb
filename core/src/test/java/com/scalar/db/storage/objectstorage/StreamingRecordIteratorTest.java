package com.scalar.db.storage.objectstorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class StreamingRecordIteratorTest {
  private static final String NAMESPACE = "test_namespace";
  private static final String TABLE = "test_table";
  private static final String PARTITION_KEY_1 = "partition1";
  private static final String PARTITION_KEY_2 = "partition2";
  private static final String PARTITION_KEY_3 = "partition3";
  private static final String VERSION = "version1";

  @Mock private ObjectStorageWrapper wrapper;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @Test
  public void hasNext_WhenNoPartitionKeys_ShouldReturnFalse() {
    // Arrange
    List<String> partitionKeys = Collections.emptyList();
    StreamingRecordIterator iterator =
        new StreamingRecordIterator(wrapper, NAMESPACE, TABLE, partitionKeys);

    // Act
    boolean hasNext = iterator.hasNext();

    // Assert
    assertThat(hasNext).isFalse();
  }

  @Test
  public void next_WhenNoPartitionKeys_ShouldThrowNoSuchElementException() {
    // Arrange
    List<String> partitionKeys = Collections.emptyList();
    StreamingRecordIterator iterator =
        new StreamingRecordIterator(wrapper, NAMESPACE, TABLE, partitionKeys);

    // Act & Assert
    assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void hasNext_WhenSinglePartitionWithRecords_ShouldReturnTrue() throws Exception {
    // Arrange
    List<String> partitionKeys = Collections.singletonList(PARTITION_KEY_1);
    Map<String, ObjectStorageRecord> records = createRecords(2);
    setupPartitionWithRecords(PARTITION_KEY_1, records);

    StreamingRecordIterator iterator =
        new StreamingRecordIterator(wrapper, NAMESPACE, TABLE, partitionKeys);

    // Act
    boolean hasNext = iterator.hasNext();

    // Assert
    assertThat(hasNext).isTrue();
  }

  @Test
  public void next_WhenSinglePartitionWithRecords_ShouldReturnAllRecords() throws Exception {
    // Arrange
    List<String> partitionKeys = Collections.singletonList(PARTITION_KEY_1);
    Map<String, ObjectStorageRecord> records = createRecords(2);
    setupPartitionWithRecords(PARTITION_KEY_1, records);

    StreamingRecordIterator iterator =
        new StreamingRecordIterator(wrapper, NAMESPACE, TABLE, partitionKeys);

    // Act
    List<ObjectStorageRecord> result = new ArrayList<>();
    while (iterator.hasNext()) {
      result.add(iterator.next());
    }

    // Assert
    assertThat(result).hasSize(2);
    assertThat(result).containsAll(records.values());
  }

  @Test
  public void next_WhenMultiplePartitionsWithRecords_ShouldReturnAllRecordsInOrder()
      throws Exception {
    // Arrange
    List<String> partitionKeys = Arrays.asList(PARTITION_KEY_1, PARTITION_KEY_2, PARTITION_KEY_3);
    Map<String, ObjectStorageRecord> records1 = createRecords(2, "record1_");
    Map<String, ObjectStorageRecord> records2 = createRecords(3, "record2_");
    Map<String, ObjectStorageRecord> records3 = createRecords(1, "record3_");

    setupPartitionWithRecords(PARTITION_KEY_1, records1);
    setupPartitionWithRecords(PARTITION_KEY_2, records2);
    setupPartitionWithRecords(PARTITION_KEY_3, records3);

    StreamingRecordIterator iterator =
        new StreamingRecordIterator(wrapper, NAMESPACE, TABLE, partitionKeys);

    // Act
    List<ObjectStorageRecord> result = new ArrayList<>();
    while (iterator.hasNext()) {
      result.add(iterator.next());
    }

    // Assert
    assertThat(result).hasSize(6);
    assertThat(result.subList(0, 2)).containsAll(records1.values());
    assertThat(result.subList(2, 5)).containsAll(records2.values());
    assertThat(result.subList(5, 6)).containsAll(records3.values());
  }

  @Test
  public void next_WhenPartitionDoesNotExist_ShouldSkipPartition() throws Exception {
    // Arrange
    List<String> partitionKeys = Arrays.asList(PARTITION_KEY_1, PARTITION_KEY_2);
    Map<String, ObjectStorageRecord> records = createRecords(2);

    setupNonExistentPartition();
    setupPartitionWithRecords(PARTITION_KEY_2, records);

    StreamingRecordIterator iterator =
        new StreamingRecordIterator(wrapper, NAMESPACE, TABLE, partitionKeys);

    // Act
    List<ObjectStorageRecord> result = new ArrayList<>();
    while (iterator.hasNext()) {
      result.add(iterator.next());
    }

    // Assert
    assertThat(result).hasSize(2);
    assertThat(result).containsAll(records.values());
  }

  @Test
  public void next_WhenPartitionHasNoRecords_ShouldSkipPartition() throws Exception {
    // Arrange
    List<String> partitionKeys = Arrays.asList(PARTITION_KEY_1, PARTITION_KEY_2);
    Map<String, ObjectStorageRecord> records = createRecords(2);

    setupPartitionWithRecords(PARTITION_KEY_1, Collections.emptyMap());
    setupPartitionWithRecords(PARTITION_KEY_2, records);

    StreamingRecordIterator iterator =
        new StreamingRecordIterator(wrapper, NAMESPACE, TABLE, partitionKeys);

    // Act
    List<ObjectStorageRecord> result = new ArrayList<>();
    while (iterator.hasNext()) {
      result.add(iterator.next());
    }

    // Assert
    assertThat(result).hasSize(2);
    assertThat(result).containsAll(records.values());
  }

  @Test
  public void hasNext_WhenCalledMultipleTimes_ShouldNotLoadPartitionMultipleTimes()
      throws Exception {
    // Arrange
    List<String> partitionKeys = Collections.singletonList(PARTITION_KEY_1);
    Map<String, ObjectStorageRecord> records = createRecords(1);
    setupPartitionWithRecords(PARTITION_KEY_1, records);

    StreamingRecordIterator iterator =
        new StreamingRecordIterator(wrapper, NAMESPACE, TABLE, partitionKeys);

    // Act
    boolean hasNext1 = iterator.hasNext();
    boolean hasNext2 = iterator.hasNext();
    boolean hasNext3 = iterator.hasNext();

    // Assert
    assertThat(hasNext1).isTrue();
    assertThat(hasNext2).isTrue();
    assertThat(hasNext3).isTrue();
    // wrapper.get should be called only once
    verify(wrapper, times(1)).get(anyString());
  }

  @Test
  public void next_WhenWrapperThrowsException_ShouldThrowRuntimeException() throws Exception {
    // Arrange
    List<String> partitionKeys = Collections.singletonList(PARTITION_KEY_1);
    ObjectStorageWrapperException exception = new ObjectStorageWrapperException("Test error");
    when(wrapper.get(anyString())).thenThrow(exception);

    StreamingRecordIterator iterator =
        new StreamingRecordIterator(wrapper, NAMESPACE, TABLE, partitionKeys);

    // Act & Assert
    assertThatThrownBy(iterator::hasNext).isInstanceOf(RuntimeException.class);
  }

  @Test
  public void hasNext_AfterConsumingAllRecords_ShouldReturnFalse() throws Exception {
    // Arrange
    List<String> partitionKeys = Collections.singletonList(PARTITION_KEY_1);
    Map<String, ObjectStorageRecord> records = createRecords(2);
    setupPartitionWithRecords(PARTITION_KEY_1, records);

    StreamingRecordIterator iterator =
        new StreamingRecordIterator(wrapper, NAMESPACE, TABLE, partitionKeys);

    // Act
    iterator.next();
    iterator.next();
    boolean hasNext = iterator.hasNext();

    // Assert
    assertThat(hasNext).isFalse();
  }

  @Test
  public void next_AfterConsumingAllRecords_ShouldThrowNoSuchElementException() throws Exception {
    // Arrange
    List<String> partitionKeys = Collections.singletonList(PARTITION_KEY_1);
    Map<String, ObjectStorageRecord> records = createRecords(1);
    setupPartitionWithRecords(PARTITION_KEY_1, records);

    StreamingRecordIterator iterator =
        new StreamingRecordIterator(wrapper, NAMESPACE, TABLE, partitionKeys);

    // Act
    iterator.next();

    // Assert
    assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
  }

  private Map<String, ObjectStorageRecord> createRecords(int count) {
    return createRecords(count, "record_");
  }

  private Map<String, ObjectStorageRecord> createRecords(int count, String prefix) {
    Map<String, ObjectStorageRecord> records = new HashMap<>();
    for (int i = 0; i < count; i++) {
      String recordId = prefix + i;
      ObjectStorageRecord record =
          ObjectStorageRecord.newBuilder()
              .id(recordId)
              .partitionKey(Collections.singletonMap("pk", "value"))
              .clusteringKey(Collections.singletonMap("ck", "value"))
              .values(Collections.singletonMap("col1", i))
              .build();
      records.put(recordId, record);
    }
    return records;
  }

  private void setupPartitionWithRecords(
      String partitionKey, Map<String, ObjectStorageRecord> records)
      throws ObjectStorageWrapperException {
    ObjectStoragePartition partition = new ObjectStoragePartition(records);
    String serializedPartition = Serializer.serialize(partition);
    ObjectStorageWrapperResponse response =
        new ObjectStorageWrapperResponse(serializedPartition, VERSION);
    String objectKey = ObjectStorageUtils.getObjectKey(NAMESPACE, TABLE, partitionKey);
    when(wrapper.get(objectKey)).thenReturn(Optional.of(response));
  }

  private void setupNonExistentPartition() throws ObjectStorageWrapperException {
    String objectKey =
        ObjectStorageUtils.getObjectKey(
            NAMESPACE, TABLE, StreamingRecordIteratorTest.PARTITION_KEY_1);
    when(wrapper.get(objectKey)).thenReturn(Optional.empty());
  }
}
