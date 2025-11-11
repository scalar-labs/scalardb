package com.scalar.db.storage.objectstorage;

import com.scalar.db.common.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Iterator that streams records from partitions in a lazy manner, loading partitions on-demand
 * instead of loading all records into memory at once.
 */
public class StreamingRecordIterator implements Iterator<ObjectStorageRecord> {
  private final ObjectStorageWrapper wrapper;
  private final String namespaceName;
  private final String tableName;
  private final Iterator<String> partitionKeyIterator;
  private Iterator<ObjectStorageRecord> partitionRecordIterator;

  public StreamingRecordIterator(
      ObjectStorageWrapper wrapper,
      String namespaceName,
      String tableName,
      List<String> partitionKeys) {
    this.wrapper = wrapper;
    this.namespaceName = namespaceName;
    this.tableName = tableName;
    this.partitionKeyIterator = partitionKeys.iterator();
    this.partitionRecordIterator = Collections.emptyIterator();
  }

  @Override
  public boolean hasNext() {
    while (!partitionRecordIterator.hasNext() && partitionKeyIterator.hasNext()) {
      loadNextPartition();
    }
    return partitionRecordIterator.hasNext();
  }

  @Override
  public ObjectStorageRecord next() {
    if (!hasNext()) {
      throw new java.util.NoSuchElementException();
    }
    return partitionRecordIterator.next();
  }

  private void loadNextPartition() {
    try {
      String partitionKey = partitionKeyIterator.next();
      ObjectStoragePartition partition = getPartition(partitionKey);
      partitionRecordIterator = partition.getRecords().values().iterator();
    } catch (ExecutionException e) {
      throw new RuntimeException(
          CoreError.OBJECT_STORAGE_ERROR_OCCURRED_IN_SELECTION.buildMessage(e.getMessage()), e);
    }
  }

  private ObjectStoragePartition getPartition(String partitionKey) throws ExecutionException {
    try {
      Optional<ObjectStorageWrapperResponse> response =
          wrapper.get(ObjectStorageUtils.getObjectKey(namespaceName, tableName, partitionKey));
      if (!response.isPresent()) {
        return new ObjectStoragePartition(Collections.emptyMap());
      }
      return ObjectStoragePartition.deserialize(response.get().getPayload());
    } catch (ObjectStorageWrapperException e) {
      throw new ExecutionException(
          CoreError.OBJECT_STORAGE_ERROR_OCCURRED_IN_SELECTION.buildMessage(e.getMessage()), e);
    }
  }
}
