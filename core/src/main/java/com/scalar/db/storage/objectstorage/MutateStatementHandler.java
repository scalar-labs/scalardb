package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.CoreError;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MutateStatementHandler extends StatementHandler {
  public MutateStatementHandler(
      ObjectStorageWrapper wrapper, TableMetadataManager metadataManager) {
    super(wrapper, metadataManager);
  }

  public void handle(Mutation mutation) throws ExecutionException {
    TableMetadata tableMetadata = metadataManager.getTableMetadata(mutation);
    ObjectStorageMutation objectStorageMutation =
        new ObjectStorageMutation(mutation, tableMetadata);
    String objectKey =
        ObjectStoragePartition.getObjectKey(
            getNamespace(mutation),
            getTable(mutation),
            objectStorageMutation.getConcatenatedPartitionKey());
    mutate(objectKey, Collections.singletonList(mutation));
  }

  public void handle(List<? extends Mutation> mutations) throws ExecutionException {
    Map<String, List<Mutation>> mutationPerPartition = new HashMap<>();
    for (Mutation mutation : mutations) {
      TableMetadata tableMetadata = metadataManager.getTableMetadata(mutation);
      ObjectStorageMutation objectStorageMutation =
          new ObjectStorageMutation(mutation, tableMetadata);
      String partitionKey = objectStorageMutation.getConcatenatedPartitionKey();
      String objectKey =
          ObjectStoragePartition.getObjectKey(
              getNamespace(mutation), getTable(mutation), partitionKey);
      mutationPerPartition.computeIfAbsent(objectKey, k -> new ArrayList<>()).add(mutation);
    }
    for (Map.Entry<String, List<Mutation>> entry : mutationPerPartition.entrySet()) {
      mutate(entry.getKey(), entry.getValue());
    }
  }

  private void mutate(String objectKey, List<Mutation> mutations) throws ExecutionException {
    ObjectStoragePartitionSnapshot snapshot = getPartition(objectKey);
    for (Mutation mutation : mutations) {
      TableMetadata tableMetadata = metadataManager.getTableMetadata(mutation);
      if (mutation instanceof Put) {
        snapshot.applyPut((Put) mutation, tableMetadata);
      } else {
        assert mutation instanceof Delete;
        snapshot.applyDelete((Delete) mutation, tableMetadata);
      }
    }
    writePartition(snapshot);
  }

  /**
   * Writes a partition to the object storage.
   *
   * @param snapshot the partition snapshot
   * @throws ExecutionException if a failure occurs during the operation
   */
  private void writePartition(ObjectStoragePartitionSnapshot snapshot) throws ExecutionException {
    try {
      if (snapshot.getReadVersion().isPresent()) {
        if (!snapshot.getPartition().isEmpty()) {
          wrapper.update(
              snapshot.getObjectKey(),
              snapshot.getPartition().serialize(),
              snapshot.getReadVersion().get());
        } else {
          wrapper.delete(snapshot.getObjectKey(), snapshot.getReadVersion().get());
        }
      } else {
        if (!snapshot.getPartition().isEmpty()) {
          wrapper.insert(snapshot.getObjectKey(), snapshot.getPartition().serialize());
        }
      }
    } catch (PreconditionFailedException e) {
      throw new RetriableExecutionException(
          CoreError.OBJECT_STORAGE_CONFLICT_OCCURRED_IN_MUTATION.buildMessage(e.getMessage()), e);
    } catch (ObjectStorageWrapperException e) {
      throw new ExecutionException(
          CoreError.OBJECT_STORAGE_ERROR_OCCURRED_IN_MUTATION.buildMessage(e.getMessage()), e);
    }
  }

  /**
   * Gets a partition and its version as a snapshot from the object storage.
   *
   * @param objectKey the object key
   * @return the partition
   * @throws ExecutionException if a failure occurs during the operation
   */
  private ObjectStoragePartitionSnapshot getPartition(String objectKey) throws ExecutionException {
    try {
      return wrapper
          .get(objectKey)
          .map(
              response ->
                  new ObjectStoragePartitionSnapshot(
                      objectKey, response.getPayload(), response.getVersion()))
          .orElseGet(
              () ->
                  new ObjectStoragePartitionSnapshot(
                      objectKey, new ObjectStoragePartition(null), null));
    } catch (ObjectStorageWrapperException e) {
      throw new ExecutionException(
          CoreError.OBJECT_STORAGE_ERROR_OCCURRED_IN_MUTATION.buildMessage(e.getMessage()), e);
    }
  }
}
