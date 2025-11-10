package com.scalar.db.storage.objectstorage;

import com.fasterxml.jackson.core.type.TypeReference;
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
import java.util.Optional;

public class MutateStatementHandler extends StatementHandler {
  public MutateStatementHandler(
      ObjectStorageWrapper wrapper, TableMetadataManager metadataManager) {
    super(wrapper, metadataManager);
  }

  public void handle(Mutation mutation) throws ExecutionException {
    TableMetadata tableMetadata = metadataManager.getTableMetadata(mutation);
    ObjectStorageMutation objectStorageMutation =
        new ObjectStorageMutation(mutation, tableMetadata);
    mutate(
        getNamespace(mutation),
        getTable(mutation),
        objectStorageMutation.getConcatenatedPartitionKey(),
        Collections.singletonList(mutation));
  }

  public void handle(List<? extends Mutation> mutations) throws ExecutionException {
    Map<PartitionIdentifier, List<Mutation>> mutationPerPartition = new HashMap<>();
    for (Mutation mutation : mutations) {
      TableMetadata tableMetadata = metadataManager.getTableMetadata(mutation);
      ObjectStorageMutation objectStorageMutation =
          new ObjectStorageMutation(mutation, tableMetadata);
      String partitionKey = objectStorageMutation.getConcatenatedPartitionKey();
      PartitionIdentifier partitionIdentifier =
          PartitionIdentifier.of(getNamespace(mutation), getTable(mutation), partitionKey);
      mutationPerPartition
          .computeIfAbsent(partitionIdentifier, k -> new ArrayList<>())
          .add(mutation);
    }
    for (Map.Entry<PartitionIdentifier, List<Mutation>> entry : mutationPerPartition.entrySet()) {
      PartitionIdentifier partitionIdentifier = entry.getKey();
      mutate(
          partitionIdentifier.getNamespaceName(),
          partitionIdentifier.getTableName(),
          partitionIdentifier.getPartitionName(),
          entry.getValue());
    }
  }

  private void mutate(
      String namespaceName, String tableName, String partitionKey, List<Mutation> mutations)
      throws ExecutionException {
    Map<PartitionIdentifier, String> readVersionMap = new HashMap<>();
    ObjectStoragePartition partition =
        getPartition(namespaceName, tableName, partitionKey, readVersionMap);
    for (Mutation mutation : mutations) {
      if (mutation instanceof Put) {
        partition.applyPut((Put) mutation, metadataManager.getTableMetadata(mutation));
      } else {
        assert mutation instanceof Delete;
        partition.applyDelete((Delete) mutation, metadataManager.getTableMetadata(mutation));
      }
    }
    applyPartitionWrite(namespaceName, tableName, partitionKey, partition, readVersionMap);
  }

  /**
   * Applies the partition write.
   *
   * @param namespaceName the namespace name
   * @param tableName the table name
   * @param partitionKey the partition key
   * @param partition the partition to be written
   * @param readVersionMap the map of read versions
   * @throws ExecutionException if a failure occurs during the operation
   */
  private void applyPartitionWrite(
      String namespaceName,
      String tableName,
      String partitionKey,
      ObjectStoragePartition partition,
      Map<PartitionIdentifier, String> readVersionMap)
      throws ExecutionException {
    if (readVersionMap.containsKey(partition.getPartitionIdentifier())) {
      String readVersion = readVersionMap.get(partition.getPartitionIdentifier());
      if (!partition.isEmpty()) {
        updatePartition(namespaceName, tableName, partitionKey, partition, readVersion);
      } else {
        deletePartition(namespaceName, tableName, partitionKey, readVersion);
      }
    } else {
      if (!partition.isEmpty()) {
        insertPartition(namespaceName, tableName, partitionKey, partition);
      }
    }
  }

  /**
   * Gets a partition from the object storage.
   *
   * @param namespaceName the namespace name
   * @param tableName the table name
   * @param partitionKey the partition key
   * @param readVersionMap the map to store the read version
   * @return the partition
   * @throws ExecutionException if a failure occurs during the operation
   */
  private ObjectStoragePartition getPartition(
      String namespaceName,
      String tableName,
      String partitionKey,
      Map<PartitionIdentifier, String> readVersionMap)
      throws ExecutionException {
    String objectKey = ObjectStorageUtils.getObjectKey(namespaceName, tableName, partitionKey);
    try {
      Optional<ObjectStorageWrapperResponse> response = wrapper.get(objectKey);
      if (!response.isPresent()) {
        return ObjectStoragePartition.newBuilder()
            .namespaceName(namespaceName)
            .tableName(tableName)
            .partitionKey(partitionKey)
            .build();
      }
      ObjectStoragePartition partition =
          Serializer.deserialize(
              response.get().getPayload(), new TypeReference<ObjectStoragePartition>() {});
      readVersionMap.put(partition.getPartitionIdentifier(), response.get().getVersion());
      return partition;
    } catch (ObjectStorageWrapperException e) {
      throw new ExecutionException(
          CoreError.OBJECT_STORAGE_ERROR_OCCURRED_IN_MUTATION.buildMessage(e.getMessage()), e);
    }
  }

  /**
   * Inserts a partition into the object storage. This method is called after confirming that the
   * partition does not exist.
   *
   * @param namespaceName the namespace name
   * @param tableName the table name
   * @param partitionKey the partition key
   * @param partition the partition to be inserted
   * @throws ExecutionException if a failure occurs during the operation
   */
  private void insertPartition(
      String namespaceName, String tableName, String partitionKey, ObjectStoragePartition partition)
      throws ExecutionException {
    try {
      wrapper.insert(
          ObjectStorageUtils.getObjectKey(namespaceName, tableName, partitionKey),
          Serializer.serialize(partition));
    } catch (PreconditionFailedException e) {
      throw new RetriableExecutionException(
          CoreError.OBJECT_STORAGE_CONFLICT_OCCURRED_IN_MUTATION.buildMessage(e.getMessage()), e);
    } catch (ObjectStorageWrapperException e) {
      throw new ExecutionException(
          CoreError.OBJECT_STORAGE_ERROR_OCCURRED_IN_MUTATION.buildMessage(e.getMessage()), e);
    }
  }

  /**
   * Updates a partition in the object storage. This method is called after confirming that the
   * partition exists.
   *
   * @param namespaceName the namespace name
   * @param tableName the table name
   * @param partitionKey the partition key
   * @param partition the partition to be updated
   * @param readVersion the read version
   * @throws ExecutionException if a failure occurs during the operation
   */
  private void updatePartition(
      String namespaceName,
      String tableName,
      String partitionKey,
      ObjectStoragePartition partition,
      String readVersion)
      throws ExecutionException {
    try {
      wrapper.update(
          ObjectStorageUtils.getObjectKey(namespaceName, tableName, partitionKey),
          Serializer.serialize(partition),
          readVersion);
    } catch (PreconditionFailedException e) {
      throw new RetriableExecutionException(
          CoreError.OBJECT_STORAGE_CONFLICT_OCCURRED_IN_MUTATION.buildMessage(e.getMessage()), e);
    } catch (ObjectStorageWrapperException e) {
      throw new ExecutionException(
          CoreError.OBJECT_STORAGE_ERROR_OCCURRED_IN_MUTATION.buildMessage(e.getMessage()), e);
    }
  }

  /**
   * Deletes a partition from the object storage. This method is called after confirming that the
   * partition exists.
   *
   * @param namespaceName the namespace name
   * @param tableName the table name
   * @param partitionKey the partition key
   * @param readVersion the read version
   * @throws ExecutionException if a failure occurs during the operation
   */
  private void deletePartition(
      String namespaceName, String tableName, String partitionKey, String readVersion)
      throws ExecutionException {
    try {
      wrapper.delete(
          ObjectStorageUtils.getObjectKey(namespaceName, tableName, partitionKey), readVersion);
    } catch (PreconditionFailedException e) {
      throw new RetriableExecutionException(
          CoreError.OBJECT_STORAGE_CONFLICT_OCCURRED_IN_MUTATION.buildMessage(e.getMessage()), e);
    } catch (ObjectStorageWrapperException e) {
      throw new ExecutionException(
          CoreError.OBJECT_STORAGE_ERROR_OCCURRED_IN_MUTATION.buildMessage(e.getMessage()), e);
    }
  }
}
