package com.scalar.db.storage.objectstorage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.CoreError;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
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
    Map<String, ObjectStorageRecord> partition =
        getPartition(namespaceName, tableName, partitionKey, readVersionMap);
    for (Mutation mutation : mutations) {
      if (mutation instanceof Put) {
        putInternal(partition, (Put) mutation);
      } else {
        assert mutation instanceof Delete;
        deleteInternal(partition, (Delete) mutation);
      }
    }
    applyPartitionWrite(namespaceName, tableName, partitionKey, partition, readVersionMap);
  }

  private void putInternal(Map<String, ObjectStorageRecord> partition, Put put)
      throws ExecutionException {
    TableMetadata tableMetadata = metadataManager.getTableMetadata(put);
    ObjectStorageMutation mutation = new ObjectStorageMutation(put, tableMetadata);
    if (!put.getCondition().isPresent()) {
      ObjectStorageRecord existingRecord = partition.get(mutation.getRecordId());
      if (existingRecord == null) {
        partition.put(mutation.getRecordId(), mutation.makeRecord());
      } else {
        partition.put(mutation.getRecordId(), mutation.makeRecord(existingRecord));
      }
    } else if (put.getCondition().get() instanceof PutIfNotExists) {
      if (partition.containsKey(mutation.getRecordId())) {
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage(), Collections.singletonList(put));
      }
      partition.put(mutation.getRecordId(), mutation.makeRecord());
    } else if (put.getCondition().get() instanceof PutIfExists) {
      ObjectStorageRecord existingRecord = partition.get(mutation.getRecordId());
      if (existingRecord == null) {
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage(), Collections.singletonList(put));
      }
      partition.put(mutation.getRecordId(), mutation.makeRecord(existingRecord));
    } else {
      assert put.getCondition().get() instanceof PutIf;
      ObjectStorageRecord existingRecord = partition.get(mutation.getRecordId());
      if (existingRecord == null) {
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage(), Collections.singletonList(put));
      }
      try {
        validateConditions(
            partition.get(mutation.getRecordId()),
            put.getCondition().get().getExpressions(),
            metadataManager.getTableMetadata(mutation.getOperation()));
      } catch (ExecutionException e) {
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage(), Collections.singletonList(put), e);
      }
      partition.put(mutation.getRecordId(), mutation.makeRecord(existingRecord));
    }
  }

  private void deleteInternal(Map<String, ObjectStorageRecord> partition, Delete delete)
      throws ExecutionException {
    TableMetadata tableMetadata = metadataManager.getTableMetadata(delete);
    ObjectStorageMutation mutation = new ObjectStorageMutation(delete, tableMetadata);
    if (!delete.getCondition().isPresent()) {
      partition.remove(mutation.getRecordId());
    } else if (delete.getCondition().get() instanceof DeleteIfExists) {
      if (!partition.containsKey(mutation.getRecordId())) {
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage(), Collections.singletonList(delete));
      }
      partition.remove(mutation.getRecordId());
    } else {
      assert delete.getCondition().get() instanceof DeleteIf;
      if (!partition.containsKey(mutation.getRecordId())) {
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage(), Collections.singletonList(delete));
      }
      try {
        validateConditions(
            partition.get(mutation.getRecordId()),
            delete.getCondition().get().getExpressions(),
            metadataManager.getTableMetadata(mutation.getOperation()));
      } catch (ExecutionException e) {
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage(), Collections.singletonList(delete), e);
      }
      partition.remove(mutation.getRecordId());
    }
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
      Map<String, ObjectStorageRecord> partition,
      Map<PartitionIdentifier, String> readVersionMap)
      throws ExecutionException {
    if (readVersionMap.containsKey(
        PartitionIdentifier.of(namespaceName, tableName, partitionKey))) {
      String readVersion =
          readVersionMap.get(PartitionIdentifier.of(namespaceName, tableName, partitionKey));
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
  private Map<String, ObjectStorageRecord> getPartition(
      String namespaceName,
      String tableName,
      String partitionKey,
      Map<PartitionIdentifier, String> readVersionMap)
      throws ExecutionException {
    String objectKey = ObjectStorageUtils.getObjectKey(namespaceName, tableName, partitionKey);
    try {
      Optional<ObjectStorageWrapperResponse> response = wrapper.get(objectKey);
      if (!response.isPresent()) {
        return new HashMap<>();
      }
      readVersionMap.put(
          PartitionIdentifier.of(namespaceName, tableName, partitionKey),
          response.get().getVersion());
      return Serializer.deserialize(
          response.get().getPayload(), new TypeReference<Map<String, ObjectStorageRecord>>() {});
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
      String namespaceName,
      String tableName,
      String partitionKey,
      Map<String, ObjectStorageRecord> partition)
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
      Map<String, ObjectStorageRecord> partition,
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
