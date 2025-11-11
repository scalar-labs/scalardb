package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Put;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.NoMutationException;
import java.util.Optional;
import javax.annotation.Nullable;

public class ObjectStoragePartitionSnapshot {
  private final String namespaceName;
  private final String tableName;
  private final String partitionKey;
  private final ObjectStoragePartition partition;
  private final String readVersion;

  public ObjectStoragePartitionSnapshot(
      String objectKey, String serializedPartition, @Nullable String readVersion) {
    String[] parts = ObjectStorageUtils.parseObjectKey(objectKey);
    String namespaceName = parts[0];
    String tableName = parts[1];
    String partitionKey = parts[2];
    this.namespaceName = namespaceName;
    this.tableName = tableName;
    this.partitionKey = partitionKey;
    this.partition = ObjectStoragePartition.deserialize(serializedPartition);
    this.readVersion = readVersion;
  }

  public ObjectStoragePartitionSnapshot(
      String objectKey, ObjectStoragePartition partition, @Nullable String readVersion) {
    String[] parts = ObjectStorageUtils.parseObjectKey(objectKey);
    String namespaceName = parts[0];
    String tableName = parts[1];
    String partitionKey = parts[2];
    this.namespaceName = namespaceName;
    this.tableName = tableName;
    this.partitionKey = partitionKey;
    this.partition = partition;
    this.readVersion = readVersion;
  }

  public String getNamespaceName() {
    return namespaceName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getPartitionKey() {
    return partitionKey;
  }

  public ObjectStoragePartition getPartition() {
    return partition;
  }

  public Optional<String> getReadVersion() {
    return Optional.ofNullable(readVersion);
  }

  public String getObjectKey() {
    return ObjectStorageUtils.getObjectKey(namespaceName, tableName, partitionKey);
  }

  public void applyPut(Put put, TableMetadata tableMetadata) throws NoMutationException {
    partition.applyPut(put, tableMetadata);
  }

  public void applyDelete(Delete delete, TableMetadata tableMetadata) throws NoMutationException {
    partition.applyDelete(delete, tableMetadata);
  }
}
