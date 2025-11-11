package com.scalar.db.storage.objectstorage;

import static com.scalar.db.storage.objectstorage.StatementHandler.validateConditions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

@SuppressFBWarnings("EI_EXPOSE_REP2")
public class ObjectStoragePartition {
  private final String namespaceName;
  private final String tableName;
  private final String partitionKey;
  private final Map<String, ObjectStorageRecord> records;

  @JsonCreator
  public ObjectStoragePartition(
      @JsonProperty("namespaceName") @Nullable String namespaceName,
      @JsonProperty("tableName") @Nullable String tableName,
      @JsonProperty("partitionKey") @Nullable String partitionKey,
      @JsonProperty("records") @Nullable Map<String, ObjectStorageRecord> records) {
    this.namespaceName = namespaceName != null ? namespaceName : "";
    this.tableName = tableName != null ? tableName : "";
    this.partitionKey = partitionKey != null ? partitionKey : "";
    this.records = records != null ? records : new HashMap<>();
  }

  public static ObjectStoragePartition.Builder newBuilder() {
    return new ObjectStoragePartition.Builder();
  }

  public PartitionIdentifier getPartitionIdentifier() {
    return PartitionIdentifier.of(namespaceName, tableName, partitionKey);
  }

  public Optional<ObjectStorageRecord> getRecord(String recordId) {
    return Optional.ofNullable(records.get(recordId));
  }

  public Map<String, ObjectStorageRecord> getRecords() {
    return Collections.unmodifiableMap(records);
  }

  public boolean isEmpty() {
    return records.isEmpty();
  }

  public void applyPut(Put put, TableMetadata tableMetadata) throws NoMutationException {
    ObjectStorageMutation mutation = new ObjectStorageMutation(put, tableMetadata);
    if (!put.getCondition().isPresent()) {
      if (!records.containsKey(mutation.getRecordId())) {
        records.put(mutation.getRecordId(), mutation.makeRecord());
      } else {
        records.compute(
            mutation.getRecordId(), (id, existingRecord) -> mutation.makeRecord(existingRecord));
      }
    } else if (put.getCondition().get() instanceof PutIfNotExists) {
      if (records.containsKey(mutation.getRecordId())) {
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage(), Collections.singletonList(put));
      }
      records.put(mutation.getRecordId(), mutation.makeRecord());
    } else if (put.getCondition().get() instanceof PutIfExists) {
      if (!records.containsKey(mutation.getRecordId())) {
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage(), Collections.singletonList(put));
      }
      records.compute(
          mutation.getRecordId(), (id, existingRecord) -> mutation.makeRecord(existingRecord));
    } else {
      assert put.getCondition().get() instanceof PutIf;
      if (!records.containsKey(mutation.getRecordId())) {
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage(), Collections.singletonList(put));
      }
      ObjectStorageRecord existingRecord = records.get(mutation.getRecordId());
      try {
        validateConditions(
            existingRecord, put.getCondition().get().getExpressions(), tableMetadata);
      } catch (ExecutionException e) {
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage(), Collections.singletonList(put), e);
      }
      records.put(mutation.getRecordId(), mutation.makeRecord(existingRecord));
    }
  }

  public void applyDelete(Delete delete, TableMetadata tableMetadata) throws NoMutationException {
    ObjectStorageMutation mutation = new ObjectStorageMutation(delete, tableMetadata);
    if (!delete.getCondition().isPresent()) {
      records.remove(mutation.getRecordId());
    } else if (delete.getCondition().get() instanceof DeleteIfExists) {
      if (!records.containsKey(mutation.getRecordId())) {
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage(), Collections.singletonList(delete));
      }
      records.remove(mutation.getRecordId());
    } else {
      assert delete.getCondition().get() instanceof DeleteIf;
      if (!records.containsKey(mutation.getRecordId())) {
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage(), Collections.singletonList(delete));
      }
      ObjectStorageRecord existingRecord = records.get(mutation.getRecordId());
      try {
        validateConditions(
            existingRecord, delete.getCondition().get().getExpressions(), tableMetadata);
      } catch (ExecutionException e) {
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage(), Collections.singletonList(delete), e);
      }
      records.remove(mutation.getRecordId());
    }
  }

  @VisibleForTesting
  protected void putRecord(String recordId, ObjectStorageRecord record) {
    records.put(recordId, record);
  }

  public static final class Builder {
    private String namespaceName;
    private String tableName;
    private String partitionKey;
    private Map<String, ObjectStorageRecord> records;

    public Builder namespaceName(String namespaceName) {
      this.namespaceName = namespaceName;
      return this;
    }

    public Builder tableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder partitionKey(String partitionKey) {
      this.partitionKey = partitionKey;
      return this;
    }

    public Builder records(Map<String, ObjectStorageRecord> records) {
      this.records = records;
      return this;
    }

    public ObjectStoragePartition build() {
      return new ObjectStoragePartition(namespaceName, tableName, partitionKey, records);
    }
  }
}
