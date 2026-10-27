package com.scalar.db.storage.objectstorage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Ordering;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.CoreError;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.Column;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

@SuppressFBWarnings("EI_EXPOSE_REP2")
public class ObjectStoragePartition {
  private final Map<String, ObjectStorageRecord> records;

  @JsonCreator
  public ObjectStoragePartition(
      @JsonProperty("records") @Nullable Map<String, ObjectStorageRecord> records) {
    this.records = records != null ? records : new HashMap<>();
  }

  public static ObjectStoragePartition deserialize(String serializedObject) {
    return Serializer.deserialize(serializedObject, new TypeReference<ObjectStoragePartition>() {});
  }

  public static String getObjectKey(String namespaceName, String tableName, String partitionKey) {
    return ObjectStorageUtils.getObjectKey(namespaceName, tableName, partitionKey);
  }

  public String serialize() {
    return Serializer.serialize(this);
  }

  public Map<String, ObjectStorageRecord> getRecords() {
    return Collections.unmodifiableMap(records);
  }

  public Optional<ObjectStorageRecord> getRecord(String recordId) {
    return Optional.ofNullable(records.get(recordId));
  }

  public boolean isEmpty() {
    return records.isEmpty();
  }

  public void applyPut(Put put, TableMetadata tableMetadata) throws NoMutationException {
    ObjectStorageMutation mutation = new ObjectStorageMutation(put, tableMetadata);
    if (!put.getCondition().isPresent()) {
      ObjectStorageRecord existingRecord = records.get(mutation.getRecordId());
      if (existingRecord == null) {
        records.put(mutation.getRecordId(), mutation.makeRecord());
      } else {
        records.put(mutation.getRecordId(), mutation.makeRecord(existingRecord));
      }
    } else if (put.getCondition().get() instanceof PutIfNotExists) {
      if (records.containsKey(mutation.getRecordId())) {
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage(), Collections.singletonList(put));
      }
      records.put(mutation.getRecordId(), mutation.makeRecord());
    } else if (put.getCondition().get() instanceof PutIfExists) {
      ObjectStorageRecord existingRecord = records.get(mutation.getRecordId());
      if (existingRecord == null) {
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage(), Collections.singletonList(put));
      }
      records.put(mutation.getRecordId(), mutation.makeRecord(existingRecord));
    } else {
      assert put.getCondition().get() instanceof PutIf;
      if (!records.containsKey(mutation.getRecordId())) {
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage(), Collections.singletonList(put));
      }
      ObjectStorageRecord existingRecord = records.get(mutation.getRecordId());
      if (areConditionsMet(
          existingRecord, put.getCondition().get().getExpressions(), tableMetadata)) {
        records.put(mutation.getRecordId(), mutation.makeRecord(existingRecord));
        return;
      }
      throw new NoMutationException(
          CoreError.NO_MUTATION_APPLIED.buildMessage(), Collections.singletonList(put));
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
      if (areConditionsMet(
          existingRecord, delete.getCondition().get().getExpressions(), tableMetadata)) {
        records.remove(mutation.getRecordId());
        return;
      }
      throw new NoMutationException(
          CoreError.NO_MUTATION_APPLIED.buildMessage(), Collections.singletonList(delete));
    }
  }

  @VisibleForTesting
  protected void putRecord(String recordId, ObjectStorageRecord record) {
    records.put(recordId, record);
  }

  @VisibleForTesting
  protected boolean areConditionsMet(
      ObjectStorageRecord record, List<ConditionalExpression> expressions, TableMetadata metadata) {
    for (ConditionalExpression expression : expressions) {
      Column<?> expectedColumn = expression.getColumn();
      Column<?> actualColumn =
          ColumnValueMapper.convert(
              record.getValues().get(expectedColumn.getName()),
              expectedColumn.getName(),
              metadata.getColumnDataType(expectedColumn.getName()));
      switch (expression.getOperator()) {
        case EQ:
          if (actualColumn.hasNullValue()) {
            return false;
          }
          if (Ordering.natural().compare(actualColumn, expectedColumn) != 0) {
            return false;
          }
          break;
        case NE:
          if (actualColumn.hasNullValue()) {
            return false;
          }
          if (Ordering.natural().compare(actualColumn, expectedColumn) == 0) {
            return false;
          }
          break;
        case GT:
          if (actualColumn.hasNullValue()) {
            return false;
          }
          if (Ordering.natural().compare(actualColumn, expectedColumn) <= 0) {
            return false;
          }
          break;
        case GTE:
          if (actualColumn.hasNullValue()) {
            return false;
          }
          if (Ordering.natural().compare(actualColumn, expectedColumn) < 0) {
            return false;
          }
          break;
        case LT:
          if (actualColumn.hasNullValue()) {
            return false;
          }
          if (Ordering.natural().compare(actualColumn, expectedColumn) >= 0) {
            return false;
          }
          break;
        case LTE:
          if (actualColumn.hasNullValue()) {
            return false;
          }
          if (Ordering.natural().compare(actualColumn, expectedColumn) > 0) {
            return false;
          }
          break;
        case IS_NULL:
          if (!actualColumn.hasNullValue()) {
            return false;
          }
          break;
        case IS_NOT_NULL:
          if (actualColumn.hasNullValue()) {
            return false;
          }
          break;
        case LIKE:
        case NOT_LIKE:
        default:
          throw new AssertionError("Unsupported operator");
      }
    }
    return true;
  }
}
