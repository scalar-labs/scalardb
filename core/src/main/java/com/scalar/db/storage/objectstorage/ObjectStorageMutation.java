package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Column;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

@Immutable
public class ObjectStorageMutation extends ObjectStorageOperation {
  ObjectStorageMutation(Mutation mutation, TableMetadata metadata) {
    super(mutation, metadata);
  }

  @Nonnull
  public ObjectStorageRecord makeRecord() {
    Mutation mutation = (Mutation) getOperation();

    if (mutation instanceof Delete) {
      throw new IllegalStateException("Delete mutation should not make a new record.");
    }
    Put put = (Put) getOperation();

    return ObjectStorageRecord.newBuilder()
        .id(getRecordId())
        .partitionKey(toMap(put.getPartitionKey().getColumns()))
        .clusteringKey(
            put.getClusteringKey().map(k -> toMap(k.getColumns())).orElse(Collections.emptyMap()))
        .values(toMapForPut(put))
        .build();
  }

  @Nonnull
  public ObjectStorageRecord makeRecord(ObjectStorageRecord existingRecord) {
    Mutation mutation = (Mutation) getOperation();

    if (mutation instanceof Delete) {
      throw new IllegalStateException("Delete mutation should not make a new record.");
    }
    Put put = (Put) mutation;

    Map<String, Object> newValues = new HashMap<>(existingRecord.getValues());
    newValues.putAll(toMapForPut(put));
    return ObjectStorageRecord.newBuilder()
        .id(existingRecord.getId())
        .partitionKey(existingRecord.getPartitionKey())
        .clusteringKey(existingRecord.getClusteringKey())
        .values(newValues)
        .build();
  }

  private Map<String, Object> toMap(Collection<Column<?>> columns) {
    MapVisitor visitor = new MapVisitor();
    columns.forEach(c -> c.accept(visitor));
    return visitor.get();
  }

  private Map<String, Object> toMapForPut(Put put) {
    MapVisitor visitor = new MapVisitor();
    put.getColumns().values().forEach(c -> c.accept(visitor));
    return visitor.get();
  }
}
