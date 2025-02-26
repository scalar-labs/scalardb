package com.scalar.db.storage.blob;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Column;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

@Immutable
public class BlobMutation extends BlobOperation {
  BlobMutation(Mutation mutation, TableMetadata metadata) {
    super(mutation, metadata);
  }

  @Nonnull
  public BlobRecord makeRecord() {
    Mutation mutation = (Mutation) getOperation();

    if (mutation instanceof Delete) {
      return new BlobRecord();
    }
    Put put = (Put) mutation;

    return new BlobRecord(
        getConcatenatedKey(),
        toMap(put.getPartitionKey().getColumns()),
        put.getClusteringKey().map(k -> toMap(k.getColumns())).orElse(Collections.emptyMap()),
        toMapForPut(put));
  }

  @Nonnull
  public BlobRecord makeRecord(BlobRecord existingRecord) {
    Mutation mutation = (Mutation) getOperation();

    if (mutation instanceof Delete) {
      return new BlobRecord();
    }
    Put put = (Put) mutation;

    BlobRecord newRecord = new BlobRecord(existingRecord);
    toMapForPut(put).forEach((k, v) -> newRecord.getValues().put(k, v));
    return newRecord;
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
