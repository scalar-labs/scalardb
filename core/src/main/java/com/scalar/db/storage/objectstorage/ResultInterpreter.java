package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.io.Column;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class ResultInterpreter {
  private final List<String> projections;
  private final TableMetadata metadata;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public ResultInterpreter(List<String> projections, TableMetadata metadata) {
    this.projections = Objects.requireNonNull(projections);
    this.metadata = Objects.requireNonNull(metadata);
  }

  public Result interpret(ObjectStorageRecord record) {
    Map<String, Column<?>> ret = new HashMap<>();

    if (projections.isEmpty()) {
      metadata.getColumnNames().forEach(name -> add(ret, name, record, metadata));
    } else {
      projections.forEach(name -> add(ret, name, record, metadata));
    }

    return new ResultImpl(ret, metadata);
  }

  private void add(
      Map<String, Column<?>> columns,
      String name,
      ObjectStorageRecord record,
      TableMetadata metadata) {
    Object value;
    if (record.getPartitionKey().containsKey(name)) {
      value = record.getPartitionKey().get(name);
    } else if (record.getClusteringKey().containsKey(name)) {
      value = record.getClusteringKey().get(name);
    } else {
      value = record.getValues().get(name);
    }

    columns.put(name, ColumnValueMapper.convert(value, name, metadata.getColumnDataType(name)));
  }
}
