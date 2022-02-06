package com.scalar.db.storage.cosmos;

import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import com.scalar.db.util.ResultImpl;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class ResultInterpreter {

  private final List<String> projections;
  private final TableMetadata metadata;

  public ResultInterpreter(List<String> projections, TableMetadata metadata) {
    this.projections = Objects.requireNonNull(projections);
    this.metadata = Objects.requireNonNull(metadata);
  }

  public Result interpret(Record record) {
    Map<String, Optional<Value<?>>> ret = new HashMap<>();

    Map<String, Object> recordValues = record.getValues();
    if (projections.isEmpty()) {
      metadata.getColumnNames().forEach(name -> add(ret, name, recordValues.get(name), metadata));
    } else {
      // This isn't actual projection...
      projections.forEach(name -> add(ret, name, recordValues.get(name), metadata));
    }

    metadata
        .getPartitionKeyNames()
        .forEach(name -> add(ret, name, record.getPartitionKey().get(name), metadata));
    metadata
        .getClusteringKeyNames()
        .forEach(name -> add(ret, name, record.getClusteringKey().get(name), metadata));

    return new ResultImpl(ret, metadata);
  }

  private void add(
      Map<String, Optional<Value<?>>> values, String name, Object value, TableMetadata metadata) {
    values.put(name, Optional.ofNullable(convert(value, name, metadata.getColumnDataType(name))));
  }

  @Nullable
  private Value<?> convert(@Nullable Object recordValue, String name, DataType dataType) {
    if (recordValue == null) {
      return null;
    }
    switch (dataType) {
      case BOOLEAN:
        return new BooleanValue(name, (boolean) recordValue);
      case INT:
        return new IntValue(name, ((Number) recordValue).intValue());
      case BIGINT:
        return new BigIntValue(name, ((Number) recordValue).longValue());
      case FLOAT:
        return new FloatValue(name, ((Number) recordValue).floatValue());
      case DOUBLE:
        return new DoubleValue(name, ((Number) recordValue).doubleValue());
      case TEXT:
        return new TextValue(name, (String) recordValue);
      case BLOB:
        return new BlobValue(name, Base64.getDecoder().decode((String) recordValue));
      default:
        throw new AssertionError();
    }
  }
}
