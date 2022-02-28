package com.scalar.db.storage.cassandra;

import com.datastax.driver.core.Row;
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

  public Result interpret(Row row) {
    Map<String, Optional<Value<?>>> ret = new HashMap<>();
    if (projections.isEmpty()) {
      metadata
          .getColumnNames()
          .forEach(
              name ->
                  ret.put(
                      name,
                      Optional.ofNullable(convert(row, name, metadata.getColumnDataType(name)))));
    } else {
      projections.forEach(
          name ->
              ret.put(
                  name, Optional.ofNullable(convert(row, name, metadata.getColumnDataType(name)))));
    }
    return new ResultImpl(ret, metadata);
  }

  @Nullable
  private Value<?> convert(Row row, String name, DataType type) {
    if (row.isNull(name)) {
      return null;
    }
    switch (type) {
      case BOOLEAN:
        return new BooleanValue(name, row.getBool(name));
      case INT:
        return new IntValue(name, row.getInt(name));
      case BIGINT:
        return new BigIntValue(name, row.getLong(name));
      case FLOAT:
        return new FloatValue(name, row.getFloat(name));
      case DOUBLE:
        return new DoubleValue(name, row.getDouble(name));
      case TEXT:
        return new TextValue(name, row.getString(name));
      case BLOB:
        return new BlobValue(name, row.getBytes(name));
      default:
        throw new AssertionError();
    }
  }
}
