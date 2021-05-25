package com.scalar.db.storage.cassandra;

import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.UnsupportedTypeException;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import com.scalar.db.storage.common.ResultImpl;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class ResultInterpreter {

  private final TableMetadata metadata;

  public ResultInterpreter(TableMetadata metadata) {
    this.metadata = Objects.requireNonNull(metadata);
  }

  public Result interpret(Row row) {
    Map<String, Value<?>> ret = new HashMap<>();
    getColumnDefinitions(row)
        .forEach(
            (name, type) -> {
              Value<?> value = convert(row, name, type.getName());
              ret.put(name, value);
            });
    return new ResultImpl(ret, metadata);
  }

  @VisibleForTesting
  Map<String, DataType> getColumnDefinitions(Row row) {
    return row.getColumnDefinitions().asList().stream()
        .collect(Collectors.toMap(Definition::getName, Definition::getType, (d1, d2) -> d1));
  }

  private Value<?> convert(Row row, String name, DataType.Name type)
      throws UnsupportedTypeException {
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
      case TEXT: // for backwards compatibility
      case VARCHAR:
        return new TextValue(name, row.getString(name));
      case BLOB:
        ByteBuffer buffer = row.getBytes(name);
        return new BlobValue(name, buffer == null ? null : buffer.array());
      default:
        throw new UnsupportedTypeException(type.toString());
    }
  }
}
