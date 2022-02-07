package com.scalar.db.storage.jdbc;

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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class ResultInterpreter {

  private final List<String> projections;
  private final TableMetadata metadata;

  public ResultInterpreter(List<String> projections, TableMetadata metadata) {
    this.projections = Objects.requireNonNull(projections);
    this.metadata = Objects.requireNonNull(metadata);
  }

  public Result interpret(ResultSet resultSet) throws SQLException {
    Map<String, Value<?>> values = new HashMap<>();
    if (projections.isEmpty()) {
      for (String projection : metadata.getColumnNames()) {
        values.put(projection, getValue(projection, resultSet));
      }
    } else {
      for (String projection : projections) {
        values.put(projection, getValue(projection, resultSet));
      }
    }
    return new ResultImpl(values, metadata);
  }

  private Value<?> getValue(String name, ResultSet resultSet) throws SQLException {
    DataType dataType = metadata.getColumnDataType(name);
    switch (dataType) {
      case BOOLEAN:
        return new BooleanValue(name, resultSet.getBoolean(name));
      case INT:
        return new IntValue(name, resultSet.getInt(name));
      case BIGINT:
        return new BigIntValue(name, resultSet.getLong(name));
      case FLOAT:
        // To handle Float.MAX_VALUE in MySQL, we need to get the value as double, then cast it to
        // float
        return new FloatValue(name, (float) resultSet.getDouble(name));
      case DOUBLE:
        return new DoubleValue(name, resultSet.getDouble(name));
      case TEXT:
        return new TextValue(name, resultSet.getString(name));
      case BLOB:
        return new BlobValue(name, resultSet.getBytes(name));
      default:
        throw new AssertionError();
    }
  }
}
