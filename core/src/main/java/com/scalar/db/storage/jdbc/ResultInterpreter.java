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

  public Result interpret(ResultSet resultSet) throws SQLException {
    Map<String, Optional<Value<?>>> values = new HashMap<>();
    if (projections.isEmpty()) {
      for (String projection : metadata.getColumnNames()) {
        values.put(projection, Optional.ofNullable(getValue(projection, resultSet)));
      }
    } else {
      for (String projection : projections) {
        values.put(projection, Optional.ofNullable(getValue(projection, resultSet)));
      }
    }
    return new ResultImpl(values, metadata);
  }

  @Nullable
  private Value<?> getValue(String name, ResultSet resultSet) throws SQLException {
    Value<?> ret;

    DataType dataType = metadata.getColumnDataType(name);
    switch (dataType) {
      case BOOLEAN:
        ret = new BooleanValue(name, resultSet.getBoolean(name));
        break;
      case INT:
        ret = new IntValue(name, resultSet.getInt(name));
        break;
      case BIGINT:
        ret = new BigIntValue(name, resultSet.getLong(name));
        break;
      case FLOAT:
        // To handle Float.MAX_VALUE in MySQL, we need to get the value as double, then cast it to
        // float
        ret = new FloatValue(name, (float) resultSet.getDouble(name));
        break;
      case DOUBLE:
        ret = new DoubleValue(name, resultSet.getDouble(name));
        break;
      case TEXT:
        ret = new TextValue(name, resultSet.getString(name));
        break;
      case BLOB:
        ret = new BlobValue(name, resultSet.getBytes(name));
        break;
      default:
        throw new AssertionError();
    }
    if (resultSet.wasNull()) {
      return null;
    }
    return ret;
  }
}
