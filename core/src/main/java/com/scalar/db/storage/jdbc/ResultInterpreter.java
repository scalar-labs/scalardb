package com.scalar.db.storage.jdbc;

import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
  private final RdbEngineStrategy<?, ?, ?, ?> rdbEngine;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public ResultInterpreter(
      List<String> projections, TableMetadata metadata, RdbEngineStrategy<?, ?, ?, ?> rdbEngine) {
    this.projections = Objects.requireNonNull(projections);
    this.metadata = Objects.requireNonNull(metadata);
    this.rdbEngine = rdbEngine;
  }

  public Result interpret(ResultSet resultSet) throws SQLException {
    Map<String, Column<?>> ret = new HashMap<>();
    if (projections.isEmpty()) {
      for (String projection : metadata.getColumnNames()) {
        ret.put(projection, convert(projection, resultSet));
      }
    } else {
      for (String projection : projections) {
        ret.put(projection, convert(projection, resultSet));
      }
    }
    return new ResultImpl(ret, metadata);
  }

  private Column<?> convert(String name, ResultSet resultSet) throws SQLException {
    Column<?> ret;

    DataType dataType = metadata.getColumnDataType(name);
    switch (dataType) {
      case BOOLEAN:
        ret = BooleanColumn.of(name, resultSet.getBoolean(name));
        if (resultSet.wasNull()) {
          ret = BooleanColumn.ofNull(name);
        }
        break;
      case INT:
        ret = IntColumn.of(name, resultSet.getInt(name));
        if (resultSet.wasNull()) {
          ret = IntColumn.ofNull(name);
        }
        break;
      case BIGINT:
        ret = BigIntColumn.of(name, resultSet.getLong(name));
        if (resultSet.wasNull()) {
          ret = BigIntColumn.ofNull(name);
        }
        break;
      case FLOAT:
        // To handle Float.MAX_VALUE in MySQL, we need to get the value as double, then cast it to
        // float
        ret = FloatColumn.of(name, (float) resultSet.getDouble(name));
        if (resultSet.wasNull()) {
          ret = FloatColumn.ofNull(name);
        }
        break;
      case DOUBLE:
        ret = DoubleColumn.of(name, resultSet.getDouble(name));
        if (resultSet.wasNull()) {
          ret = DoubleColumn.ofNull(name);
        }
        break;
      case TEXT:
        ret = TextColumn.of(name, resultSet.getString(name));
        if (resultSet.wasNull()) {
          ret = TextColumn.ofNull(name);
        }
        break;
      case BLOB:
        ret = BlobColumn.of(name, resultSet.getBytes(name));
        if (resultSet.wasNull()) {
          ret = BlobColumn.ofNull(name);
        }
        break;
      case DATE:
        ret = rdbEngine.parseDateColumn(resultSet, name);
        if (resultSet.wasNull()) {
          ret = DateColumn.ofNull(name);
        }
        break;
      case TIME:
        ret = rdbEngine.parseTimeColumn(resultSet, name);
        if (resultSet.wasNull()) {
          ret = TimeColumn.ofNull(name);
        }
        break;
      case TIMESTAMP:
        ret = rdbEngine.parseTimestampColumn(resultSet, name);
        if (resultSet.wasNull()) {
          ret = TimestampColumn.ofNull(name);
        }
        break;
      case TIMESTAMPTZ:
        ret = rdbEngine.parseTimestampTZColumn(resultSet, name);
        if (resultSet.wasNull()) {
          ret = TimestampTZColumn.ofNull(name);
        }
        break;
      default:
        throw new AssertionError();
    }
    return ret;
  }
}
