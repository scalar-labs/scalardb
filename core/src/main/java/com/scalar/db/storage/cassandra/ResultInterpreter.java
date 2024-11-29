package com.scalar.db.storage.cassandra;

import com.datastax.driver.core.Row;
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
import com.scalar.db.io.TimestampTZColumn;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.LocalDate;
import java.time.LocalTime;
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

  public Result interpret(Row row) {
    Map<String, Column<?>> ret = new HashMap<>();
    if (projections.isEmpty()) {
      metadata
          .getColumnNames()
          .forEach(name -> ret.put(name, convert(row, name, metadata.getColumnDataType(name))));
    } else {
      projections.forEach(
          name -> ret.put(name, convert(row, name, metadata.getColumnDataType(name))));
    }
    return new ResultImpl(ret, metadata);
  }

  private Column<?> convert(Row row, String name, DataType type) {
    switch (type) {
      case BOOLEAN:
        return row.isNull(name)
            ? BooleanColumn.ofNull(name)
            : BooleanColumn.of(name, row.getBool(name));
      case INT:
        return row.isNull(name) ? IntColumn.ofNull(name) : IntColumn.of(name, row.getInt(name));
      case BIGINT:
        return row.isNull(name)
            ? BigIntColumn.ofNull(name)
            : BigIntColumn.of(name, row.getLong(name));
      case FLOAT:
        return row.isNull(name)
            ? FloatColumn.ofNull(name)
            : FloatColumn.of(name, row.getFloat(name));
      case DOUBLE:
        return row.isNull(name)
            ? DoubleColumn.ofNull(name)
            : DoubleColumn.of(name, row.getDouble(name));
      case TEXT:
        return row.isNull(name)
            ? TextColumn.ofNull(name)
            : TextColumn.of(name, row.getString(name));
      case BLOB:
        return row.isNull(name) ? BlobColumn.ofNull(name) : BlobColumn.of(name, row.getBytes(name));
      case DATE:
        return row.isNull(name)
            ? DateColumn.ofNull(name)
            : DateColumn.of(name, LocalDate.ofEpochDay(row.getDate(name).getDaysSinceEpoch()));
      case TIME:
        return row.isNull(name)
            ? TimeColumn.ofNull(name)
            : TimeColumn.of(name, LocalTime.ofNanoOfDay(row.getTime(name)));
      case TIMESTAMP:
        throw new UnsupportedOperationException(
            "The TIMESTAMP type is not supported with Cassandra.");
      case TIMESTAMPTZ:
        return row.isNull(name)
            ? TimestampTZColumn.ofNull(name)
            : TimestampTZColumn.of(name, row.getTimestamp(name).toInstant());
      default:
        throw new AssertionError();
    }
  }
}
