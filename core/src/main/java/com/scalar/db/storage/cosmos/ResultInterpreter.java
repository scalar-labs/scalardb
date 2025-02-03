package com.scalar.db.storage.cosmos;

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
import com.scalar.db.util.TimeRelatedColumnEncodingUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
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

  public Result interpret(Record record) {
    Map<String, Column<?>> ret = new HashMap<>();

    if (projections.isEmpty()) {
      metadata.getColumnNames().forEach(name -> add(ret, name, record, metadata));
    } else {
      projections.forEach(name -> add(ret, name, record, metadata));
    }

    return new ResultImpl(ret, metadata);
  }

  private void add(
      Map<String, Column<?>> columns, String name, Record record, TableMetadata metadata) {
    Object value;
    if (record.getPartitionKey().containsKey(name)) {
      value = record.getPartitionKey().get(name);
    } else if (record.getClusteringKey().containsKey(name)) {
      value = record.getClusteringKey().get(name);
    } else {
      value = record.getValues().get(name);
    }

    columns.put(name, convert(value, name, metadata.getColumnDataType(name)));
  }

  private Column<?> convert(@Nullable Object recordValue, String name, DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return recordValue == null
            ? BooleanColumn.ofNull(name)
            : BooleanColumn.of(name, (boolean) recordValue);
      case INT:
        return recordValue == null
            ? IntColumn.ofNull(name)
            : IntColumn.of(name, ((Number) recordValue).intValue());
      case BIGINT:
        return recordValue == null
            ? BigIntColumn.ofNull(name)
            : BigIntColumn.of(name, ((Number) recordValue).longValue());
      case FLOAT:
        return recordValue == null
            ? FloatColumn.ofNull(name)
            : FloatColumn.of(name, ((Number) recordValue).floatValue());
      case DOUBLE:
        return recordValue == null
            ? DoubleColumn.ofNull(name)
            : DoubleColumn.of(name, ((Number) recordValue).doubleValue());
      case TEXT:
        return recordValue == null
            ? TextColumn.ofNull(name)
            : TextColumn.of(name, (String) recordValue);
      case BLOB:
        return recordValue == null
            ? BlobColumn.ofNull(name)
            : BlobColumn.of(name, Base64.getDecoder().decode((String) recordValue));
      case DATE:
        return recordValue == null
            ? DateColumn.ofNull(name)
            : DateColumn.of(
                name, TimeRelatedColumnEncodingUtils.decodeDate(((Number) recordValue).intValue()));
      case TIME:
        return recordValue == null
            ? TimeColumn.ofNull(name)
            : TimeColumn.of(
                name,
                TimeRelatedColumnEncodingUtils.decodeTime(((Number) recordValue).longValue()));
      case TIMESTAMP:
        return recordValue == null
            ? TimestampColumn.ofNull(name)
            : TimestampColumn.of(
                name,
                TimeRelatedColumnEncodingUtils.decodeTimestamp(((Number) recordValue).longValue()));
      case TIMESTAMPTZ:
        return recordValue == null
            ? TimestampTZColumn.ofNull(name)
            : TimestampTZColumn.of(
                name,
                TimeRelatedColumnEncodingUtils.decodeTimestampTZ(
                    ((Number) recordValue).longValue()));
      default:
        throw new AssertionError();
    }
  }
}
