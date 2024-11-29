package com.scalar.db.storage.dynamo;

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
import com.scalar.db.storage.ColumnSerializationUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@ThreadSafe
public class ResultInterpreter {

  private final List<String> projections;
  private final TableMetadata metadata;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public ResultInterpreter(List<String> projections, TableMetadata metadata) {
    this.projections = Objects.requireNonNull(projections);
    this.metadata = Objects.requireNonNull(metadata);
  }

  public Result interpret(Map<String, AttributeValue> item) {
    Map<String, Column<?>> ret = new HashMap<>();
    if (projections.isEmpty()) {
      metadata.getColumnNames().forEach(name -> add(ret, name, item.get(name)));
    } else {
      projections.forEach(name -> add(ret, name, item.get(name)));
    }
    return new ResultImpl(ret, metadata);
  }

  private void add(Map<String, Column<?>> columns, String name, AttributeValue itemValue) {
    columns.put(name, convert(itemValue, name, metadata.getColumnDataType(name)));
  }

  private Column<?> convert(@Nullable AttributeValue itemValue, String name, DataType dataType) {
    boolean isNull = itemValue == null || (itemValue.nul() != null && itemValue.nul());
    switch (dataType) {
      case BOOLEAN:
        return isNull ? BooleanColumn.ofNull(name) : BooleanColumn.of(name, itemValue.bool());
      case INT:
        return isNull
            ? IntColumn.ofNull(name)
            : IntColumn.of(name, Integer.parseInt(itemValue.n()));
      case BIGINT:
        return isNull
            ? BigIntColumn.ofNull(name)
            : BigIntColumn.of(name, Long.parseLong(itemValue.n()));
      case FLOAT:
        return isNull
            ? FloatColumn.ofNull(name)
            : FloatColumn.of(name, Float.parseFloat(itemValue.n()));
      case DOUBLE:
        return isNull
            ? DoubleColumn.ofNull(name)
            : DoubleColumn.of(name, Double.parseDouble(itemValue.n()));
      case TEXT:
        return isNull ? TextColumn.ofNull(name) : TextColumn.of(name, itemValue.s());
      case BLOB:
        return isNull ? BlobColumn.ofNull(name) : BlobColumn.of(name, itemValue.b().asByteArray());
      case DATE:
        return isNull
            ? DateColumn.ofNull(name)
            : DateColumn.of(
                name, ColumnSerializationUtils.parseCompactDate(Long.parseLong(itemValue.n())));
      case TIME:
        return isNull
            ? TimeColumn.ofNull(name)
            : TimeColumn.of(
                name, ColumnSerializationUtils.parseCompactTime(Long.parseLong(itemValue.n())));
      case TIMESTAMP:
        return isNull
            ? TimestampColumn.ofNull(name)
            : TimestampColumn.of(
                name, ColumnSerializationUtils.parseCompactTimestamp(itemValue.s()));
      case TIMESTAMPTZ:
        return isNull
            ? TimestampTZColumn.ofNull(name)
            : TimestampTZColumn.of(
                name, ColumnSerializationUtils.parseCompactTimestampTZ(itemValue.s()));
      default:
        throw new AssertionError();
    }
  }
}
