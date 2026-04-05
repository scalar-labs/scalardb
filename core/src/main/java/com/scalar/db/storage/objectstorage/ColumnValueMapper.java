package com.scalar.db.storage.objectstorage;

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
import java.nio.ByteBuffer;
import javax.annotation.Nullable;

public class ColumnValueMapper {
  public static Column<?> convert(@Nullable Object recordValue, String name, DataType dataType) {
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
        if (recordValue == null) {
          return BlobColumn.ofNull(name);
        } else if (recordValue instanceof byte[]) {
          return BlobColumn.of(name, (byte[]) recordValue);
        } else if (recordValue instanceof ByteBuffer) {
          ByteBuffer buffer = ((ByteBuffer) recordValue).duplicate();
          byte[] bytes = new byte[buffer.remaining()];
          buffer.get(bytes);
          return BlobColumn.of(name, bytes);
        } else {
          throw new AssertionError("Unexpected BLOB value type: " + recordValue.getClass());
        }
      case DATE:
        return recordValue == null
            ? DateColumn.ofNull(name)
            : DateColumn.of(
                name, TimeRelatedColumnEncodingUtils.decodeDate(((Number) recordValue).intValue()));
      case TIME:
        return recordValue == null
            ? TimeColumn.ofNull(name)
            : TimeColumn.ofStrict(
                name,
                TimeRelatedColumnEncodingUtils.decodeTime(((Number) recordValue).longValue()));
      case TIMESTAMP:
        return recordValue == null
            ? TimestampColumn.ofNull(name)
            : TimestampColumn.ofStrict(
                name,
                TimeRelatedColumnEncodingUtils.decodeTimestamp(((Number) recordValue).longValue()));
      case TIMESTAMPTZ:
        return recordValue == null
            ? TimestampTZColumn.ofNull(name)
            : TimestampTZColumn.ofStrict(
                name,
                TimeRelatedColumnEncodingUtils.decodeTimestampTZ(
                    ((Number) recordValue).longValue()));
      default:
        throw new AssertionError();
    }
  }
}
