package com.scalar.db.storage.dynamo.bytes;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.TimestampTZColumn;
import com.scalar.db.storage.ColumnSerializationUtils;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class TimestampTZBytesEncoder implements BytesEncoder<TimestampTZColumn> {
  TimestampTZBytesEncoder() {}

  @Override
  public int encodedLength(TimestampTZColumn column, Order order) {
    return getBytes(column).length + 1;
  }

  @Override
  public void encode(TimestampTZColumn column, Order order, ByteBuffer dst) {
    BytesUtils.encodeString(getBytes(column), order, dst);
  }

  private byte[] getBytes(TimestampTZColumn column) {
    assert !column.hasNullValue();

    return ColumnSerializationUtils.toCompactFormat(column).getBytes(StandardCharsets.UTF_8);
  }
}
