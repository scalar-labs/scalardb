package com.scalar.db.storage.dynamo.bytes;

import static com.scalar.db.storage.dynamo.bytes.BytesUtils.encodeLong;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.TimestampTZColumn;
import com.scalar.db.storage.ColumnEncodingUtils;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class TimestampTZBytesEncoder implements BytesEncoder<TimestampTZColumn> {
  TimestampTZBytesEncoder() {}

  @Override
  public int encodedLength(TimestampTZColumn column, Order order) {
    return 8;
  }

  @Override
  public void encode(TimestampTZColumn column, Order order, ByteBuffer dst) {
    long value = ColumnEncodingUtils.encode(column);
    encodeLong(value, order, dst);
  }
}
