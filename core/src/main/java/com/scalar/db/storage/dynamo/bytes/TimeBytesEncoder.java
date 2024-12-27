package com.scalar.db.storage.dynamo.bytes;

import static com.scalar.db.storage.dynamo.bytes.BytesUtils.encodeLong;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.util.TimeRelatedColumnEncodingUtils;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class TimeBytesEncoder implements BytesEncoder<TimeColumn> {

  TimeBytesEncoder() {}

  @Override
  public int encodedLength(TimeColumn column, Order order) {
    assert column.getValue().isPresent();

    return 8;
  }

  @Override
  public void encode(TimeColumn column, Order order, ByteBuffer dst) {
    assert !column.hasNullValue();

    long value = TimeRelatedColumnEncodingUtils.encode(column);
    encodeLong(value, order, dst);
  }
}
