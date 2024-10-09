package com.scalar.db.storage.dynamo.bytes;

import static com.scalar.db.storage.dynamo.bytes.BytesUtils.mask;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.IntColumn;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class IntBytesEncoder implements BytesEncoder<IntColumn> {

  IntBytesEncoder() {}

  @Override
  public int encodedLength(IntColumn column, Order order) {
    return 4;
  }

  @Override
  public void encode(IntColumn column, Order order, ByteBuffer dst) {
    assert !column.hasNullValue();

    int v = column.getIntValue();
    dst.put(mask((byte) ((v >> 24) ^ 0x80), order)); // Flip a sign bit to make it binary comparable
    dst.put(mask((byte) (v >> 16), order));
    dst.put(mask((byte) (v >> 8), order));
    dst.put(mask((byte) v, order));
  }
}
