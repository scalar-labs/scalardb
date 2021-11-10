package com.scalar.db.storage.dynamo.bytes;

import static com.scalar.db.storage.dynamo.bytes.BytesUtils.mask;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.DoubleValue;
import java.nio.ByteBuffer;

public class DoubleBytesEncoder implements BytesEncoder<DoubleValue> {

  DoubleBytesEncoder() {}

  @Override
  public int encodedLength(DoubleValue value, Order order) {
    return 8;
  }

  @Override
  public void encode(DoubleValue value, Order order, ByteBuffer dst) {
    long l = Double.doubleToLongBits(value.getAsDouble());
    l ^= ((l >> (Long.SIZE - 1)) | Long.MIN_VALUE);
    dst.put(mask((byte) (l >> 56), order))
        .put(mask((byte) (l >> 48), order))
        .put(mask((byte) (l >> 40), order))
        .put(mask((byte) (l >> 32), order))
        .put(mask((byte) (l >> 24), order))
        .put(mask((byte) (l >> 16), order))
        .put(mask((byte) (l >> 8), order))
        .put(mask((byte) l, order));
  }
}
