package com.scalar.db.storage.dynamo.bytes;

import static com.scalar.db.storage.dynamo.bytes.BytesUtils.mask;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.DoubleColumn;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class DoubleBytesEncoder implements BytesEncoder<DoubleColumn> {

  DoubleBytesEncoder() {}

  @Override
  public int encodedLength(DoubleColumn column, Order order) {
    return 8;
  }

  @Override
  public void encode(DoubleColumn column, Order order, ByteBuffer dst) {
    assert !column.hasNullValue();
    /*
     * The IEE754 floating point format already preserves sort ordering for positive floating point
     * numbers when the raw bytes are compared in most significant byte order.
     * Thus, we need only ensure that negative numbers sort in the exact opposite order as positive
     * numbers (so that say, negative infinity is less than negative 1), and that all negative
     * numbers compare less than any positive number. To accomplish this, we invert the sign bit of
     * all floating point numbers, and we also invert the exponent and significand bits if the
     * floating point number was negative.
     */

    // store the floating point bits into a 64-bit long
    long l = Double.doubleToLongBits(column.getDoubleValue());

    // invert the sign bit and XOR's all other bits with the sign bit itself
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
