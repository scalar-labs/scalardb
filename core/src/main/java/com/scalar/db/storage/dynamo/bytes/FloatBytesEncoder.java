package com.scalar.db.storage.dynamo.bytes;

import static com.scalar.db.storage.dynamo.bytes.BytesUtils.mask;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.FloatColumn;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class FloatBytesEncoder implements BytesEncoder<FloatColumn> {

  FloatBytesEncoder() {}

  @Override
  public int encodedLength(FloatColumn column, Order order) {
    return 4;
  }

  @Override
  public void encode(FloatColumn column, Order order, ByteBuffer dst) {
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

    // store the floating point bits into a 32-bit int
    int i = Float.floatToIntBits(column.getFloatValue());

    // invert the sign bit and XOR's all other bits with the sign bit itself
    i ^= ((i >> (Integer.SIZE - 1)) | Integer.MIN_VALUE);

    dst.put(mask((byte) (i >> 24), order))
        .put(mask((byte) (i >> 16), order))
        .put(mask((byte) (i >> 8), order))
        .put(mask((byte) i, order));
  }
}
