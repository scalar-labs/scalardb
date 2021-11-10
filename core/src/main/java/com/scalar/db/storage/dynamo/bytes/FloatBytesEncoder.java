package com.scalar.db.storage.dynamo.bytes;

import static com.scalar.db.storage.dynamo.bytes.BytesUtils.mask;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.FloatValue;
import java.nio.ByteBuffer;

public class FloatBytesEncoder implements BytesEncoder<FloatValue> {

  FloatBytesEncoder() {}

  @Override
  public int encodedLength(FloatValue value, Order order) {
    return 4;
  }

  @Override
  public void encode(FloatValue value, Order order, ByteBuffer dst) {
    int i = Float.floatToIntBits(value.getAsFloat());
    i ^= ((i >> (Integer.SIZE - 1)) | Integer.MIN_VALUE);
    dst.put(mask((byte) (i >> 24), order))
        .put(mask((byte) (i >> 16), order))
        .put(mask((byte) (i >> 8), order))
        .put(mask((byte) i, order));
  }
}
