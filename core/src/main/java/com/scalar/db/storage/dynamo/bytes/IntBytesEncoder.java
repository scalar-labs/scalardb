package com.scalar.db.storage.dynamo.bytes;

import static com.scalar.db.storage.dynamo.bytes.BytesUtils.mask;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.IntValue;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class IntBytesEncoder implements BytesEncoder<IntValue> {

  IntBytesEncoder() {}

  @Override
  public int encodedLength(IntValue value, Order order) {
    return 4;
  }

  @Override
  public void encode(IntValue value, Order order, ByteBuffer dst) {
    int v = value.getAsInt();
    dst.put(mask((byte) ((v >> 24) ^ 0x80), order)); // Flip a sign bit to make it binary comparable
    dst.put(mask((byte) (v >> 16), order));
    dst.put(mask((byte) (v >> 8), order));
    dst.put(mask((byte) v, order));
  }
}
