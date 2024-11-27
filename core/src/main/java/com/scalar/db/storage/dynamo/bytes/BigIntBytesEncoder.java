package com.scalar.db.storage.dynamo.bytes;

import static com.scalar.db.storage.dynamo.bytes.BytesUtils.mask;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.BigIntColumn;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class BigIntBytesEncoder implements BytesEncoder<BigIntColumn> {

  BigIntBytesEncoder() {}

  @Override
  public int encodedLength(BigIntColumn column, Order order) {
    return 8;
  }

  @Override
  public void encode(BigIntColumn column, Order order, ByteBuffer dst) {
    assert !column.hasNullValue();

    long v = column.getBigIntValue();
    dst.put(mask((byte) ((v >> 56) ^ 0x80), order)); // Flip a sign bit to make it binary comparable
    dst.put(mask((byte) (v >> 48), order));
    dst.put(mask((byte) (v >> 40), order));
    dst.put(mask((byte) (v >> 32), order));
    dst.put(mask((byte) (v >> 24), order));
    dst.put(mask((byte) (v >> 16), order));
    dst.put(mask((byte) (v >> 8), order));
    dst.put(mask((byte) v, order));
  }
}
