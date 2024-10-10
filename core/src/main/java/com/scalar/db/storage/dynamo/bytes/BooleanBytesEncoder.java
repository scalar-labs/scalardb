package com.scalar.db.storage.dynamo.bytes;

import static com.scalar.db.storage.dynamo.bytes.BytesUtils.mask;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.BooleanColumn;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class BooleanBytesEncoder implements BytesEncoder<BooleanColumn> {
  private static final byte FALSE = 0x00;
  private static final byte TRUE = 0x01;

  BooleanBytesEncoder() {}

  @Override
  public int encodedLength(BooleanColumn column, Order order) {
    return 1;
  }

  @Override
  public void encode(BooleanColumn column, Order order, ByteBuffer dst) {
    assert !column.hasNullValue();

    boolean b = column.getBooleanValue();
    dst.put(mask(b ? TRUE : FALSE, order));
  }
}
