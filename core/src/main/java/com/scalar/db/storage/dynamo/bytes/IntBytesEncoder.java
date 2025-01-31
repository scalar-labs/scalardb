package com.scalar.db.storage.dynamo.bytes;

import static com.scalar.db.storage.dynamo.bytes.BytesUtils.encodeInt;

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
    encodeInt(v, order, dst);
  }
}
