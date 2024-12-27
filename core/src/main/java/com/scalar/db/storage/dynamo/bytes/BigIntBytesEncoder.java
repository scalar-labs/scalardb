package com.scalar.db.storage.dynamo.bytes;

import static com.scalar.db.storage.dynamo.bytes.BytesUtils.encodeLong;

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

    encodeLong(column.getBigIntValue(), order, dst);
  }
}
