package com.scalar.db.storage.dynamo.bytes;

import static com.scalar.db.storage.dynamo.bytes.BytesUtils.mask;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.BlobValue;
import java.nio.ByteBuffer;

public class BlobBytesEncoder implements BytesEncoder<BlobValue> {

  private static final byte DESC_TERM = (byte) 0xff;

  BlobBytesEncoder() {}

  @Override
  public int encodedLength(BlobValue value, Order order) {
    assert value.getAsBytes().isPresent();
    assert value.getAsBytes().get().length > 0;
    return value.getAsBytes().get().length + (order == Order.ASC ? 0 : 1);
  }

  @Override
  public void encode(BlobValue value, Order order, ByteBuffer dst) {
    assert value.getAsBytes().isPresent();
    assert value.getAsBytes().get().length > 0;

    for (byte b : value.getAsBytes().get()) {
      dst.put(mask(b, order));
    }
    if (order == Order.DESC) {
      // DESC ordered BlobValue requires a termination bit to preserve the sort order
      dst.put(DESC_TERM);
    }
  }
}
