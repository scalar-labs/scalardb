package com.scalar.db.storage.dynamo.bytes;

import static com.scalar.db.storage.dynamo.bytes.BytesUtils.mask;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.io.BlobColumn;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class BlobBytesEncoder implements BytesEncoder<BlobColumn> {

  private static final byte TERM = (byte) 0x00;
  private static final byte MASKED_TERM = (byte) 0xff;

  BlobBytesEncoder() {}

  @Override
  public int encodedLength(BlobColumn column, Order order) {
    assert column.getBlobValueAsBytes() != null;

    return column.getBlobValueAsBytes().length + (order == Order.ASC ? 0 : 1);
  }

  @Override
  public void encode(BlobColumn column, Order order, ByteBuffer dst) {
    assert column.getBlobValueAsBytes() != null;

    byte[] value = column.getBlobValueAsBytes();

    if (order == Order.DESC) {
      for (byte b : value) {
        if (b == TERM) {
          throw new IllegalArgumentException(
              CoreError.DYNAMO_ENCODER_0X00_BYTES_NOT_ACCEPTED_IN_BLOB_VALUES_IN_DESC_ORDER
                  .buildMessage());
        }
      }
    }

    for (byte b : value) {
      dst.put(mask(b, order));
    }
    if (order == Order.DESC) {
      // DESC ordered BlobValue requires a termination bit to preserve the sort order
      dst.put(MASKED_TERM);
    }
  }
}
