package com.scalar.db.storage.dynamo.bytes;

import static com.scalar.db.storage.dynamo.bytes.BytesUtils.encodeInt;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.DateColumn;
import com.scalar.db.util.TimeRelatedColumnEncodingUtils;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class DateBytesEncoder implements BytesEncoder<DateColumn> {

  DateBytesEncoder() {}

  @Override
  public int encodedLength(DateColumn column, Order order) {
    assert column.getValue().isPresent();

    return 4;
  }

  @Override
  public void encode(DateColumn column, Order order, ByteBuffer dst) {
    assert column.getValue().isPresent();

    int value = TimeRelatedColumnEncodingUtils.encode(column);
    encodeInt(value, order, dst);
  }
}
