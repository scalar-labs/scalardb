package com.scalar.db.storage.dynamo.bytes;

import static com.scalar.db.storage.dynamo.bytes.BytesUtils.encodeLong;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.DateColumn;
import com.scalar.db.storage.ColumnSerializationUtils;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class DateBytesEncoder implements BytesEncoder<DateColumn> {

  DateBytesEncoder() {}

  @Override
  public int encodedLength(DateColumn column, Order order) {
    assert column.getValue().isPresent();

    return 8;
  }

  @Override
  public void encode(DateColumn column, Order order, ByteBuffer dst) {
    assert column.getValue().isPresent();

    long value = ColumnSerializationUtils.toCompactFormat(column);
    encodeLong(value, order, dst);
  }
}
