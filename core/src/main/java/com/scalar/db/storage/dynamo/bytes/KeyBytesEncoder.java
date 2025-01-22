package com.scalar.db.storage.dynamo.bytes;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.ColumnVisitor;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

/** An encoder that converts a key to bytes while preserving the sort order. */
@NotThreadSafe
public class KeyBytesEncoder implements ColumnVisitor {

  private ByteBuffer dst;
  private Map<String, Order> keyOrders;

  public ByteBuffer encode(Key key) {
    return encode(key, Collections.emptyMap());
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public ByteBuffer encode(Key key, Map<String, Order> keyOrders) {
    this.keyOrders = keyOrders;
    int length = new KeyBytesEncodedLengthCalculator().calculate(key, keyOrders);
    dst = ByteBuffer.allocate(length);
    for (Column<?> value : key.getColumns()) {
      value.accept(this);
    }
    dst.flip();
    return dst;
  }

  @Override
  public void visit(BooleanColumn value) {
    BytesEncoders.BOOLEAN.encode(value, keyOrders.getOrDefault(value.getName(), Order.ASC), dst);
  }

  @Override
  public void visit(IntColumn value) {
    BytesEncoders.INT.encode(value, keyOrders.getOrDefault(value.getName(), Order.ASC), dst);
  }

  @Override
  public void visit(BigIntColumn value) {
    BytesEncoders.BIGINT.encode(value, keyOrders.getOrDefault(value.getName(), Order.ASC), dst);
  }

  @Override
  public void visit(FloatColumn value) {
    BytesEncoders.FLOAT.encode(value, keyOrders.getOrDefault(value.getName(), Order.ASC), dst);
  }

  @Override
  public void visit(DoubleColumn value) {
    BytesEncoders.DOUBLE.encode(value, keyOrders.getOrDefault(value.getName(), Order.ASC), dst);
  }

  @Override
  public void visit(TextColumn value) {
    BytesEncoders.TEXT.encode(value, keyOrders.getOrDefault(value.getName(), Order.ASC), dst);
  }

  @Override
  public void visit(BlobColumn value) {
    BytesEncoders.BLOB.encode(value, keyOrders.getOrDefault(value.getName(), Order.ASC), dst);
  }

  @Override
  public void visit(DateColumn column) {
    BytesEncoders.DATE.encode(column, keyOrders.getOrDefault(column.getName(), Order.ASC), dst);
  }

  @Override
  public void visit(TimeColumn column) {
    BytesEncoders.TIME.encode(column, keyOrders.getOrDefault(column.getName(), Order.ASC), dst);
  }

  @Override
  public void visit(TimestampColumn column) {
    BytesEncoders.TIMESTAMP.encode(
        column, keyOrders.getOrDefault(column.getName(), Order.ASC), dst);
  }

  @Override
  public void visit(TimestampTZColumn column) {
    BytesEncoders.TIMESTAMPTZ.encode(
        column, keyOrders.getOrDefault(column.getName(), Order.ASC), dst);
  }
}
