package com.scalar.db.storage.dynamo.bytes;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.ColumnVisitor;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class KeyBytesEncodedLengthCalculator implements ColumnVisitor {

  private int length;
  private Map<String, Order> keyOrders;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public int calculate(Key key, Map<String, Order> keyOrders) {
    this.keyOrders = keyOrders;
    for (Column<?> column : key.getColumns()) {
      column.accept(this);
    }
    return length;
  }

  @Override
  public void visit(BooleanColumn value) {
    length +=
        BytesEncoders.BOOLEAN.encodedLength(
            value, keyOrders.getOrDefault(value.getName(), Order.ASC));
  }

  @Override
  public void visit(IntColumn value) {
    length +=
        BytesEncoders.INT.encodedLength(value, keyOrders.getOrDefault(value.getName(), Order.ASC));
  }

  @Override
  public void visit(BigIntColumn value) {
    length +=
        BytesEncoders.BIGINT.encodedLength(
            value, keyOrders.getOrDefault(value.getName(), Order.ASC));
  }

  @Override
  public void visit(FloatColumn value) {
    length +=
        BytesEncoders.FLOAT.encodedLength(
            value, keyOrders.getOrDefault(value.getName(), Order.ASC));
  }

  @Override
  public void visit(DoubleColumn value) {
    length +=
        BytesEncoders.DOUBLE.encodedLength(
            value, keyOrders.getOrDefault(value.getName(), Order.ASC));
  }

  @Override
  public void visit(TextColumn value) {
    length +=
        BytesEncoders.TEXT.encodedLength(value, keyOrders.getOrDefault(value.getName(), Order.ASC));
  }

  @Override
  public void visit(BlobColumn value) {
    length +=
        BytesEncoders.BLOB.encodedLength(value, keyOrders.getOrDefault(value.getName(), Order.ASC));
  }
}
