package com.scalar.db.storage.dynamo.bytes;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import com.scalar.db.io.ValueVisitor;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class KeyBytesEncodedLengthCalculator implements ValueVisitor {

  private int length;
  private Map<String, Order> keyOrders;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public int calculate(Key key, Map<String, Order> keyOrders) {
    this.keyOrders = keyOrders;
    for (Value<?> value : key) {
      value.accept(this);
    }
    return length;
  }

  @Override
  public void visit(BooleanValue value) {
    length +=
        BytesEncoders.BOOLEAN.encodedLength(
            value, keyOrders.getOrDefault(value.getName(), Order.ASC));
  }

  @Override
  public void visit(IntValue value) {
    length +=
        BytesEncoders.INT.encodedLength(value, keyOrders.getOrDefault(value.getName(), Order.ASC));
  }

  @Override
  public void visit(BigIntValue value) {
    length +=
        BytesEncoders.BIGINT.encodedLength(
            value, keyOrders.getOrDefault(value.getName(), Order.ASC));
  }

  @Override
  public void visit(FloatValue value) {
    length +=
        BytesEncoders.FLOAT.encodedLength(
            value, keyOrders.getOrDefault(value.getName(), Order.ASC));
  }

  @Override
  public void visit(DoubleValue value) {
    length +=
        BytesEncoders.DOUBLE.encodedLength(
            value, keyOrders.getOrDefault(value.getName(), Order.ASC));
  }

  @Override
  public void visit(TextValue value) {
    length +=
        BytesEncoders.TEXT.encodedLength(value, keyOrders.getOrDefault(value.getName(), Order.ASC));
  }

  @Override
  public void visit(BlobValue value) {
    length +=
        BytesEncoders.BLOB.encodedLength(value, keyOrders.getOrDefault(value.getName(), Order.ASC));
  }
}
