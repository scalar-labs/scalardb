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
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

/** An encoder that converts a key to bytes while preserving the sort order. */
@NotThreadSafe
public class KeyBytesEncoder implements ValueVisitor {

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
    for (Value<?> value : key) {
      value.accept(this);
    }
    dst.flip();
    return dst;
  }

  @Override
  public void visit(BooleanValue value) {
    BytesEncoders.BOOLEAN.encode(value, keyOrders.getOrDefault(value.getName(), Order.ASC), dst);
  }

  @Override
  public void visit(IntValue value) {
    BytesEncoders.INT.encode(value, keyOrders.getOrDefault(value.getName(), Order.ASC), dst);
  }

  @Override
  public void visit(BigIntValue value) {
    BytesEncoders.BIGINT.encode(value, keyOrders.getOrDefault(value.getName(), Order.ASC), dst);
  }

  @Override
  public void visit(FloatValue value) {
    BytesEncoders.FLOAT.encode(value, keyOrders.getOrDefault(value.getName(), Order.ASC), dst);
  }

  @Override
  public void visit(DoubleValue value) {
    BytesEncoders.DOUBLE.encode(value, keyOrders.getOrDefault(value.getName(), Order.ASC), dst);
  }

  @Override
  public void visit(TextValue value) {
    BytesEncoders.TEXT.encode(value, keyOrders.getOrDefault(value.getName(), Order.ASC), dst);
  }

  @Override
  public void visit(BlobValue value) {
    BytesEncoders.BLOB.encode(value, keyOrders.getOrDefault(value.getName(), Order.ASC), dst);
  }
}
