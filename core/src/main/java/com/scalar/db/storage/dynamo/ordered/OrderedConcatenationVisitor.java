package com.scalar.db.storage.dynamo.ordered;

import com.fasterxml.jackson.core.util.ByteArrayBuilder;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.ValueVisitor;
import com.scalar.db.storage.dynamo.ordered.OrderedEncoder;
import java.util.ArrayList;
import java.util.List;

public class OrderedConcatenationVisitor implements ValueVisitor {

  private static final Order DEFAULT_ORDER = Order.ASC;
  private static final byte[] VALUE_SEPARATOR_BEFORE = new byte[] {0};
  private static final byte[] VALUE_SEPARATOR = new byte[] {1};
  private static final byte[] VALUE_SEPARATOR_AFTER = new byte[] {2};
  private final ByteArrayBuilder byteArrayBuilder;
  // Using for storing values before build
  private final List<byte[]> values;
  private final Order order;

  public OrderedConcatenationVisitor() {
    byteArrayBuilder = new ByteArrayBuilder();

    values = new ArrayList<>();
    order = DEFAULT_ORDER;
  }

  public OrderedConcatenationVisitor(Order order) {
    byteArrayBuilder = new ByteArrayBuilder();
    values = new ArrayList<>();
    this.order = order;
  }

  public byte[] build() {
    for (byte[] val : values) {
      byteArrayBuilder.write(val);
      byteArrayBuilder.write(VALUE_SEPARATOR);
    }
    return byteArrayBuilder.toByteArray();
  }

  public byte[] buildAsStartInclusive() {
    for (int i = 0; i < values.size(); i++) {
      byteArrayBuilder.write(values.get(i));
      if (i < (values.size() - 1)) {
        byteArrayBuilder.write(VALUE_SEPARATOR);
      } else {
        byteArrayBuilder.write(VALUE_SEPARATOR_BEFORE);
      }
    }
    return byteArrayBuilder.toByteArray();
  }

  public byte[] buildAsStartExclusive() {
    for (int i = 0; i < values.size(); i++) {
      byteArrayBuilder.write(values.get(i));
      if (i < (values.size() - 1)) {
        byteArrayBuilder.write(VALUE_SEPARATOR);
      } else {
        byteArrayBuilder.write(VALUE_SEPARATOR_AFTER);
      }
    }
    return byteArrayBuilder.toByteArray();
  }

  public byte[] buildAsEndInclusive() {
    return buildAsStartExclusive();
  }

  public byte[] buildAsEndExclusive() {
    return buildAsStartInclusive();
  }

  @Override
  public void visit(BooleanValue value) {
    byte[] bytesValue = OrderedEncoder.orderedEncode(value, order);
    values.add(bytesValue);
  }

  @Override
  public void visit(IntValue value) {
    byte[] bytesValue = OrderedEncoder.orderedEncode(value, order);
    values.add(bytesValue);
  }

  @Override
  public void visit(BigIntValue value) {
    byte[] bytesValue = OrderedEncoder.orderedEncode(value, order);
    values.add(bytesValue);
  }

  @Override
  public void visit(FloatValue value) {
    byte[] bytesValue = OrderedEncoder.orderedEncode(value, order);
    values.add(bytesValue);
  }

  @Override
  public void visit(DoubleValue value) {
    byte[] bytesValue = OrderedEncoder.orderedEncode(value, order);
    values.add(bytesValue);
  }

  @Override
  public void visit(TextValue value) {
    byte[] bytesValue = OrderedEncoder.orderedEncode(value, order);
    values.add(bytesValue);
  }

  @Override
  public void visit(BlobValue value) {
    if (value.get().isPresent()) {
      values.add(value.get().get());
    }
  }
}
