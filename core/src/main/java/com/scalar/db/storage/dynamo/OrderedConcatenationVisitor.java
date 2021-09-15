package com.scalar.db.storage.dynamo;

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

public class OrderedConcatenationVisitor implements ValueVisitor {

  private static final Order DEFAULT_ORDER = Order.ASC;
  private final ByteArrayBuilder byteArrayBuilder;

  public OrderedConcatenationVisitor() {
    byteArrayBuilder = new ByteArrayBuilder();
  }

  public byte[] build() {
    return byteArrayBuilder.toByteArray();
  }

  @Override
  public void visit(BooleanValue value) {
    byte[] bytesValue = OrderedEncoder.orderedEncode(value, DEFAULT_ORDER);
    byteArrayBuilder.write(bytesValue);
  }

  @Override
  public void visit(IntValue value) {
    byte[] bytesValue = OrderedEncoder.orderedEncode(value, DEFAULT_ORDER);
    byteArrayBuilder.write(bytesValue);
  }

  @Override
  public void visit(BigIntValue value) {
    byte[] bytesValue = OrderedEncoder.orderedEncode(value, DEFAULT_ORDER);
    byteArrayBuilder.write(bytesValue);
  }

  @Override
  public void visit(FloatValue value) {
    byte[] bytesValue = OrderedEncoder.orderedEncode(value, DEFAULT_ORDER);
    byteArrayBuilder.write(bytesValue);
  }

  @Override
  public void visit(DoubleValue value) {
    byte[] bytesValue = OrderedEncoder.orderedEncode(value, DEFAULT_ORDER);
    byteArrayBuilder.write(bytesValue);
  }

  @Override
  public void visit(TextValue value) {
    byte[] bytesValue = OrderedEncoder.orderedEncode(value, DEFAULT_ORDER);
    byteArrayBuilder.write(bytesValue);
  }

  @Override
  public void visit(BlobValue value) {
    byteArrayBuilder.write(value.get().get());
  }
}
