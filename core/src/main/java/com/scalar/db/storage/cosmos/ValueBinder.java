package com.scalar.db.storage.cosmos;

import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.ValueVisitor;
import java.util.Base64;
import java.util.function.Consumer;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A visitor class to bind {@code Value}s to a {@link StringBuilder}
 *
 * @author Yuji Ito
 */
@NotThreadSafe
public final class ValueBinder implements ValueVisitor {
  private Consumer<Object> consumer;

  /** Constructs {@code ValueBinder} */
  public ValueBinder() {}

  public void set(Consumer<Object> consumer) {
    this.consumer = consumer;
  }

  /**
   * Sets the specified {@code BooleanValue} to the query
   *
   * @param value a {@code BooleanValue} to be set
   */
  @Override
  public void visit(BooleanValue value) {
    consumer.accept(value.get());
  }

  /**
   * Sets the specified {@code IntValue} to the query
   *
   * @param value a {@code IntValue} to be set
   */
  @Override
  public void visit(IntValue value) {
    consumer.accept(value.get());
  }

  /**
   * Sets the specified {@code BigIntValue} to the query
   *
   * @param value a {@code BigIntValue} to be set
   */
  @Override
  public void visit(BigIntValue value) {
    consumer.accept(value.get());
  }

  /**
   * Sets the specified {@code FloatValue} to the query
   *
   * @param value a {@code FloatValue} to be set
   */
  @Override
  public void visit(FloatValue value) {
    consumer.accept(value.get());
  }

  /**
   * Sets the specified {@code DoubleValue} to the query
   *
   * @param value a {@code DoubleValue} to be set
   */
  @Override
  public void visit(DoubleValue value) {
    consumer.accept(value.get());
  }

  /**
   * Sets the specified {@code TextValue} to the query
   *
   * @param value a {@code TextValue} to be set
   */
  @Override
  public void visit(TextValue value) {
    value.get().ifPresent(s -> consumer.accept(s));
  }

  /**
   * Sets the specified {@code BlobValue} to the query
   *
   * @param value a {@code BlobValue} to be set
   */
  @Override
  public void visit(BlobValue value) {
    value.get().ifPresent(b -> consumer.accept(Base64.getEncoder().encodeToString(b)));
  }
}
