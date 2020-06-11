package com.scalar.db.storage.cosmos;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.ValueVisitor;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A visitor class to bind {@code Value}s to a {@link StringBuilder}
 *
 * @author Yuji Ito
 */
@NotThreadSafe
public final class ValueBinder implements ValueVisitor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ValueBinder.class);
  private final StringBuilder builder;

  /**
   * Constructs {@code ValueBinder} with the specified {@link StringBuilder}
   *
   * @param builde a {@link StringBuilder} to be bound
   */
  public ValueBinder(StringBuilder builder) {
    this.builder = checkNotNull(builder);
  }

  /**
   * Sets the specified {@code BooleanValue} to the builder
   *
   * @param value a {@code BooleanValue} to be set
   */
  @Override
  public void visit(BooleanValue value) {
    builder.append(value.get());
  }

  /**
   * Sets the specified {@code IntValue} to the builder
   *
   * @param value a {@code IntValue} to be set
   */
  @Override
  public void visit(IntValue value) {
    builder.append(value.get());
  }

  /**
   * Sets the specified {@code BigIntValue} to the builder
   *
   * @param value a {@code BigIntValue} to be set
   */
  @Override
  public void visit(BigIntValue value) {
    builder.append(value.get());
  }

  /**
   * Sets the specified {@code FloatValue} to the builder
   *
   * @param value a {@code FloatValue} to be set
   */
  @Override
  public void visit(FloatValue value) {
    builder.append(value.get());
  }

  /**
   * Sets the specified {@code DoubleValue} to the builder
   *
   * @param value a {@code DoubleValue} to be set
   */
  @Override
  public void visit(DoubleValue value) {
    builder.append(value.get());
  }

  /**
   * Sets the specified {@code TextValue} to the builder
   *
   * @param value a {@code TextValue} to be set
   */
  @Override
  public void visit(TextValue value) {
    value.getString().ifPresent(s -> builder.append("'" + s + "'"));
  }

  /**
   * Sets the specified {@code BlobValue} to the builder
   *
   * @param value a {@code BlobValue} to be set
   */
  @Override
  public void visit(BlobValue value) {
    value
        .get()
        .ifPresent(
            b -> {
              ByteBuffer buffer = (ByteBuffer) ByteBuffer.allocate(b.length).put(b).flip();
              builder.append(new String(buffer.array()));
            });
  }
}
