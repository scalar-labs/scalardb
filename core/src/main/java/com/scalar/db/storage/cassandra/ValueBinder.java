package com.scalar.db.storage.cassandra;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.driver.core.BoundStatement;
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
 * A visitor class to bind {@code Value}s to a {@link BoundStatement}
 *
 * @author Hiroyuki Yamada
 */
@NotThreadSafe
public final class ValueBinder implements ValueVisitor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ValueBinder.class);
  private final BoundStatement bound;
  private int i;

  /**
   * Constructs {@code ValueBinder} with the specified {@code BoundStatement}
   *
   * @param bound a {@code BoundStatement} to be bound
   */
  public ValueBinder(BoundStatement bound) {
    this.bound = checkNotNull(bound);
    i = 0;
  }

  /**
   * Sets the specified {@code BooleanValue} to the bound statement
   *
   * @param value a {@code BooleanValue} to be set
   */
  @Override
  public void visit(BooleanValue value) {
    LOGGER.debug(value.get() + " is bound to " + i);
    bound.setBool(i++, value.get());
  }

  /**
   * Sets the specified {@code IntValue} to the bound statement
   *
   * @param value a {@code IntValue} to be set
   */
  @Override
  public void visit(IntValue value) {
    LOGGER.debug(value.get() + " is bound to " + i);
    bound.setInt(i++, value.get());
  }

  /**
   * Sets the specified {@code BigIntValue} to the bound statement
   *
   * @param value a {@code BigIntValue} to be set
   */
  @Override
  public void visit(BigIntValue value) {
    LOGGER.debug(value.get() + " is bound to " + i);
    bound.setLong(i++, value.get());
  }

  /**
   * Sets the specified {@code FloatValue} to the bound statement
   *
   * @param value a {@code FloatValue} to be set
   */
  @Override
  public void visit(FloatValue value) {
    LOGGER.debug(value.get() + " is bound to " + i);
    bound.setFloat(i++, value.get());
  }

  /**
   * Sets the specified {@code DoubleValue} to the bound statement
   *
   * @param value a {@code DoubleValue} to be set
   */
  @Override
  public void visit(DoubleValue value) {
    LOGGER.debug(value.get() + " is bound to " + i);
    bound.setDouble(i++, value.get());
  }

  /**
   * Sets the specified {@code TextValue} to the bound statement
   *
   * @param value a {@code TextValue} to be set
   */
  @Override
  public void visit(TextValue value) {
    LOGGER.debug(value.get() + " is bound to " + i);
    value.get().ifPresent(s -> bound.setString(i, s));
    i++;
  }

  /**
   * Sets the specified {@code BlobValue} to the bound statement
   *
   * @param value a {@code BlobValue} to be set
   */
  @Override
  public void visit(BlobValue value) {
    LOGGER.debug(value.get() + " is bound to " + i);
    value
        .get()
        .ifPresent(
            b -> bound.setBytes(i, (ByteBuffer) ByteBuffer.allocate(b.length).put(b).flip()));
    i++;
  }
}
