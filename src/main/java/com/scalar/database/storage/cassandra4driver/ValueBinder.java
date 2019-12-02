package com.scalar.database.storage.cassandra4driver;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.scalar.database.io.BigIntValue;
import com.scalar.database.io.BlobValue;
import com.scalar.database.io.BooleanValue;
import com.scalar.database.io.DoubleValue;
import com.scalar.database.io.FloatValue;
import com.scalar.database.io.IntValue;
import com.scalar.database.io.TextValue;
import com.scalar.database.io.ValueVisitor;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A visitor class to bind {@code Value}s to a {@link BoundStatementBuilder}
 *
 * @author Hiroyuki Yamada, Yuji Ito
 */
@NotThreadSafe
public final class ValueBinder implements ValueVisitor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ValueBinder.class);
  private final BoundStatementBuilder builder;
  private int i;

  /**
   * Constructs {@code ValueBinder} with the specified {@code BoundStatementBuilder}
   *
   * @param builder a {@code BoundStatementBuilder} to be bound
   */
  public ValueBinder(BoundStatementBuilder builder) {
    this.builder = checkNotNull(builder);
    i = 0;
  }

  /**
   * Sets the specified {@code BooleanValue} to the bound statement builder
   *
   * @param value a {@code BooleanValue} to be set
   */
  @Override
  public void visit(BooleanValue value) {
    LOGGER.debug(value.get() + " is bound to " + i);
    builder.setBoolean(i++, value.get());
  }

  /**
   * Sets the specified {@code IntValue} to the bound statement builder
   *
   * @param value a {@code IntValue} to be set
   */
  @Override
  public void visit(IntValue value) {
    LOGGER.debug(value.get() + " is bound to " + i);
    builder.setInt(i++, value.get());
  }

  /**
   * Sets the specified {@code BigIntValue} to the bound statement builder
   *
   * @param value a {@code BigIntValue} to be set
   */
  @Override
  public void visit(BigIntValue value) {
    LOGGER.debug(value.get() + " is bound to " + i);
    builder.setLong(i++, value.get());
  }

  /**
   * Sets the specified {@code FloatValue} to the bound statement builder
   *
   * @param value a {@code FloatValue} to be set
   */
  @Override
  public void visit(FloatValue value) {
    LOGGER.debug(value.get() + " is bound to " + i);
    builder.setFloat(i++, value.get());
  }

  /**
   * Sets the specified {@code DoubleValue} to the bound statement builder
   *
   * @param value a {@code DoubleValue} to be set
   */
  @Override
  public void visit(DoubleValue value) {
    LOGGER.debug(value.get() + " is bound to " + i);
    builder.setDouble(i++, value.get());
  }

  /**
   * Sets the specified {@code TextValue} to the bound statement builder
   *
   * @param value a {@code TextValue} to be set
   */
  @Override
  public void visit(TextValue value) {
    LOGGER.debug(value.getString() + " is bound to " + i);
    value.getString().ifPresent(s -> builder.setString(i, s));
    i++;
  }

  /**
   * Sets the specified {@code BlobValue} to the bound statement builder
   *
   * @param value a {@code BlobValue} to be set
   */
  @Override
  public void visit(BlobValue value) {
    LOGGER.debug(value.get() + " is bound to " + i);
    value
        .get()
        .ifPresent(
            b -> {
              builder.setByteBuffer(i, (ByteBuffer) ByteBuffer.allocate(b.length).put(b).flip());
            });
    i++;
  }
}
