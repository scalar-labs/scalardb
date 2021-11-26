package com.scalar.db.storage.dynamo;

import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.ValueVisitor;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * A visitor class to bind {@code Value}s to a condition expression
 *
 * @author Yuji Ito
 */
@NotThreadSafe
public final class ValueBinder implements ValueVisitor {
  private final Map<String, AttributeValue> values;
  private final String alias;
  private int i;

  /**
   * Constructs {@code ValueBinder} with the specified {@code BoundStatement}
   *
   * @param alias an alias
   */
  public ValueBinder(String alias) {
    this.values = new HashMap<>();
    this.alias = alias;
    this.i = 0;
  }

  @Nonnull
  public Map<String, AttributeValue> build() {
    return values;
  }

  /**
   * Sets the specified {@code BooleanValue} to the expression
   *
   * @param value a {@code BooleanValue} to be set
   */
  @Override
  public void visit(BooleanValue value) {
    values.put(alias + i, AttributeValue.builder().bool(value.getAsBoolean()).build());
    i++;
  }

  /**
   * Sets the specified {@code IntValue} to the expression
   *
   * @param value a {@code IntValue} to be set
   */
  @Override
  public void visit(IntValue value) {
    values.put(alias + i, AttributeValue.builder().n(String.valueOf(value.getAsInt())).build());
    i++;
  }

  /**
   * Sets the specified {@code BigIntValue} to the expression
   *
   * @param value a {@code BigIntValue} to be set
   */
  @Override
  public void visit(BigIntValue value) {
    values.put(alias + i, AttributeValue.builder().n(String.valueOf(value.getAsLong())).build());
    i++;
  }

  /**
   * Sets the specified {@code FloatValue} to the expression
   *
   * @param value a {@code FloatValue} to be set
   */
  @Override
  public void visit(FloatValue value) {
    values.put(alias + i, AttributeValue.builder().n(String.valueOf(value.getAsFloat())).build());
    i++;
  }

  /**
   * Sets the specified {@code DoubleValue} to the expression
   *
   * @param value a {@code DoubleValue} to be set
   */
  @Override
  public void visit(DoubleValue value) {
    values.put(alias + i, AttributeValue.builder().n(String.valueOf(value.getAsDouble())).build());
    i++;
  }

  /**
   * Sets the specified {@code TextValue} to the expression
   *
   * @param value a {@code TextValue} to be set
   */
  @Override
  public void visit(TextValue value) {
    AttributeValue.Builder builder = AttributeValue.builder();
    if (value.get().isPresent()) {
      builder.s(value.get().get());
    } else {
      builder.nul(true);
    }
    values.put(alias + i, builder.build());
    i++;
  }

  /**
   * Sets the specified {@code BlobValue} to the bound statement
   *
   * @param value a {@code BlobValue} to be set
   */
  @Override
  public void visit(BlobValue value) {
    AttributeValue.Builder builder = AttributeValue.builder();
    if (value.get().isPresent()) {
      builder.b(SdkBytes.fromByteArray(value.get().get()));
    } else {
      builder.nul(true);
    }
    values.put(alias + i, builder.build());
    i++;
  }
}
