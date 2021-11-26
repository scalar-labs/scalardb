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
 * A visitor to make a map of {@link AttributeValue}.
 *
 * @author Yuji Ito
 */
@NotThreadSafe
public class MapVisitor implements ValueVisitor {
  private final Map<String, AttributeValue> values;

  public MapVisitor() {
    values = new HashMap<>();
  }

  @Nonnull
  public Map<String, AttributeValue> get() {
    return values;
  }

  /**
   * Sets the specified {@code BooleanValue} to the map
   *
   * @param value a {@code BooleanValue} to be set
   */
  @Override
  public void visit(BooleanValue value) {
    values.put(value.getName(), AttributeValue.builder().bool(value.getAsBoolean()).build());
  }

  /**
   * Sets the specified {@code IntValue} to the map
   *
   * @param value a {@code IntValue} to be set
   */
  @Override
  public void visit(IntValue value) {
    String v = String.valueOf(value.getAsInt());
    values.put(value.getName(), AttributeValue.builder().n(v).build());
  }

  /**
   * Sets the specified {@code BigIntValue} to the map
   *
   * @param value a {@code BigIntValue} to be set
   */
  @Override
  public void visit(BigIntValue value) {
    String v = String.valueOf(value.getAsLong());
    values.put(value.getName(), AttributeValue.builder().n(v).build());
  }

  /**
   * Sets the specified {@code FloatValue} to the map
   *
   * @param value a {@code FloatValue} to be set
   */
  @Override
  public void visit(FloatValue value) {
    String v = String.valueOf(value.getAsFloat());
    values.put(value.getName(), AttributeValue.builder().n(v).build());
  }

  /**
   * Sets the specified {@code DoubleValue} to the map
   *
   * @param value a {@code DoubleValue} to be set
   */
  @Override
  public void visit(DoubleValue value) {
    String v = String.valueOf(value.getAsDouble());
    values.put(value.getName(), AttributeValue.builder().n(v).build());
  }

  /**
   * Sets the specified {@code TextValue} to the map
   *
   * @param value a {@code TextValue} to be set
   */
  @Override
  public void visit(TextValue value) {
    value.get().ifPresent(s -> values.put(value.getName(), AttributeValue.builder().s(s).build()));
  }

  /**
   * Sets the specified {@code BlobValue} to the map
   *
   * @param value a {@code BlobValue} to be set
   */
  @Override
  public void visit(BlobValue value) {
    value
        .get()
        .ifPresent(
            b ->
                values.put(
                    value.getName(),
                    AttributeValue.builder().b(SdkBytes.fromByteArray(b)).build()));
  }
}
