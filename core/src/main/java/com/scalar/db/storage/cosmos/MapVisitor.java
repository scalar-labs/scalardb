package com.scalar.db.storage.cosmos;

import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.ValueVisitor;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A visitor to make a map to be used to create {@link Record}
 *
 * @author Yuji Ito
 */
@NotThreadSafe
public class MapVisitor implements ValueVisitor {
  private final Map<String, Object> values;

  public MapVisitor() {
    values = new HashMap<>();
  }

  public Map<String, Object> get() {
    return values;
  }

  /**
   * Sets the specified {@code BooleanValue} to the map
   *
   * @param value a {@code BooleanValue} to be set
   */
  @Override
  public void visit(BooleanValue value) {
    values.put(value.getName(), value.get());
  }

  /**
   * Sets the specified {@code IntValue} to the map
   *
   * @param value a {@code IntValue} to be set
   */
  @Override
  public void visit(IntValue value) {
    values.put(value.getName(), value.get());
  }

  /**
   * Sets the specified {@code BigIntValue} to the map
   *
   * @param value a {@code BigIntValue} to be set
   */
  @Override
  public void visit(BigIntValue value) {
    values.put(value.getName(), value.get());
  }

  /**
   * Sets the specified {@code FloatValue} to the map
   *
   * @param value a {@code FloatValue} to be set
   */
  @Override
  public void visit(FloatValue value) {
    values.put(value.getName(), value.get());
  }

  /**
   * Sets the specified {@code DoubleValue} to the map
   *
   * @param value a {@code DoubleValue} to be set
   */
  @Override
  public void visit(DoubleValue value) {
    values.put(value.getName(), value.get());
  }

  /**
   * Sets the specified {@code TextValue} to the map
   *
   * @param value a {@code TextValue} to be set
   */
  @Override
  public void visit(TextValue value) {
    value.get().ifPresent(s -> values.put(value.getName(), s));
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
        .ifPresent(b -> values.put(value.getName(), ByteBuffer.allocate(b.length).put(b).flip()));
  }
}
