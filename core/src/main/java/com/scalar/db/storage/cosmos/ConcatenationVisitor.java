package com.scalar.db.storage.cosmos;

import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.ValueVisitor;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A visitor class to make a concatenated key string for the partition key. This uses a colon as a
 * key separator, so the text column value should not contain colons.
 *
 * @author Yuji Ito
 */
@NotThreadSafe
public class ConcatenationVisitor implements ValueVisitor {
  private final List<String> values;

  public ConcatenationVisitor() {
    values = new ArrayList<>();
  }

  public String build() {
    return String.join(":", values);
  }

  /**
   * Sets the specified {@code BooleanValue} to the key string
   *
   * @param value a {@code BooleanValue} to be set
   */
  @Override
  public void visit(BooleanValue value) {
    values.add(String.valueOf(value.get()));
  }

  /**
   * Sets the specified {@code IntValue} to the key string
   *
   * @param value a {@code IntValue} to be set
   */
  @Override
  public void visit(IntValue value) {
    values.add(String.valueOf(value.get()));
  }

  /**
   * Sets the specified {@code BigIntValue} to the key string
   *
   * @param value a {@code BigIntValue} to be set
   */
  @Override
  public void visit(BigIntValue value) {
    values.add(String.valueOf(value.get()));
  }

  /**
   * Sets the specified {@code FloatValue} to the key string
   *
   * @param value a {@code FloatValue} to be set
   */
  @Override
  public void visit(FloatValue value) {
    values.add(String.valueOf(value.get()));
  }

  /**
   * Sets the specified {@code DoubleValue} to the key string
   *
   * @param value a {@code DoubleValue} to be set
   */
  @Override
  public void visit(DoubleValue value) {
    values.add(String.valueOf(value.get()));
  }

  /**
   * Sets the specified {@code TextValue} to the key string
   *
   * @param value a {@code TextValue} to be set
   */
  @Override
  public void visit(TextValue value) {
    value.get().ifPresent(values::add);
  }

  /**
   * Sets the specified {@code BlobValue} to the key string
   *
   * @param value a {@code BlobValue} to be set
   */
  @Override
  public void visit(BlobValue value) {
    // Use Base64 encoding
    value
        .get()
        .ifPresent(b -> values.add(Base64.getUrlEncoder().withoutPadding().encodeToString(b)));
  }
}
