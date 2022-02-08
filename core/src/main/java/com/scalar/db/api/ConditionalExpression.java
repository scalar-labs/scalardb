package com.scalar.db.api;

import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import java.nio.ByteBuffer;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

/**
 * A conditional expression used in {@link MutationCondition}.
 *
 * @author Hiroyuki Yamada
 */
@Immutable
public class ConditionalExpression {
  private final String name;
  private final Value<?> value;
  private final Operator operator;

  /**
   * Constructs a {@code ConditionalExpression} with the specified name, value and operator. A
   * conditional expression will be "{@code <name> <operator> <value's content>}". {@code Value}'s
   * name won't be used to create an expression, so giving anonymous {@code Value} makes more sense
   * and is more readable.
   *
   * @param name name of target value
   * @param value value used to compare with the target value
   * @param operator operator used to compare the target value specified with the name and the value
   */
  public ConditionalExpression(String name, Value<?> value, Operator operator) {
    this.name = name;
    this.value = value;
    this.operator = operator;
    checkOperator(operator);
  }

  /**
   * Constructs a {@code ConditionalExpression} with the specified name, value and operator.
   *
   * @param name name of target value
   * @param value value used to compare with the target value
   * @param operator operator used to compare the target value specified with the name and the value
   */
  public ConditionalExpression(String name, boolean value, Operator operator) {
    this(name, new BooleanValue(value), operator);
  }

  /**
   * Constructs a {@code ConditionalExpression} with the specified name, value and operator.
   *
   * @param name name of target value
   * @param value value used to compare with the target value
   * @param operator operator used to compare the target value specified with the name and the value
   */
  public ConditionalExpression(String name, int value, Operator operator) {
    this(name, new IntValue(value), operator);
  }

  /**
   * Constructs a {@code ConditionalExpression} with the specified name, value and operator.
   *
   * @param name name of target value
   * @param value value used to compare with the target value
   * @param operator operator used to compare the target value specified with the name and the value
   */
  public ConditionalExpression(String name, long value, Operator operator) {
    this(name, new BigIntValue(value), operator);
  }

  /**
   * Constructs a {@code ConditionalExpression} with the specified name, value and operator.
   *
   * @param name name of target value
   * @param value value used to compare with the target value
   * @param operator operator used to compare the target value specified with the name and the value
   */
  public ConditionalExpression(String name, float value, Operator operator) {
    this(name, new FloatValue(value), operator);
  }

  /**
   * Constructs a {@code ConditionalExpression} with the specified name, value and operator.
   *
   * @param name name of target value
   * @param value value used to compare with the target value
   * @param operator operator used to compare the target value specified with the name and the value
   */
  public ConditionalExpression(String name, double value, Operator operator) {
    this(name, new DoubleValue(value), operator);
  }

  /**
   * Constructs a {@code ConditionalExpression} with the specified name, value and operator.
   *
   * @param name name of target value
   * @param value value used to compare with the target value
   * @param operator operator used to compare the target value specified with the name and the value
   */
  public ConditionalExpression(String name, String value, Operator operator) {
    this(name, new TextValue(value), operator);
  }

  /**
   * Constructs a {@code ConditionalExpression} with the specified name, value and operator.
   *
   * @param name name of target value
   * @param value value used to compare with the target value
   * @param operator operator used to compare the target value specified with the name and the value
   */
  public ConditionalExpression(String name, byte[] value, Operator operator) {
    this(name, new BlobValue(value), operator);
  }

  /**
   * Constructs a {@code ConditionalExpression} with the specified name, value and operator.
   *
   * @param name name of target value
   * @param value value used to compare with the target value
   * @param operator operator used to compare the target value specified with the name and the value
   */
  public ConditionalExpression(String name, ByteBuffer value, Operator operator) {
    this(name, new BlobValue(value), operator);
  }

  /**
   * Returns the name of target value
   *
   * @return the name of target value
   */
  public String getName() {
    return name;
  }

  /**
   * Return the value used to compare with the target value
   *
   * @return the value used to compare with the target value
   */
  public Value<?> getValue() {
    return value;
  }

  /**
   * Returns the operator used to compare the target value specified with the name and the value
   *
   * @return the operator used to compare the target value specified with the name and the value
   */
  public Operator getOperator() {
    return operator;
  }

  /**
   * Indicates whether some other object is "equal to" this object. The other object is considered
   * equal if:
   *
   * <ul>
   *   <li>it is also an {@code ConditionalExpression} and
   *   <li>both instances have the same name, value and operator.
   * </ul>
   *
   * @param o an object to be tested for equality
   * @return {@code true} if the other object is "equal to" this object otherwise {@code false}
   */
  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof ConditionalExpression)) {
      return false;
    }
    ConditionalExpression other = (ConditionalExpression) o;
    return name.equals(other.name) && value.equals(other.value) && operator.equals(other.operator);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, value, operator);
  }

  private void checkOperator(Operator op) {
    switch (op) {
      case EQ:
      case NE:
      case GT:
      case GTE:
      case LT:
      case LTE:
        return;
      default:
        throw new IllegalArgumentException(op + " is not supported.");
    }
  }

  public enum Operator {
    EQ,
    NE,
    GT,
    GTE,
    LT,
    LTE,
  }
}
