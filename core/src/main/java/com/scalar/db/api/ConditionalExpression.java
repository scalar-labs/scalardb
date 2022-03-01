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
  private final String columnName;
  private final Value<?> value;
  private final Operator operator;

  /**
   * Constructs a {@code ConditionalExpression} with the specified column name, value and operator.
   * A conditional expression will be "{@code <columnName> <operator> <value>}".
   *
   * <p>{@code Value}'s name won't be used to create an expression, so giving anonymous {@code
   * Value} makes more sense and is more readable.
   *
   * @param columnName a name of target column
   * @param value a value used to compare with the target column
   * @param operator an operator used to compare the target column
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  public ConditionalExpression(String columnName, Value<?> value, Operator operator) {
    this.columnName = columnName;
    this.value = value;
    this.operator = operator;
  }

  /**
   * Constructs a {@code ConditionalExpression} with the specified column name, value and operator.
   *
   * @param columnName a name of target column
   * @param booleanValue a BOOLEAN value used to compare with the target column
   * @param operator an operator used to compare the target column
   */
  public ConditionalExpression(String columnName, boolean booleanValue, Operator operator) {
    this(columnName, new BooleanValue(booleanValue), operator);
  }

  /**
   * Constructs a {@code ConditionalExpression} with the specified column name, value and operator.
   *
   * @param columnName a name of target column
   * @param intValue an INT value used to compare with the target column
   * @param operator an operator used to compare the target column
   */
  public ConditionalExpression(String columnName, int intValue, Operator operator) {
    this(columnName, new IntValue(intValue), operator);
  }

  /**
   * Constructs a {@code ConditionalExpression} with the specified column name, value and operator.
   *
   * @param columnName a name of target column
   * @param bigIntValue a BIGINT value used to compare with the target column
   * @param operator an operator used to compare the target column
   */
  public ConditionalExpression(String columnName, long bigIntValue, Operator operator) {
    this(columnName, new BigIntValue(bigIntValue), operator);
  }

  /**
   * Constructs a {@code ConditionalExpression} with the specified column name, value and operator.
   *
   * @param columnName a name of target column
   * @param floatValue a FLOAT value used to compare with the target column
   * @param operator an operator used to compare the target column
   */
  public ConditionalExpression(String columnName, float floatValue, Operator operator) {
    this(columnName, new FloatValue(floatValue), operator);
  }

  /**
   * Constructs a {@code ConditionalExpression} with the specified column name, value and operator.
   *
   * @param columnName a name of target column
   * @param doubleValue a DOUBLE value used to compare with the target column
   * @param operator an operator used to compare the target column
   */
  public ConditionalExpression(String columnName, double doubleValue, Operator operator) {
    this(columnName, new DoubleValue(doubleValue), operator);
  }

  /**
   * Constructs a {@code ConditionalExpression} with the specified column name, value and operator.
   *
   * @param columnName a name of target column
   * @param textValue a TEXT value used to compare with the target column
   * @param operator an operator used to compare the target column
   */
  public ConditionalExpression(String columnName, String textValue, Operator operator) {
    this(columnName, new TextValue(textValue), operator);
  }

  /**
   * Constructs a {@code ConditionalExpression} with the specified column name, value and operator.
   *
   * @param columnName a name of target column
   * @param blobValue a BLOB value used to compare with the target column
   * @param operator an operator used to compare the target column
   */
  public ConditionalExpression(String columnName, byte[] blobValue, Operator operator) {
    this(columnName, new BlobValue(blobValue), operator);
  }

  /**
   * Constructs a {@code ConditionalExpression} with the specified column name, value and operator.
   *
   * @param columnName a name of target column
   * @param blobValue a BLOB value used to compare with the target column
   * @param operator an operator used to compare the target column
   */
  public ConditionalExpression(String columnName, ByteBuffer blobValue, Operator operator) {
    this(columnName, new BlobValue(blobValue), operator);
  }

  /**
   * Returns the column name of target column.
   *
   * @return the column name of target column
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  public String getName() {
    return columnName;
  }

  /**
   * Returns the column name of target column.
   *
   * @return the column name of target column
   */
  public String getColumnName() {
    return columnName;
  }

  /**
   * Return the value used to compare with the target column.
   *
   * <p>This method is primarily for internal use. Breaking changes can and will be introduced to
   * this method. Users should not depend on it.
   *
   * @return the value used to compare with the target column
   */
  public Value<?> getValue() {
    return value;
  }

  /**
   * Returns the BOOLEAN value to compare with the target column.
   *
   * @return the BOOLEAN value to compare with the target column
   */
  public boolean getBooleanValue() {
    return value.getAsBoolean();
  }

  /**
   * Returns the INT value to compare with the target column.
   *
   * @return the INT value to compare with the target column
   */
  public int getIntValue() {
    return value.getAsInt();
  }

  /**
   * Returns the BIGINT value to compare with the target column.
   *
   * @return the BIGINT value to compare with the target column
   */
  public long getBigIntValue() {
    return value.getAsLong();
  }

  /**
   * Returns the FLOAT value to compare with the target column.
   *
   * @return the FLOAT value to compare with the target column
   */
  public float getFloatValue() {
    return value.getAsFloat();
  }

  /**
   * Returns the DOUBLE value to compare with the target column.
   *
   * @return the DOUBLE value to compare with the target column
   */
  public double getDoubleValue() {
    return value.getAsDouble();
  }

  /**
   * Returns the TEXT value to compare with the target column.
   *
   * @return the TEXT value to compare with the target column
   */
  public String getTextValue() {
    return value.getAsString().orElse(null);
  }

  /**
   * Returns the BLOB value to compare with the target column as a ByteBuffer type.
   *
   * @return the BLOB value to compare with the target column as a ByteBuffer type
   */
  public ByteBuffer getBlobValue() {
    return getBlobValueAsByteBuffer();
  }

  /**
   * Returns the BLOB value to compare with the target column as a ByteBuffer type.
   *
   * @return the BLOB value to compare with the target column as a ByteBuffer type
   */
  public ByteBuffer getBlobValueAsByteBuffer() {
    return value.getAsByteBuffer().orElse(null);
  }

  /**
   * Returns the BLOB value to compare with the target column as a byte array type.
   *
   * @return the BLOB value to compare with the target column as a byte array type
   */
  public byte[] getBlobValueAsBytes() {
    return value.getAsBytes().orElse(null);
  }

  /**
   * Returns the value to compare with the target column as an Object type.
   *
   * <p>If the columns is a BOOLEAN type, it returns a {@code Boolean} object. If the columns is an
   * INT type, it returns an {@code Integer} object. If the columns is a BIGINT type, it returns a
   * {@code LONG} object. If the columns is a FLOAT type, it returns a {@code FLOAT} object. If the
   * columns is a DOUBLE type, it returns a {@code DOUBLE} object. If the columns is a TEXT type, it
   * returns a {@code String} object. If the columns is a BLOB type, it returns a {@code ByteBuffer}
   * object.
   *
   * @return the value to compare with the target column as an Object type
   */
  public Object getValueAsObject() {
    if (value instanceof BooleanValue) {
      return getBooleanValue();
    } else if (value instanceof IntValue) {
      return getIntValue();
    } else if (value instanceof BigIntValue) {
      return getBigIntValue();
    } else if (value instanceof FloatValue) {
      return getFloatValue();
    } else if (value instanceof DoubleValue) {
      return getDoubleValue();
    } else if (value instanceof TextValue) {
      return getTextValue();
    } else if (value instanceof BlobValue) {
      return getBlobValue();
    } else {
      throw new AssertionError();
    }
  }

  /**
   * Returns the operator used to compare the target column.
   *
   * @return the operator used to compare the target column
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
   *   <li>both instances have the same column name, value and operator.
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
    return columnName.equals(other.columnName)
        && value.equals(other.value)
        && operator.equals(other.operator);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnName, value, operator);
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
