package com.scalar.db.api;

import com.google.common.base.MoreObjects;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.Value;
import com.scalar.db.util.ScalarDbUtils;
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
  private final Column<?> column;
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
    this(ScalarDbUtils.toColumn(value).copyWith(columnName), operator);
  }

  /**
   * Constructs a {@code ConditionalExpression} with the specified column and operator.
   *
   * @param column a target column used to compare
   * @param operator an operator used to compare the target column
   */
  ConditionalExpression(Column<?> column, Operator operator) {
    this.column = column;
    this.operator = operator;
  }

  /**
   * Constructs a {@code ConditionalExpression} with the specified column name, value and operator.
   *
   * @param columnName a name of target column
   * @param booleanValue a BOOLEAN value used to compare with the target column
   * @param operator an operator used to compare the target column
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link ConditionBuilder}
   *     to build a condition instead
   */
  @Deprecated
  public ConditionalExpression(String columnName, boolean booleanValue, Operator operator) {
    this(BooleanColumn.of(columnName, booleanValue), operator);
  }

  /**
   * Constructs a {@code ConditionalExpression} with the specified column name, value and operator.
   *
   * @param columnName a name of target column
   * @param intValue an INT value used to compare with the target column
   * @param operator an operator used to compare the target column
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link ConditionBuilder}
   *     to build a condition instead
   */
  @Deprecated
  public ConditionalExpression(String columnName, int intValue, Operator operator) {
    this(IntColumn.of(columnName, intValue), operator);
  }

  /**
   * Constructs a {@code ConditionalExpression} with the specified column name, value and operator.
   *
   * @param columnName a name of target column
   * @param bigIntValue a BIGINT value used to compare with the target column
   * @param operator an operator used to compare the target column
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link ConditionBuilder}
   *     to build a condition instead
   */
  @Deprecated
  public ConditionalExpression(String columnName, long bigIntValue, Operator operator) {
    this(BigIntColumn.of(columnName, bigIntValue), operator);
  }

  /**
   * Constructs a {@code ConditionalExpression} with the specified column name, value and operator.
   *
   * @param columnName a name of target column
   * @param floatValue a FLOAT value used to compare with the target column
   * @param operator an operator used to compare the target column
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link ConditionBuilder}
   *     to build a condition instead
   */
  @Deprecated
  public ConditionalExpression(String columnName, float floatValue, Operator operator) {
    this(FloatColumn.of(columnName, floatValue), operator);
  }

  /**
   * Constructs a {@code ConditionalExpression} with the specified column name, value and operator.
   *
   * @param columnName a name of target column
   * @param doubleValue a DOUBLE value used to compare with the target column
   * @param operator an operator used to compare the target column
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link ConditionBuilder}
   *     to build a condition instead
   */
  @Deprecated
  public ConditionalExpression(String columnName, double doubleValue, Operator operator) {
    this(DoubleColumn.of(columnName, doubleValue), operator);
  }

  /**
   * Constructs a {@code ConditionalExpression} with the specified column name, value and operator.
   *
   * @param columnName a name of target column
   * @param textValue a TEXT value used to compare with the target column
   * @param operator an operator used to compare the target column
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link ConditionBuilder}
   *     to build a condition instead
   */
  @Deprecated
  public ConditionalExpression(String columnName, String textValue, Operator operator) {
    this(TextColumn.of(columnName, textValue), operator);
  }

  /**
   * Constructs a {@code ConditionalExpression} with the specified column name, value and operator.
   *
   * @param columnName a name of target column
   * @param blobValue a BLOB value used to compare with the target column
   * @param operator an operator used to compare the target column
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link ConditionBuilder}
   *     to build a condition instead
   */
  @Deprecated
  public ConditionalExpression(String columnName, byte[] blobValue, Operator operator) {
    this(BlobColumn.of(columnName, blobValue), operator);
  }

  /**
   * Constructs a {@code ConditionalExpression} with the specified column name, value and operator.
   *
   * @param columnName a name of target column
   * @param blobValue a BLOB value used to compare with the target column
   * @param operator an operator used to compare the target column
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0. Use {@link ConditionBuilder}
   *     to build a condition instead
   */
  @Deprecated
  public ConditionalExpression(String columnName, ByteBuffer blobValue, Operator operator) {
    this(BlobColumn.of(columnName, blobValue), operator);
  }

  /**
   * Returns the column name of target column.
   *
   * @return the column name of target column
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  public String getName() {
    return column.getName();
  }

  /**
   * Returns the value used to compare with the target column.
   *
   * @return the value used to compare with the target column
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  public Value<?> getValue() {
    return ScalarDbUtils.toValue(column).copyWith("");
  }

  /**
   * Returns the target column used to compare.
   *
   * <p>This method is primarily for internal use. Breaking changes can and will be introduced to
   * this method. Users should not depend on it.
   *
   * @return the target column
   */
  public Column<?> getColumn() {
    return column;
  }

  /**
   * Returns the BOOLEAN value to compare with the target column.
   *
   * @return the BOOLEAN value to compare with the target column
   */
  public boolean getBooleanValue() {
    return column.getBooleanValue();
  }

  /**
   * Returns the INT value to compare with the target column.
   *
   * @return the INT value to compare with the target column
   */
  public int getIntValue() {
    return column.getIntValue();
  }

  /**
   * Returns the BIGINT value to compare with the target column.
   *
   * @return the BIGINT value to compare with the target column
   */
  public long getBigIntValue() {
    return column.getBigIntValue();
  }

  /**
   * Returns the FLOAT value to compare with the target column.
   *
   * @return the FLOAT value to compare with the target column
   */
  public float getFloatValue() {
    return column.getFloatValue();
  }

  /**
   * Returns the DOUBLE value to compare with the target column.
   *
   * @return the DOUBLE value to compare with the target column
   */
  public double getDoubleValue() {
    return column.getDoubleValue();
  }

  /**
   * Returns the TEXT value to compare with the target column.
   *
   * @return the TEXT value to compare with the target column
   */
  public String getTextValue() {
    return column.getTextValue();
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
    return column.getBlobValueAsByteBuffer();
  }

  /**
   * Returns the BLOB value to compare with the target column as a byte array type.
   *
   * @return the BLOB value to compare with the target column as a byte array type
   */
  public byte[] getBlobValueAsBytes() {
    return column.getBlobValueAsBytes();
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
    return column.getValueAsObject();
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
    return column.equals(other.column) && operator.equals(other.operator);
  }

  @Override
  public int hashCode() {
    return Objects.hash(column, operator);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("column", column)
        .add("operator", operator)
        .toString();
  }

  public enum Operator {
    EQ,
    NE,
    GT,
    GTE,
    LT,
    LTE,
    IS_NULL,
    IS_NOT_NULL,
    LIKE,
    NOT_LIKE,
  }
}
