package com.scalar.db.api;

import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.TextColumn;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ConditionBuilder {

  /**
   * Returns a builder object for a PutIf condition.
   *
   * @param conditionalExpression a condition expression for a PutIf condition
   * @return a builder object
   */
  public static PutIfBuilder putIf(ConditionalExpression conditionalExpression) {
    return new PutIfBuilder(conditionalExpression);
  }

  /**
   * Creates a PutIf condition with the specified conditional expressions.
   *
   * @param conditionalExpressions condition expressions for a PutIf condition
   * @return a PutIf condition
   */
  public static PutIf putIf(List<ConditionalExpression> conditionalExpressions) {
    return new PutIf(conditionalExpressions);
  }

  /**
   * Creates a PutIfExists condition.
   *
   * @return a PutIfExists condition
   */
  public static PutIfExists putIfExists() {
    return new PutIfExists();
  }

  /**
   * Creates a PutIfNotExists condition.
   *
   * @return a PutIfNotExists condition
   */
  public static PutIfNotExists putIfNotExists() {
    return new PutIfNotExists();
  }

  /**
   * Returns a builder object for a DeleteIf condition.
   *
   * @param conditionalExpression a condition expression for a DeleteIf condition
   * @return a builder object
   */
  public static DeleteIfBuilder deleteIf(ConditionalExpression conditionalExpression) {
    return new DeleteIfBuilder(conditionalExpression);
  }

  /**
   * Creates a DeleteIf condition with the specified conditional expressions.
   *
   * @param conditionalExpressions condition expressions for a DeleteIf condition
   * @return a DeleteIf condition
   */
  public static DeleteIf deleteIf(List<ConditionalExpression> conditionalExpressions) {
    return new DeleteIf(conditionalExpressions);
  }

  /**
   * Creates a DeleteIfExists condition.
   *
   * @return a DeleteIfExists condition
   */
  public static DeleteIfExists deleteIfExists() {
    return new DeleteIfExists();
  }

  /**
   * Returns a builder object for a UpdateIf condition.
   *
   * @param conditionalExpression a condition expression for a UpdateIf condition
   * @return a builder object
   */
  public static UpdateIfBuilder updateIf(ConditionalExpression conditionalExpression) {
    return new UpdateIfBuilder(conditionalExpression);
  }

  /**
   * Creates a UpdateIf condition with the specified conditional expressions.
   *
   * @param conditionalExpressions condition expressions for a UpdateIf condition
   * @return a UpdateIf condition
   */
  public static UpdateIf updateIf(List<ConditionalExpression> conditionalExpressions) {
    return new UpdateIf(conditionalExpressions);
  }

  /**
   * Builds a conditional expression with the specified column and operator.
   *
   * <p>This method is primarily for internal use. Breaking changes can and will be introduced to
   * this method. Users should not depend on it.
   *
   * @param column a target column used to compare
   * @param operator an operator used to compare the target column
   * @return a conditional expression
   */
  public static ConditionalExpression buildConditionalExpression(
      Column<?> column, Operator operator) {
    return new ConditionalExpression(column, operator);
  }

  /**
   * Returns a builder object for a condition expression for PutIf/DeleteIf
   *
   * @param columnName a column name for a condition expression
   * @return a builder object
   */
  public static ConditionalExpressionBuilder column(String columnName) {
    return new ConditionalExpressionBuilder(columnName);
  }

  public static class ConditionalExpressionBuilder {

    private final String columnName;

    private ConditionalExpressionBuilder(String columnName) {
      this.columnName = columnName;
    }

    /**
     * Creates an 'equal' conditional expression for a BOOLEAN value.
     *
     * @param value a BOOLEAN value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isEqualToBoolean(boolean value) {
      return new ConditionalExpression(columnName, value, Operator.EQ);
    }

    /**
     * Creates an 'equal' conditional expression for an INT value.
     *
     * @param value an INT value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isEqualToInt(int value) {
      return new ConditionalExpression(columnName, value, Operator.EQ);
    }

    /**
     * Creates an 'equal' conditional expression for a BIGINT value.
     *
     * @param value a BIGINT value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isEqualToBigInt(long value) {
      return new ConditionalExpression(columnName, value, Operator.EQ);
    }

    /**
     * Creates an 'equal' conditional expression for a FLOAT value.
     *
     * @param value a FLOAT value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isEqualToFloat(float value) {
      return new ConditionalExpression(columnName, value, Operator.EQ);
    }

    /**
     * Creates an 'equal' conditional expression for a DOUBLE value.
     *
     * @param value a DOUBLE value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isEqualToDouble(double value) {
      return new ConditionalExpression(columnName, value, Operator.EQ);
    }

    /**
     * Creates an 'equal' conditional expression for a TEXT value.
     *
     * @param value a TEXT value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isEqualToText(String value) {
      return new ConditionalExpression(columnName, value, Operator.EQ);
    }

    /**
     * Creates an 'equal' conditional expression for a BLOB value.
     *
     * @param value a BLOB value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isEqualToBlob(byte[] value) {
      return new ConditionalExpression(columnName, value, Operator.EQ);
    }

    /**
     * Creates an 'equal' conditional expression for a BLOB value.
     *
     * @param value a BLOB value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isEqualToBlob(ByteBuffer value) {
      return new ConditionalExpression(columnName, value, Operator.EQ);
    }

    /**
     * Creates a 'not equal' conditional expression for a BOOLEAN value.
     *
     * @param value a BOOLEAN value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isNotEqualToBoolean(boolean value) {
      return new ConditionalExpression(columnName, value, Operator.NE);
    }

    /**
     * Creates a 'not equal' conditional expression for an INT value.
     *
     * @param value an INT value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isNotEqualToInt(int value) {
      return new ConditionalExpression(columnName, value, Operator.NE);
    }

    /**
     * Creates a 'not equal' conditional expression for a BIGINT value.
     *
     * @param value a BIGINT value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isNotEqualToBigInt(long value) {
      return new ConditionalExpression(columnName, value, Operator.NE);
    }

    /**
     * Creates a 'not equal' conditional expression for a FLOAT value.
     *
     * @param value a FLOAT value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isNotEqualToFloat(float value) {
      return new ConditionalExpression(columnName, value, Operator.NE);
    }

    /**
     * Creates a 'not equal' conditional expression for a DOUBLE value.
     *
     * @param value a DOUBLE value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isNotEqualToDouble(double value) {
      return new ConditionalExpression(columnName, value, Operator.NE);
    }

    /**
     * Creates a 'not equal' conditional expression for a TEXT value.
     *
     * @param value a TEXT value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isNotEqualToText(String value) {
      return new ConditionalExpression(columnName, value, Operator.NE);
    }

    /**
     * Creates a 'not equal' conditional expression for a BLOB value.
     *
     * @param value a BLOB value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isNotEqualToBlob(byte[] value) {
      return new ConditionalExpression(columnName, value, Operator.NE);
    }

    /**
     * Creates a 'not equal' conditional expression for a BLOB value.
     *
     * @param value a BLOB value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isNotEqualToBlob(ByteBuffer value) {
      return new ConditionalExpression(columnName, value, Operator.NE);
    }

    /**
     * Creates a 'greater than' conditional expression for a BOOLEAN value.
     *
     * @param value a BOOLEAN value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isGreaterThanBoolean(boolean value) {
      return new ConditionalExpression(columnName, value, Operator.GT);
    }

    /**
     * Creates a 'greater than' conditional expression for an INT value.
     *
     * @param value an INT value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isGreaterThanInt(int value) {
      return new ConditionalExpression(columnName, value, Operator.GT);
    }

    /**
     * Creates a 'greater than' conditional expression for a BIGINT value.
     *
     * @param value a BIGINT value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isGreaterThanBigInt(long value) {
      return new ConditionalExpression(columnName, value, Operator.GT);
    }

    /**
     * Creates a 'greater than' conditional expression for a FLOAT value.
     *
     * @param value a FLOAT value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isGreaterThanFloat(float value) {
      return new ConditionalExpression(columnName, value, Operator.GT);
    }

    /**
     * Creates a 'greater than' conditional expression for a DOUBLE value.
     *
     * @param value a DOUBLE value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isGreaterThanDouble(double value) {
      return new ConditionalExpression(columnName, value, Operator.GT);
    }

    /**
     * Creates a 'greater than' conditional expression for a TEXT value.
     *
     * @param value a TEXT value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isGreaterThanText(String value) {
      return new ConditionalExpression(columnName, value, Operator.GT);
    }

    /**
     * Creates a 'greater than' conditional expression for a BLOB value.
     *
     * @param value a BLOB value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isGreaterThanBlob(byte[] value) {
      return new ConditionalExpression(columnName, value, Operator.GT);
    }

    /**
     * Creates a 'greater than' conditional expression for a BLOB value.
     *
     * @param value a BLOB value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isGreaterThanBlob(ByteBuffer value) {
      return new ConditionalExpression(columnName, value, Operator.GT);
    }

    /**
     * Creates a 'greater than or equal' conditional expression for a BOOLEAN value.
     *
     * @param value a BOOLEAN value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isGreaterThanOrEqualToBoolean(boolean value) {
      return new ConditionalExpression(columnName, value, Operator.GTE);
    }

    /**
     * Creates a 'greater than or equal' conditional expression for an INT value.
     *
     * @param value an INT value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isGreaterThanOrEqualToInt(int value) {
      return new ConditionalExpression(columnName, value, Operator.GTE);
    }

    /**
     * Creates a 'greater than or equal' conditional expression for a BIGINT value.
     *
     * @param value a BIGINT value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isGreaterThanOrEqualToBigInt(long value) {
      return new ConditionalExpression(columnName, value, Operator.GTE);
    }

    /**
     * Creates a 'greater than or equal' conditional expression for a FLOAT value.
     *
     * @param value a FLOAT value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isGreaterThanOrEqualToFloat(float value) {
      return new ConditionalExpression(columnName, value, Operator.GTE);
    }

    /**
     * Creates a 'greater than or equal' conditional expression for a DOUBLE value.
     *
     * @param value a DOUBLE value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isGreaterThanOrEqualToDouble(double value) {
      return new ConditionalExpression(columnName, value, Operator.GTE);
    }

    /**
     * Creates a 'greater than or equal' conditional expression for a TEXT value.
     *
     * @param value a TEXT value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isGreaterThanOrEqualToText(String value) {
      return new ConditionalExpression(columnName, value, Operator.GTE);
    }

    /**
     * Creates a 'greater than or equal' conditional expression for a BLOB value.
     *
     * @param value a BLOB value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isGreaterThanOrEqualToBlob(byte[] value) {
      return new ConditionalExpression(columnName, value, Operator.GTE);
    }

    /**
     * Creates a 'greater than or equal' conditional expression for a BLOB value.
     *
     * @param value a BLOB value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isGreaterThanOrEqualToBlob(ByteBuffer value) {
      return new ConditionalExpression(columnName, value, Operator.GTE);
    }

    /**
     * Creates a 'less than' conditional expression for a BOOLEAN value.
     *
     * @param value a BOOLEAN value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isLessThanBoolean(boolean value) {
      return new ConditionalExpression(columnName, value, Operator.LT);
    }

    /**
     * Creates a 'less than' conditional expression for an INT value.
     *
     * @param value an INT value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isLessThanInt(int value) {
      return new ConditionalExpression(columnName, value, Operator.LT);
    }

    /**
     * Creates a 'less than' conditional expression for a BIGINT value.
     *
     * @param value a BIGINT value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isLessThanBigInt(long value) {
      return new ConditionalExpression(columnName, value, Operator.LT);
    }

    /**
     * Creates a 'less than' conditional expression for a FLOAT value.
     *
     * @param value a FLOAT value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isLessThanFloat(float value) {
      return new ConditionalExpression(columnName, value, Operator.LT);
    }

    /**
     * Creates a 'less than' conditional expression for a DOUBLE value.
     *
     * @param value a DOUBLE value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isLessThanDouble(double value) {
      return new ConditionalExpression(columnName, value, Operator.LT);
    }

    /**
     * Creates a 'less than' conditional expression for a TEXT value.
     *
     * @param value a TEXT value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isLessThanText(String value) {
      return new ConditionalExpression(columnName, value, Operator.LT);
    }

    /**
     * Creates a 'less than' conditional expression for a BLOB value.
     *
     * @param value a BLOB value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isLessThanBlob(byte[] value) {
      return new ConditionalExpression(columnName, value, Operator.LT);
    }

    /**
     * Creates a 'less than' conditional expression for a BLOB value.
     *
     * @param value a BLOB value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isLessThanBlob(ByteBuffer value) {
      return new ConditionalExpression(columnName, value, Operator.LT);
    }

    /**
     * Creates a 'less than or equal' conditional expression for a BOOLEAN value.
     *
     * @param value a BOOLEAN value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isLessThanOrEqualToBoolean(boolean value) {
      return new ConditionalExpression(columnName, value, Operator.LTE);
    }

    /**
     * Creates a 'less than or equal' conditional expression for an INT value.
     *
     * @param value an INT value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isLessThanOrEqualToInt(int value) {
      return new ConditionalExpression(columnName, value, Operator.LTE);
    }

    /**
     * Creates a 'less than or equal' conditional expression for a BIGINT value.
     *
     * @param value a BIGINT value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isLessThanOrEqualToBigInt(long value) {
      return new ConditionalExpression(columnName, value, Operator.LTE);
    }

    /**
     * Creates a 'less than or equal' conditional expression for a FLOAT value.
     *
     * @param value a FLOAT value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isLessThanOrEqualToFloat(float value) {
      return new ConditionalExpression(columnName, value, Operator.LTE);
    }

    /**
     * Creates a 'less than or equal' conditional expression for a DOUBLE value.
     *
     * @param value a DOUBLE value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isLessThanOrEqualToDouble(double value) {
      return new ConditionalExpression(columnName, value, Operator.LTE);
    }

    /**
     * Creates a 'less than or equal' conditional expression for a TEXT value.
     *
     * @param value a TEXT value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isLessThanOrEqualToText(String value) {
      return new ConditionalExpression(columnName, value, Operator.LTE);
    }

    /**
     * Creates a 'less than or equal' conditional expression for a BLOB value.
     *
     * @param value a BLOB value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isLessThanOrEqualToBlob(byte[] value) {
      return new ConditionalExpression(columnName, value, Operator.LTE);
    }

    /**
     * Creates a 'less than or equal' conditional expression for a BLOB value.
     *
     * @param value a BLOB value used to compare with the target column
     * @return a conditional expression
     */
    public ConditionalExpression isLessThanOrEqualToBlob(ByteBuffer value) {
      return new ConditionalExpression(columnName, value, Operator.LTE);
    }

    /**
     * Creates a 'is null' conditional expression for a BOOLEAN value.
     *
     * @return a conditional expression
     */
    public ConditionalExpression isNullBoolean() {
      return new ConditionalExpression(BooleanColumn.ofNull(columnName), Operator.IS_NULL);
    }

    /**
     * Creates a 'is null' conditional expression for an INT value.
     *
     * @return a conditional expression
     */
    public ConditionalExpression isNullInt() {
      return new ConditionalExpression(IntColumn.ofNull(columnName), Operator.IS_NULL);
    }

    /**
     * Creates a 'is null' conditional expression for a BIGINT value.
     *
     * @return a conditional expression
     */
    public ConditionalExpression isNullBigInt() {
      return new ConditionalExpression(BigIntColumn.ofNull(columnName), Operator.IS_NULL);
    }

    /**
     * Creates a 'is null' conditional expression for a FLOAT value.
     *
     * @return a conditional expression
     */
    public ConditionalExpression isNullFloat() {
      return new ConditionalExpression(FloatColumn.ofNull(columnName), Operator.IS_NULL);
    }

    /**
     * Creates a 'is null' conditional expression for a DOUBLE value.
     *
     * @return a conditional expression
     */
    public ConditionalExpression isNullDouble() {
      return new ConditionalExpression(DoubleColumn.ofNull(columnName), Operator.IS_NULL);
    }

    /**
     * Creates a 'is null' conditional expression for a TEXT value.
     *
     * @return a conditional expression
     */
    public ConditionalExpression isNullText() {
      return new ConditionalExpression(TextColumn.ofNull(columnName), Operator.IS_NULL);
    }

    /**
     * Creates a 'is null' conditional expression for a BLOB value.
     *
     * @return a conditional expression
     */
    public ConditionalExpression isNullBlob() {
      return new ConditionalExpression(BlobColumn.ofNull(columnName), Operator.IS_NULL);
    }

    /**
     * Creates a 'is not null' conditional expression for a BOOLEAN value.
     *
     * @return a conditional expression
     */
    public ConditionalExpression isNotNullBoolean() {
      return new ConditionalExpression(BooleanColumn.ofNull(columnName), Operator.IS_NOT_NULL);
    }

    /**
     * Creates a 'is not null' conditional expression for an INT value.
     *
     * @return a conditional expression
     */
    public ConditionalExpression isNotNullInt() {
      return new ConditionalExpression(IntColumn.ofNull(columnName), Operator.IS_NOT_NULL);
    }

    /**
     * Creates a 'is not null' conditional expression for a BIGINT value.
     *
     * @return a conditional expression
     */
    public ConditionalExpression isNotNullBigInt() {
      return new ConditionalExpression(BigIntColumn.ofNull(columnName), Operator.IS_NOT_NULL);
    }

    /**
     * Creates a 'is not null' conditional expression for a FLOAT value.
     *
     * @return a conditional expression
     */
    public ConditionalExpression isNotNullFloat() {
      return new ConditionalExpression(FloatColumn.ofNull(columnName), Operator.IS_NOT_NULL);
    }

    /**
     * Creates a 'is not null' conditional expression for a DOUBLE value.
     *
     * @return a conditional expression
     */
    public ConditionalExpression isNotNullDouble() {
      return new ConditionalExpression(DoubleColumn.ofNull(columnName), Operator.IS_NOT_NULL);
    }

    /**
     * Creates a 'is not null' conditional expression for a TEXT value.
     *
     * @return a conditional expression
     */
    public ConditionalExpression isNotNullText() {
      return new ConditionalExpression(TextColumn.ofNull(columnName), Operator.IS_NOT_NULL);
    }

    /**
     * Creates a 'is not null' conditional expression for a BLOB value.
     *
     * @return a conditional expression
     */
    public ConditionalExpression isNotNullBlob() {
      return new ConditionalExpression(BlobColumn.ofNull(columnName), Operator.IS_NOT_NULL);
    }

    /**
     * Creates a 'like' conditional expression for a TEXT value. For the escape character, the
     * default one ("\", i.e., backslash) is used.
     *
     * @param value a TEXT value used to compare with the target column
     * @return a like conditional expression
     */
    public LikeExpression isLikeText(String value) {
      return new LikeExpression(TextColumn.of(columnName, value), Operator.LIKE);
    }

    /**
     * Creates a 'like' conditional expression for a TEXT value with an escape character. The escape
     * character must be a string of a single character or an empty string. If an empty string is
     * specified, the escape character is disabled.
     *
     * @param value a pattern used to compare with the target column
     * @param escape an escape character used in the pattern
     * @return a like conditional expression
     */
    public LikeExpression isLikeText(String value, String escape) {
      return new LikeExpression(TextColumn.of(columnName, value), Operator.LIKE, escape);
    }

    /**
     * Creates a 'not like' conditional expression for a TEXT value.
     *
     * @param value a TEXT value used to compare with the target column
     * @return a not-like conditional expression
     */
    public LikeExpression isNotLikeText(String value) {
      return new LikeExpression(TextColumn.of(columnName, value), Operator.NOT_LIKE);
    }

    /**
     * Creates a 'not like' conditional expression for a TEXT value with escape a character.
     *
     * @param value a pattern used to compare with the target column
     * @param escape an escape character used in the pattern
     * @return a not-like like conditional expression
     */
    public LikeExpression isNotLikeText(String value, String escape) {
      return new LikeExpression(TextColumn.of(columnName, value), Operator.NOT_LIKE, escape);
    }
  }

  public static class PutIfBuilder {

    private final List<ConditionalExpression> conditionalExpressions;

    private PutIfBuilder(ConditionalExpression conditionalExpression) {
      check(conditionalExpression);
      conditionalExpressions = new ArrayList<>();
      conditionalExpressions.add(conditionalExpression);
    }

    /**
     * Adds a condition for a PutIf condition.
     *
     * @param conditionalExpression a condition for a PutIf condition
     * @return a builder object
     */
    public PutIfBuilder and(ConditionalExpression conditionalExpression) {
      check(conditionalExpression);
      conditionalExpressions.add(conditionalExpression);
      return this;
    }

    /**
     * Builds a PutIf condition with the specified conditional expressions.
     *
     * @return a PutIf condition
     */
    public PutIf build() {
      return new PutIf(conditionalExpressions);
    }

    private void check(ConditionalExpression conditionalExpression) {
      if (conditionalExpression.getOperator().equals(Operator.LIKE)
          || conditionalExpression.getOperator().equals(Operator.NOT_LIKE)) {
        throw new IllegalArgumentException(
            CoreError.CONDITION_BUILD_ERROR_CONDITION_NOT_ALLOWED_FOR_PUT_IF.buildMessage(
                conditionalExpression));
      }
    }
  }

  public static class DeleteIfBuilder {

    private final List<ConditionalExpression> conditionalExpressions;

    private DeleteIfBuilder(ConditionalExpression conditionalExpression) {
      check(conditionalExpression);
      conditionalExpressions = new ArrayList<>();
      conditionalExpressions.add(conditionalExpression);
    }

    /**
     * Adds a condition for a DeleteIf condition.
     *
     * @param conditionalExpression a condition for a DeleteIf condition
     * @return a builder object
     */
    public DeleteIfBuilder and(ConditionalExpression conditionalExpression) {
      check(conditionalExpression);
      conditionalExpressions.add(conditionalExpression);
      return this;
    }

    /**
     * Builds a DeleteIf condition with the specified conditional expressions.
     *
     * @return a DeleteIf condition
     */
    public DeleteIf build() {
      return new DeleteIf(conditionalExpressions);
    }

    private void check(ConditionalExpression conditionalExpression) {
      if (conditionalExpression.getOperator().equals(Operator.LIKE)
          || conditionalExpression.getOperator().equals(Operator.NOT_LIKE)) {
        throw new IllegalArgumentException(
            CoreError.CONDITION_BUILD_ERROR_CONDITION_NOT_ALLOWED_FOR_DELETE_IF.buildMessage(
                conditionalExpression));
      }
    }
  }

  public static class UpdateIfBuilder {

    private final List<ConditionalExpression> conditionalExpressions;

    private UpdateIfBuilder(ConditionalExpression conditionalExpression) {
      check(conditionalExpression);
      conditionalExpressions = new ArrayList<>();
      conditionalExpressions.add(conditionalExpression);
    }

    /**
     * Adds a condition for a UpdateIf condition.
     *
     * @param conditionalExpression a condition for a UpdateIf condition
     * @return a builder object
     */
    public UpdateIfBuilder and(ConditionalExpression conditionalExpression) {
      check(conditionalExpression);
      conditionalExpressions.add(conditionalExpression);
      return this;
    }

    /**
     * Builds a UpdateIf condition with the specified conditional expressions.
     *
     * @return a UpdateIf condition
     */
    public UpdateIf build() {
      return new UpdateIf(conditionalExpressions);
    }

    private void check(ConditionalExpression conditionalExpression) {
      if (conditionalExpression.getOperator().equals(Operator.LIKE)
          || conditionalExpression.getOperator().equals(Operator.NOT_LIKE)) {
        throw new IllegalArgumentException(
            CoreError.CONDITION_BUILD_ERROR_CONDITION_NOT_ALLOWED_FOR_UPDATE_IF.buildMessage(
                conditionalExpression));
      }
    }
  }
}
