package com.scalar.db.api;

import com.scalar.db.api.ConditionalExpression.Operator;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ConditionBuilder {

  /**
   * Returns a builder object for a PutIf condition.
   *
   * @return a builder object
   */
  public static IfBuilderStart putIf() {
    return new IfBuilderStart(true);
  }

  /**
   * Creates a PutIfExists condition.
   *
   * @return a PutIfExists condition
   */
  public static MutationCondition putIfExists() {
    return new PutIfExists();
  }

  /**
   * Creates a PutIfNotExists condition.
   *
   * @return a PutIfNotExists condition
   */
  public static MutationCondition putIfNotExists() {
    return new PutIfNotExists();
  }

  /**
   * Returns a builder object for a DeleteIf condition.
   *
   * @return a builder object
   */
  public static IfBuilderStart deleteIf() {
    return new IfBuilderStart(false);
  }

  /**
   * Creates a DeleteIfExists condition.
   *
   * @return a DeleteIfExists condition
   */
  public static MutationCondition deleteIfExists() {
    return new DeleteIfExists();
  }

  public static class IfBuilderStart {

    // indicates whether it's for PutIf or DeleteIf. When true, it's for PutIf.
    private final boolean isPutIf;

    private IfBuilderStart(boolean isPutIf) {
      this.isPutIf = isPutIf;
    }

    /**
     * Adds an 'equal' conditional expression for a BOOLEAN value.
     *
     * @param columnName a name of target column for the 'equal' conditional expression
     * @param value a BOOLEAN value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder eqBoolean(String columnName, boolean value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.EQ));
    }

    /**
     * Adds an 'equal' conditional expression for an INT value.
     *
     * @param columnName a name of target column for the 'equal' conditional expression
     * @param value an INT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder eqInt(String columnName, int value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.EQ));
    }

    /**
     * Adds an 'equal' conditional expression for a BIGINT value.
     *
     * @param columnName a name of target column for the 'equal' conditional expression
     * @param value a BIGINT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder eqBigInt(String columnName, long value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.EQ));
    }

    /**
     * Adds an 'equal' conditional expression for a FLOAT value.
     *
     * @param columnName a name of target column for the 'equal' conditional expression
     * @param value a FLOAT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder eqFloat(String columnName, float value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.EQ));
    }

    /**
     * Adds an 'equal' conditional expression for a DOUBLE value.
     *
     * @param columnName a name of target column for the 'equal' conditional expression
     * @param value a DOUBLE value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder eqDouble(String columnName, double value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.EQ));
    }

    /**
     * Adds an 'equal' conditional expression for a TEXT value.
     *
     * @param columnName a name of target column for the 'equal' conditional expression
     * @param value a TEXT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder eqText(String columnName, String value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.EQ));
    }

    /**
     * Adds an 'equal' conditional expression for a BLOB value.
     *
     * @param columnName a name of target column for the 'equal' conditional expression
     * @param value a BLOB value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder eqBlob(String columnName, byte[] value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.EQ));
    }

    /**
     * Adds an 'equal' conditional expression for a BLOB value.
     *
     * @param columnName a name of target column for the 'equal' conditional expression
     * @param value a BLOB value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder eqBlob(String columnName, ByteBuffer value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.EQ));
    }

    /**
     * Adds a 'not equal' conditional expression for a BOOLEAN value.
     *
     * @param columnName a name of target column for the 'not equal' conditional expression
     * @param value a BOOLEAN value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder neBoolean(String columnName, boolean value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.NE));
    }

    /**
     * Adds a 'not equal' conditional expression for an INT value.
     *
     * @param columnName a name of target column for the 'not equal' conditional expression
     * @param value an INT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder neInt(String columnName, int value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.NE));
    }

    /**
     * Adds a 'not equal' conditional expression for a BIGINT value.
     *
     * @param columnName a name of target column for the 'not equal' conditional expression
     * @param value a BIGINT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder neBigInt(String columnName, long value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.NE));
    }

    /**
     * Adds a 'not equal' conditional expression for a FLOAT value.
     *
     * @param columnName a name of target column for the 'not equal' conditional expression
     * @param value a FLOAT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder neFloat(String columnName, float value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.NE));
    }

    /**
     * Adds a 'not equal' conditional expression for a DOUBLE value.
     *
     * @param columnName a name of target column for the 'not equal' conditional expression
     * @param value a DOUBLE value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder neDouble(String columnName, double value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.NE));
    }

    /**
     * Adds a 'not equal' conditional expression for a TEXT value.
     *
     * @param columnName a name of target column for the 'not equal' conditional expression
     * @param value a TEXT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder neText(String columnName, String value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.NE));
    }

    /**
     * Adds a 'not equal' conditional expression for a BLOB value.
     *
     * @param columnName a name of target column for the 'not equal' conditional expression
     * @param value a BLOB value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder neBlob(String columnName, byte[] value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.NE));
    }

    /**
     * Adds a 'not equal' conditional expression for a BLOB value.
     *
     * @param columnName a name of target column for the 'not equal' conditional expression
     * @param value a BLOB value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder neBlob(String columnName, ByteBuffer value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.NE));
    }

    /**
     * Adds a 'greater than' conditional expression for a BOOLEAN value.
     *
     * @param columnName a name of target column for the 'greater than' conditional expression
     * @param value a BOOLEAN value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder gtBoolean(String columnName, boolean value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.GT));
    }

    /**
     * Adds a 'greater than' conditional expression for an INT value.
     *
     * @param columnName a name of target column for the 'greater than' conditional expression
     * @param value an INT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder gtInt(String columnName, int value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.GT));
    }

    /**
     * Adds a 'greater than' conditional expression for a BIGINT value.
     *
     * @param columnName a name of target column for the 'greater than' conditional expression
     * @param value a BIGINT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder gtBigInt(String columnName, long value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.GT));
    }

    /**
     * Adds a 'greater than' conditional expression for a FLOAT value.
     *
     * @param columnName a name of target column for the 'greater than' conditional expression
     * @param value a FLOAT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder gtFloat(String columnName, float value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.GT));
    }

    /**
     * Adds a 'greater than' conditional expression for a DOUBLE value.
     *
     * @param columnName a name of target column for the 'greater than' conditional expression
     * @param value a DOUBLE value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder gtDouble(String columnName, double value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.GT));
    }

    /**
     * Adds a 'greater than' conditional expression for a TEXT value.
     *
     * @param columnName a name of target column for the 'greater than' conditional expression
     * @param value a TEXT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder gtText(String columnName, String value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.GT));
    }

    /**
     * Adds a 'greater than' conditional expression for a BLOB value.
     *
     * @param columnName a name of target column for the 'greater than' conditional expression
     * @param value a BLOB value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder gtBlob(String columnName, byte[] value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.GT));
    }

    /**
     * Adds a 'greater than' conditional expression for a BLOB value.
     *
     * @param columnName a name of target column for the 'greater than' conditional expression
     * @param value a BLOB value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder gtBlob(String columnName, ByteBuffer value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.GT));
    }

    /**
     * Adds a 'greater than or equal' conditional expression for a BOOLEAN value.
     *
     * @param columnName a name of target column for the 'greater than or equal' conditional
     *     expression
     * @param value a BOOLEAN value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder gteBoolean(String columnName, boolean value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.GTE));
    }

    /**
     * Adds a 'greater than or equal' conditional expression for an INT value.
     *
     * @param columnName a name of target column for the 'greater than or equal' conditional
     *     expression
     * @param value an INT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder gteInt(String columnName, int value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.GTE));
    }

    /**
     * Adds a 'greater than or equal' conditional expression for a BIGINT value.
     *
     * @param columnName a name of target column for the 'greater than or equal' conditional
     *     expression
     * @param value a BIGINT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder gteBigInt(String columnName, long value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.GTE));
    }

    /**
     * Adds a 'greater than or equal' conditional expression for a FLOAT value.
     *
     * @param columnName a name of target column for the 'greater than or equal' conditional
     *     expression
     * @param value a FLOAT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder gteFloat(String columnName, float value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.GTE));
    }

    /**
     * Adds a 'greater than or equal' conditional expression for a DOUBLE value.
     *
     * @param columnName a name of target column for the 'greater than or equal' conditional
     *     expression
     * @param value a DOUBLE value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder gteDouble(String columnName, double value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.GTE));
    }

    /**
     * Adds a 'greater than or equal' conditional expression for a TEXT value.
     *
     * @param columnName a name of target column for the 'greater than or equal' conditional
     *     expression
     * @param value a TEXT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder gteText(String columnName, String value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.GTE));
    }

    /**
     * Adds a 'greater than or equal' conditional expression for a BLOB value.
     *
     * @param columnName a name of target column for the 'greater than or equal' conditional
     *     expression
     * @param value a BLOB value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder gteBlob(String columnName, byte[] value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.GTE));
    }

    /**
     * Adds a 'greater than or equal' conditional expression for a BLOB value.
     *
     * @param columnName a name of target column for the 'greater than or equal' conditional
     *     expression
     * @param value a BLOB value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder gteBlob(String columnName, ByteBuffer value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.GTE));
    }

    /**
     * Adds a 'less than' conditional expression for a BOOLEAN value.
     *
     * @param columnName a name of target column for the 'less than' conditional expression
     * @param value a BOOLEAN value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder ltBoolean(String columnName, boolean value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.LT));
    }

    /**
     * Adds a 'less than' conditional expression for an INT value.
     *
     * @param columnName a name of target column for the 'less than' conditional expression
     * @param value an INT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder ltInt(String columnName, int value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.LT));
    }

    /**
     * Adds a 'less than' conditional expression for a BIGINT value.
     *
     * @param columnName a name of target column for the 'less than' conditional expression
     * @param value a BIGINT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder ltBigInt(String columnName, long value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.LT));
    }

    /**
     * Adds a 'less than' conditional expression for a FLOAT value.
     *
     * @param columnName a name of target column for the 'less than' conditional expression
     * @param value a FLOAT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder ltFloat(String columnName, float value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.LT));
    }

    /**
     * Adds a 'less than' conditional expression for a DOUBLE value.
     *
     * @param columnName a name of target column for the 'less than' conditional expression
     * @param value a DOUBLE value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder ltDouble(String columnName, double value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.LT));
    }

    /**
     * Adds a 'less than' conditional expression for a TEXT value.
     *
     * @param columnName a name of target column for the 'less than' conditional expression
     * @param value a TEXT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder ltText(String columnName, String value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.LT));
    }

    /**
     * Adds a 'less than' conditional expression for a BLOB value.
     *
     * @param columnName a name of target column for the 'less than' conditional expression
     * @param value a BLOB value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder ltBlob(String columnName, byte[] value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.LT));
    }

    /**
     * Adds a 'less than' conditional expression for a BLOB value.
     *
     * @param columnName a name of target column for the 'less than' conditional expression
     * @param value a BLOB value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder ltBlob(String columnName, ByteBuffer value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.LT));
    }

    /**
     * Adds a 'less than or equal' conditional expression for a BOOLEAN value.
     *
     * @param columnName a name of target column for the 'less than or equal' conditional expression
     * @param value a BOOLEAN value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder lteBoolean(String columnName, boolean value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.LTE));
    }

    /**
     * Adds a 'less than or equal' conditional expression for an INT value.
     *
     * @param columnName a name of target column for the 'less than or equal' conditional expression
     * @param value an INT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder lteInt(String columnName, int value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.LTE));
    }

    /**
     * Adds a 'less than or equal' conditional expression for a BIGINT value.
     *
     * @param columnName a name of target column for the 'less than or equal' conditional expression
     * @param value a BIGINT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder lteBigInt(String columnName, long value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.LTE));
    }

    /**
     * Adds a 'less than or equal' conditional expression for a FLOAT value.
     *
     * @param columnName a name of target column for the 'less than or equal' conditional expression
     * @param value a FLOAT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder lteFloat(String columnName, float value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.LTE));
    }

    /**
     * Adds a 'less than or equal' conditional expression for a DOUBLE value.
     *
     * @param columnName a name of target column for the 'less than or equal' conditional expression
     * @param value a DOUBLE value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder lteDouble(String columnName, double value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.LTE));
    }

    /**
     * Adds a 'less than or equal' conditional expression for a TEXT value.
     *
     * @param columnName a name of target column for the 'less than or equal' conditional expression
     * @param value a TEXT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder lteText(String columnName, String value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.LTE));
    }

    /**
     * Adds a 'less than or equal' conditional expression for a BLOB value.
     *
     * @param columnName a name of target column for the 'less than or equal' conditional expression
     * @param value a BLOB value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder lteBlob(String columnName, byte[] value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.LTE));
    }

    /**
     * Adds a 'less than or equal' conditional expression for a BLOB value.
     *
     * @param columnName a name of target column for the 'less than or equal' conditional expression
     * @param value a BLOB value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder lteBlob(String columnName, ByteBuffer value) {
      return new IfBuilder(isPutIf, new ConditionalExpression(columnName, value, Operator.LTE));
    }
  }

  public static class IfBuilder {

    // indicates whether it's for PutIf or DeleteIf. When true, it's for PutIf.
    private final boolean isPutIf;

    private final List<ConditionalExpression> conditionalExpressions;

    private IfBuilder(boolean isPutIf, ConditionalExpression firstConditionalExpression) {
      this.isPutIf = isPutIf;
      conditionalExpressions = new ArrayList<>();
      conditionalExpressions.add(firstConditionalExpression);
    }

    /**
     * Adds an 'equal' conditional expression for a BOOLEAN value.
     *
     * @param columnName a name of target column for the 'equal' conditional expression
     * @param value a BOOLEAN value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andEqBoolean(String columnName, boolean value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.EQ));
      return this;
    }

    /**
     * Adds an 'equal' conditional expression for an INT value.
     *
     * @param columnName a name of target column for the 'equal' conditional expression
     * @param value an INT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andEqInt(String columnName, int value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.EQ));
      return this;
    }

    /**
     * Adds an 'equal' conditional expression for a BIGINT value.
     *
     * @param columnName a name of target column for the 'equal' conditional expression
     * @param value a BIGINT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andEqBigInt(String columnName, long value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.EQ));
      return this;
    }

    /**
     * Adds an 'equal' conditional expression for a FLOAT value.
     *
     * @param columnName a name of target column for the 'equal' conditional expression
     * @param value a FLOAT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andEqFloat(String columnName, float value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.EQ));
      return this;
    }

    /**
     * Adds an 'equal' conditional expression for a DOUBLE value.
     *
     * @param columnName a name of target column for the 'equal' conditional expression
     * @param value a DOUBLE value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andEqDouble(String columnName, double value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.EQ));
      return this;
    }

    /**
     * Adds an 'equal' conditional expression for a TEXT value.
     *
     * @param columnName a name of target column for the 'equal' conditional expression
     * @param value a TEXT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andEqText(String columnName, String value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.EQ));
      return this;
    }

    /**
     * Adds an 'equal' conditional expression for a BLOB value.
     *
     * @param columnName a name of target column for the 'equal' conditional expression
     * @param value a BLOB value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andEqBlob(String columnName, byte[] value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.EQ));
      return this;
    }

    /**
     * Adds an 'equal' conditional expression for a BLOB value.
     *
     * @param columnName a name of target column for the 'equal' conditional expression
     * @param value a BLOB value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andEqBlob(String columnName, ByteBuffer value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.EQ));
      return this;
    }

    /**
     * Adds a 'not equal' conditional expression for a BOOLEAN value.
     *
     * @param columnName a name of target column for the 'not equal' conditional expression
     * @param value a BOOLEAN value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andNeBoolean(String columnName, boolean value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.NE));
      return this;
    }

    /**
     * Adds a 'not equal' conditional expression for an INT value.
     *
     * @param columnName a name of target column for the 'not equal' conditional expression
     * @param value an INT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andNeInt(String columnName, int value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.NE));
      return this;
    }

    /**
     * Adds a 'not equal' conditional expression for a BIGINT value.
     *
     * @param columnName a name of target column for the 'not equal' conditional expression
     * @param value a BIGINT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andNeBigInt(String columnName, long value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.NE));
      return this;
    }

    /**
     * Adds a 'not equal' conditional expression for a FLOAT value.
     *
     * @param columnName a name of target column for the 'not equal' conditional expression
     * @param value a FLOAT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andNeFloat(String columnName, float value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.NE));
      return this;
    }

    /**
     * Adds a 'not equal' conditional expression for a DOUBLE value.
     *
     * @param columnName a name of target column for the 'not equal' conditional expression
     * @param value a DOUBLE value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andNeDouble(String columnName, double value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.NE));
      return this;
    }

    /**
     * Adds a 'not equal' conditional expression for a TEXT value.
     *
     * @param columnName a name of target column for the 'not equal' conditional expression
     * @param value a TEXT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andNeText(String columnName, String value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.NE));
      return this;
    }

    /**
     * Adds a 'not equal' conditional expression for a BLOB value.
     *
     * @param columnName a name of target column for the 'not equal' conditional expression
     * @param value a BLOB value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andNeBlob(String columnName, byte[] value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.NE));
      return this;
    }

    /**
     * Adds a 'not equal' conditional expression for a BLOB value.
     *
     * @param columnName a name of target column for the 'not equal' conditional expression
     * @param value a BLOB value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andNeBlob(String columnName, ByteBuffer value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.NE));
      return this;
    }

    /**
     * Adds a 'greater than' conditional expression for a BOOLEAN value.
     *
     * @param columnName a name of target column for the 'greater than' conditional expression
     * @param value a BOOLEAN value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andGtBoolean(String columnName, boolean value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.GT));
      return this;
    }

    /**
     * Adds a 'greater than' conditional expression for an INT value.
     *
     * @param columnName a name of target column for the 'greater than' conditional expression
     * @param value an INT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andGtInt(String columnName, int value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.GT));
      return this;
    }

    /**
     * Adds a 'greater than' conditional expression for a BIGINT value.
     *
     * @param columnName a name of target column for the 'greater than' conditional expression
     * @param value a BIGINT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andGtBigInt(String columnName, long value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.GT));
      return this;
    }

    /**
     * Adds a 'greater than' conditional expression for a FLOAT value.
     *
     * @param columnName a name of target column for the 'greater than' conditional expression
     * @param value a FLOAT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andGtFloat(String columnName, float value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.GT));
      return this;
    }

    /**
     * Adds a 'greater than' conditional expression for a DOUBLE value.
     *
     * @param columnName a name of target column for the 'greater than' conditional expression
     * @param value a DOUBLE value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andGtDouble(String columnName, double value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.GT));
      return this;
    }

    /**
     * Adds a 'greater than' conditional expression for a TEXT value.
     *
     * @param columnName a name of target column for the 'greater than' conditional expression
     * @param value a TEXT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andGtText(String columnName, String value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.GT));
      return this;
    }

    /**
     * Adds a 'greater than' conditional expression for a BLOB value.
     *
     * @param columnName a name of target column for the 'greater than' conditional expression
     * @param value a BLOB value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andGtBlob(String columnName, byte[] value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.GT));
      return this;
    }

    /**
     * Adds a 'greater than' conditional expression for a BLOB value.
     *
     * @param columnName a name of target column for the 'greater than' conditional expression
     * @param value a BLOB value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andGtBlob(String columnName, ByteBuffer value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.GT));
      return this;
    }

    /**
     * Adds a 'greater than or equal' conditional expression for a BOOLEAN value.
     *
     * @param columnName a name of target column for the 'greater than or equal' conditional
     *     expression
     * @param value a BOOLEAN value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andGteBoolean(String columnName, boolean value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.GTE));
      return this;
    }

    /**
     * Adds a 'greater than or equal' conditional expression for an INT value.
     *
     * @param columnName a name of target column for the 'greater than or equal' conditional
     *     expression
     * @param value an INT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andGteInt(String columnName, int value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.GTE));
      return this;
    }

    /**
     * Adds a 'greater than or equal' conditional expression for a BIGINT value.
     *
     * @param columnName a name of target column for the 'greater than or equal' conditional
     *     expression
     * @param value a BIGINT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andGteBigInt(String columnName, long value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.GTE));
      return this;
    }

    /**
     * Adds a 'greater than or equal' conditional expression for a FLOAT value.
     *
     * @param columnName a name of target column for the 'greater than or equal' conditional
     *     expression
     * @param value a FLOAT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andGteFloat(String columnName, float value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.GTE));
      return this;
    }

    /**
     * Adds a 'greater than or equal' conditional expression for a DOUBLE value.
     *
     * @param columnName a name of target column for the 'greater than or equal' conditional
     *     expression
     * @param value a DOUBLE value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andGteDouble(String columnName, double value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.GTE));
      return this;
    }

    /**
     * Adds a 'greater than or equal' conditional expression for a TEXT value.
     *
     * @param columnName a name of target column for the 'greater than or equal' conditional
     *     expression
     * @param value a TEXT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andGteText(String columnName, String value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.GTE));
      return this;
    }

    /**
     * Adds a 'greater than or equal' conditional expression for a BLOB value.
     *
     * @param columnName a name of target column for the 'greater than or equal' conditional
     *     expression
     * @param value a BLOB value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andGteBlob(String columnName, byte[] value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.GTE));
      return this;
    }

    /**
     * Adds a 'greater than or equal' conditional expression for a BLOB value.
     *
     * @param columnName a name of target column for the 'greater than or equal' conditional
     *     expression
     * @param value a BLOB value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andGteBlob(String columnName, ByteBuffer value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.GTE));
      return this;
    }

    /**
     * Adds a 'less than' conditional expression for a BOOLEAN value.
     *
     * @param columnName a name of target column for the 'less than' conditional expression
     * @param value a BOOLEAN value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andLtBoolean(String columnName, boolean value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.LT));
      return this;
    }

    /**
     * Adds a 'less than' conditional expression for an INT value.
     *
     * @param columnName a name of target column for the 'less than' conditional expression
     * @param value an INT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andLtInt(String columnName, int value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.LT));
      return this;
    }

    /**
     * Adds a 'less than' conditional expression for a BIGINT value.
     *
     * @param columnName a name of target column for the 'less than' conditional expression
     * @param value a BIGINT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andLtBigInt(String columnName, long value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.LT));
      return this;
    }

    /**
     * Adds a 'less than' conditional expression for a FLOAT value.
     *
     * @param columnName a name of target column for the 'less than' conditional expression
     * @param value a FLOAT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andLtFloat(String columnName, float value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.LT));
      return this;
    }

    /**
     * Adds a 'less than' conditional expression for a DOUBLE value.
     *
     * @param columnName a name of target column for the 'less than' conditional expression
     * @param value a DOUBLE value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andLtDouble(String columnName, double value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.LT));
      return this;
    }

    /**
     * Adds a 'less than' conditional expression for a TEXT value.
     *
     * @param columnName a name of target column for the 'less than' conditional expression
     * @param value a TEXT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andLtText(String columnName, String value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.LT));
      return this;
    }

    /**
     * Adds a 'less than' conditional expression for a BLOB value.
     *
     * @param columnName a name of target column for the 'less than' conditional expression
     * @param value a BLOB value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andLtBlob(String columnName, byte[] value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.LT));
      return this;
    }

    /**
     * Adds a 'less than' conditional expression for a BLOB value.
     *
     * @param columnName a name of target column for the 'less than' conditional expression
     * @param value a BLOB value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andLtBlob(String columnName, ByteBuffer value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.LT));
      return this;
    }

    /**
     * Adds a 'less than or equal' conditional expression for a BOOLEAN value.
     *
     * @param columnName a name of target column for the 'less than or equal' conditional expression
     * @param value a BOOLEAN value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andLteBoolean(String columnName, boolean value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.LTE));
      return this;
    }

    /**
     * Adds a 'less than or equal' conditional expression for an INT value.
     *
     * @param columnName a name of target column for the 'less than or equal' conditional expression
     * @param value an INT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andLteInt(String columnName, int value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.LTE));
      return this;
    }

    /**
     * Adds a 'less than or equal' conditional expression for a BIGINT value.
     *
     * @param columnName a name of target column for the 'less than or equal' conditional expression
     * @param value a BIGINT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andLteBigInt(String columnName, long value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.LTE));
      return this;
    }

    /**
     * Adds a 'less than or equal' conditional expression for a FLOAT value.
     *
     * @param columnName a name of target column for the 'less than or equal' conditional expression
     * @param value a FLOAT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andLteFloat(String columnName, float value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.LTE));
      return this;
    }

    /**
     * Adds a 'less than or equal' conditional expression for a DOUBLE value.
     *
     * @param columnName a name of target column for the 'less than or equal' conditional expression
     * @param value a DOUBLE value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andLteDouble(String columnName, double value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.LTE));
      return this;
    }

    /**
     * Adds a 'less than or equal' conditional expression for a TEXT value.
     *
     * @param columnName a name of target column for the 'less than or equal' conditional expression
     * @param value a TEXT value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andLteText(String columnName, String value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.LTE));
      return this;
    }

    /**
     * Adds a 'less than or equal' conditional expression for a BLOB value.
     *
     * @param columnName a name of target column for the 'less than or equal' conditional expression
     * @param value a BLOB value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andLteBlob(String columnName, byte[] value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.LTE));
      return this;
    }

    /**
     * Adds a 'less than or equal' conditional expression for a BLOB value.
     *
     * @param columnName a name of target column for the 'less than or equal' conditional expression
     * @param value a BLOB value used to compare with the target column
     * @return a builder object
     */
    public IfBuilder andLteBlob(String columnName, ByteBuffer value) {
      conditionalExpressions.add(new ConditionalExpression(columnName, value, Operator.LTE));
      return this;
    }

    /**
     * Builds a condition with the specified conditional expressions.
     *
     * @return a condition
     */
    public MutationCondition build() {
      if (isPutIf) {
        return new PutIf(conditionalExpressions);
      } else {
        return new DeleteIf(conditionalExpressions);
      }
    }
  }
}
