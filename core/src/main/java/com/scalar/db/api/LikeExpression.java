package com.scalar.db.api;

import com.google.common.base.MoreObjects;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.io.TextColumn;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

@Immutable
public class LikeExpression extends ConditionalExpression {
  private static final String DEFAULT_ESCAPE_CHAR = "\\";
  private final String escape;

  /**
   * Constructs a {@code LikeExpression} with the specified column and operator. For the escape
   * character, the default one ("\", i.e., backslash) is used.
   *
   * @param column a target column used to compare
   * @param operator an operator used to compare the target column
   */
  LikeExpression(TextColumn column, Operator operator) {
    this(column, operator, DEFAULT_ESCAPE_CHAR);
  }

  /**
   * Constructs a {@code LikeExpression} with the specified column, operator and escape character.
   * The escape character must be a string of a single character or an empty string. If an empty
   * string is specified, the escape character is disabled.
   *
   * @param column a target column used to compare
   * @param operator an operator used to compare the target column
   * @param escape an escape character for the like operator
   */
  LikeExpression(TextColumn column, Operator operator, String escape) {
    super(column, operator);
    check(column.getTextValue(), operator, escape);
    this.escape = escape;
  }

  private void check(String pattern, Operator operator, String escape) {
    if (operator != Operator.LIKE && operator != Operator.NOT_LIKE) {
      throw new IllegalArgumentException(
          CoreError.LIKE_CHECK_ERROR_OPERATOR_MUST_BE_LIKE_OR_NOT_LIKE.buildMessage(operator));
    }

    if (escape == null || escape.length() > 1) {
      throw new IllegalArgumentException(
          CoreError
              .LIKE_CHECK_ERROR_ESCAPE_CHARACTER_MUST_BE_STRING_OF_SINGLE_CHARACTER_OR_EMPTY_STRING
              .buildMessage());
    }

    if (pattern == null) {
      throw new IllegalArgumentException(
          CoreError.LIKE_CHECK_ERROR_LIKE_PATTERN_MUST_NOT_BE_NULL.buildMessage());
    }

    Character escapeChar = escape.isEmpty() ? null : escape.charAt(0);
    char[] chars = pattern.toCharArray();
    for (int i = 0; i < chars.length; i++) {
      char c = chars[i];
      if (escapeChar != null && c == escapeChar && i + 1 < chars.length) {
        char nextChar = chars[++i];
        if (nextChar != '_' && nextChar != '%' && nextChar != escapeChar) {
          throw new IllegalArgumentException(
              CoreError.LIKE_CHECK_ERROR_LIKE_PATTERN_MUST_NOT_INCLUDE_ONLY_ESCAPE_CHARACTER
                  .buildMessage());
        }
      } else if (escapeChar != null && c == escapeChar) {
        throw new IllegalArgumentException(
            CoreError.LIKE_CHECK_ERROR_LIKE_PATTERN_MUST_NOT_END_WITH_ESCAPE_CHARACTER
                .buildMessage());
      }
    }
  }

  /**
   * Returns the escape character for LIKE operator.
   *
   * @return the escape character for LIKE operator
   */
  @Nonnull
  public String getEscape() {
    return escape;
  }

  /**
   * Indicates whether some other object is "equal to" this object. The other object is considered
   * equal if:
   *
   * <ul>
   *   <li>both super class instances are equal and
   *   <li>it is also an {@code LikeExpression} and
   *   <li>both instances have the same escape character.
   * </ul>
   *
   * @param o an object to be tested for equality
   * @return {@code true} if the other object is "equal to" this object otherwise {@code false}
   */
  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    if (o == this) {
      return true;
    }
    if (!(o instanceof LikeExpression)) {
      return false;
    }
    LikeExpression other = (LikeExpression) o;
    return escape.equals(other.escape);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), escape);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("escape", escape).toString();
  }
}
