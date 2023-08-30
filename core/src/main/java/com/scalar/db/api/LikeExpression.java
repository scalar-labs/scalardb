package com.scalar.db.api;

import com.google.common.base.MoreObjects;
import com.scalar.db.io.TextColumn;
import java.util.Objects;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

@Immutable
public class LikeExpression extends ConditionalExpression {
  private static final String DEFAULT_ESCAPE_CHAR = "\\";
  @Nonnull private final String escape;
  @Nonnull private final String regexPattern;

  /**
   * Constructs a {@code LikeExpression} with the specified column and operator.
   *
   * @param column a target column used to compare
   * @param operator an operator used to compare the target column
   */
  LikeExpression(TextColumn column, Operator operator) {
    this(column, operator, DEFAULT_ESCAPE_CHAR);
  }

  /**
   * Constructs a {@code LikeExpression} with the specified column, operator and escape character.
   *
   * @param column a target column used to compare
   * @param operator an operator used to compare the target column
   * @param escape an escape character for the like operator
   */
  LikeExpression(TextColumn column, Operator operator, String escape) {
    super(column, operator);
    if (operator != Operator.LIKE && operator != Operator.NOT_LIKE) {
      throw new IllegalArgumentException("Operator must be like or not-like");
    }
    if (escape == null || escape.length() > 1) {
      throw new IllegalArgumentException(
          "Escape character must be a string of a single character or an empty string");
    }
    this.escape = escape;
    this.regexPattern =
        convertRegexPatternFrom(
            column.getTextValue(), escape.length() == 0 ? null : escape.charAt(0));
  }

  /**
   * Validate and convert SQL 'like' pattern to a Java regular expression. Underscores (_) are
   * converted to '.' and percent signs (%) are converted to '.*', other characters are quoted
   * literally. If an escape character specified, escaping is done for '_', '%', and the escape
   * character itself. An invalid pattern will throw {@code IllegalArgumentException}. This method
   * is implemented referencing the following Spark SQL implementation.
   * https://github.com/apache/spark/blob/a8eadebd686caa110c4077f4199d11e797146dc5/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/StringUtils.scala
   *
   * @param likePattern a SQL LIKE pattern to convert
   * @param escape an escape character.
   * @return the equivalent Java regular expression of the given pattern
   */
  private String convertRegexPatternFrom(String likePattern, Character escape) {
    if (likePattern == null) {
      throw new IllegalArgumentException("LIKE pattern must not be null");
    }

    StringBuilder out = new StringBuilder();
    char[] chars = likePattern.toCharArray();
    for (int i = 0; i < chars.length; i++) {
      char c = chars[i];
      if (escape != null && c == escape && i + 1 < chars.length) {
        char nextChar = chars[++i];
        if (nextChar == '_' || nextChar == '%') {
          out.append(Pattern.quote(Character.toString(nextChar)));
        } else if (nextChar == escape) {
          out.append(Pattern.quote(Character.toString(nextChar)));
        } else {
          throw new IllegalArgumentException("LIKE pattern must not include only escape character");
        }
      } else if (escape != null && c == escape) {
        throw new IllegalArgumentException("LIKE pattern must not end with escape character");
      } else if (c == '_') {
        out.append(".");
      } else if (c == '%') {
        out.append(".*");
      } else {
        out.append(Pattern.quote(Character.toString(c)));
      }
    }

    return "(?s)" + out; // (?s) enables dotall mode, causing "." to match new lines
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
   * Returns true if the LIKE pattern matches a given {@code String}.
   *
   * @return true if the LIKE pattern matches a given {@code String}
   */
  public boolean isMatchedWith(String value) {
    if (getOperator().equals(Operator.LIKE)) {
      return value != null && Pattern.compile(regexPattern).matcher(value).matches();
    } else {
      return value != null && !Pattern.compile(regexPattern).matcher(value).matches();
    }
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
