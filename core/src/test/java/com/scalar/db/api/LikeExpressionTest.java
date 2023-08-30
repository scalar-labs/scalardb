package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.google.common.collect.ImmutableList;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.io.TextColumn;
import org.junit.jupiter.api.Test;

public class LikeExpressionTest {
  private LikeExpression prepareLike(String pattern) {
    return new LikeExpression(TextColumn.of("col1", pattern), Operator.LIKE);
  }

  private LikeExpression prepareLike(String pattern, String escape) {
    return new LikeExpression(TextColumn.of("col1", pattern), Operator.LIKE, escape);
  }

  private LikeExpression prepareNotLike(String pattern) {
    return new LikeExpression(TextColumn.of("col1", pattern), Operator.NOT_LIKE);
  }

  private LikeExpression prepareNotLike(String pattern, String escape) {
    return new LikeExpression(TextColumn.of("col1", pattern), Operator.NOT_LIKE, escape);
  }

  @Test
  public void constructor_ProperArgsGiven_ShouldConstructProperly() {
    // Arrange

    // Act
    LikeExpression actual1 = new LikeExpression(TextColumn.of("col1", "%text"), Operator.LIKE);
    LikeExpression actual2 =
        new LikeExpression(TextColumn.of("col1", "+%text"), Operator.LIKE, "+");
    LikeExpression actual3 = new LikeExpression(TextColumn.of("col1", "%text"), Operator.NOT_LIKE);
    LikeExpression actual4 =
        new LikeExpression(TextColumn.of("col1", "+%text"), Operator.NOT_LIKE, "+");

    // Assert
    assertThat(actual1.getColumn()).isEqualTo(TextColumn.of("col1", "%text"));
    assertThat(actual1.getOperator()).isEqualTo(Operator.LIKE);
    assertThat(actual1.getEscape()).isEqualTo("\\");
    assertThat(actual2.getColumn()).isEqualTo(TextColumn.of("col1", "+%text"));
    assertThat(actual2.getOperator()).isEqualTo(Operator.LIKE);
    assertThat(actual2.getEscape()).isEqualTo("+");
    assertThat(actual3.getColumn()).isEqualTo(TextColumn.of("col1", "%text"));
    assertThat(actual3.getOperator()).isEqualTo(Operator.NOT_LIKE);
    assertThat(actual3.getEscape()).isEqualTo("\\");
    assertThat(actual4.getColumn()).isEqualTo(TextColumn.of("col1", "+%text"));
    assertThat(actual4.getOperator()).isEqualTo(Operator.NOT_LIKE);
    assertThat(actual4.getEscape()).isEqualTo("+");
  }

  @Test
  public void constructor_IllegalOperatorGiven_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act
    Throwable thrown1 =
        catchThrowable(() -> new LikeExpression(TextColumn.of("col1", "%text"), Operator.EQ));
    Throwable thrown2 =
        catchThrowable(() -> new LikeExpression(TextColumn.of("col1", "+%text"), Operator.EQ, "+"));
    Throwable thrown3 =
        catchThrowable(
            () -> new LikeExpression(TextColumn.of("col1", "%text"), Operator.LIKE, null));
    Throwable thrown4 =
        catchThrowable(
            () -> new LikeExpression(TextColumn.of("col1", "++%text"), Operator.LIKE, "++"));

    // Assert
    assertThat(thrown1).isInstanceOf(IllegalArgumentException.class);
    assertThat(thrown2).isInstanceOf(IllegalArgumentException.class);
    assertThat(thrown3).isInstanceOf(IllegalArgumentException.class);
    assertThat(thrown4).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_IllegalLikePatternGiven_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act
    Throwable thrown1 =
        catchThrowable(() -> new LikeExpression(TextColumn.ofNull("col1"), Operator.LIKE));
    Throwable thrown2 =
        catchThrowable(() -> new LikeExpression(TextColumn.ofNull("col1"), Operator.LIKE, "+"));
    Throwable thrown3 =
        catchThrowable(
            () -> new LikeExpression(TextColumn.of("col1", "+text"), Operator.LIKE, "+"));
    Throwable thrown4 =
        catchThrowable(
            () -> new LikeExpression(TextColumn.of("col1", "text+"), Operator.LIKE, "+"));

    // Assert
    assertThat(thrown1).isInstanceOf(IllegalArgumentException.class);
    assertThat(thrown2).isInstanceOf(IllegalArgumentException.class);
    assertThat(thrown3).isInstanceOf(IllegalArgumentException.class);
    assertThat(thrown4).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void isMatchWith_SomePatternsWithoutEscapeGiven_ShouldReturnBooleanProperly() {
    // Arrange Act Assert
    // The following tests are added referring to the similar tests in Spark.
    // https://github.com/apache/spark/blob/master/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/RegexpExpressionsSuite.scala

    // simple patterns
    assertThat(prepareLike("abdef").isMatchedWith("abdef")).isTrue();
    assertThat(prepareLike("a\\__b").isMatchedWith("a_%b")).isTrue();
    assertThat(prepareLike("a_%b").isMatchedWith("addb")).isTrue();
    assertThat(prepareLike("a\\__b").isMatchedWith("addb")).isFalse();
    assertThat(prepareLike("a%\\%b").isMatchedWith("addb")).isFalse();
    assertThat(prepareLike("a%\\%b").isMatchedWith("a_%b")).isTrue();
    assertThat(prepareLike("a%").isMatchedWith("addb")).isTrue();
    assertThat(prepareLike("**").isMatchedWith("addb")).isFalse();
    assertThat(prepareLike("a%").isMatchedWith("abc")).isTrue();
    assertThat(prepareLike("b%").isMatchedWith("abc")).isFalse();
    assertThat(prepareLike("bc%").isMatchedWith("abc")).isFalse();
    assertThat(prepareLike("a_b").isMatchedWith("a\nb")).isTrue();
    assertThat(prepareLike("a%b").isMatchedWith("ab")).isTrue();
    assertThat(prepareLike("a%b").isMatchedWith("a\nb")).isTrue();

    // empty input
    assertThat(prepareLike("").isMatchedWith("")).isTrue();
    assertThat(prepareLike("").isMatchedWith("a")).isFalse();
    assertThat(prepareLike("a").isMatchedWith("")).isFalse();

    // SI-17647 double-escaping backslash
    assertThat(prepareLike("%\\\\%").isMatchedWith("\\\\\\\\")).isTrue();
    assertThat(prepareLike("%%").isMatchedWith("%%")).isTrue();
    assertThat(prepareLike("\\\\\\__").isMatchedWith("\\__")).isTrue();
    assertThat(prepareLike("%\\\\%\\%").isMatchedWith("\\\\\\__")).isFalse();
    assertThat(prepareLike("%\\\\").isMatchedWith("_\\\\\\%")).isFalse();

    // unicode
    assertThat(prepareLike("_\u20AC_").isMatchedWith("a\u20ACa")).isTrue();
    assertThat(prepareLike("_€_").isMatchedWith("a€a")).isTrue();
    assertThat(prepareLike("_\u20AC_").isMatchedWith("a€a")).isTrue();
    assertThat(prepareLike("_€_").isMatchedWith("a\u20ACa")).isTrue();

    // case
    assertThat(prepareLike("a%").isMatchedWith("A")).isFalse();
    assertThat(prepareLike("A%").isMatchedWith("a")).isFalse();
    assertThat(prepareLike("_a_").isMatchedWith("AaA")).isTrue();

    // example
    assertThat(
            prepareLike("\\%SystemDrive\\%\\\\Users%").isMatchedWith("%SystemDrive%\\Users\\John"))
        .isTrue();
  }

  @Test
  public void isMatchWith_SomePatternsWithEscapeGiven_ShouldReturnBooleanProperly() {
    // Arrange Act Assert
    // The following tests are added referring to the similar tests in Spark.
    // https://github.com/apache/spark/blob/master/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/RegexpExpressionsSuite.scala
    ImmutableList.of("/", "#", "\"")
        .forEach(
            escape -> {
              // simple patterns
              assertThat(prepareLike("abdef", escape).isMatchedWith("abdef")).isTrue();
              assertThat(prepareLike("a" + escape + "__b", escape).isMatchedWith("a_%b")).isTrue();
              assertThat(prepareLike("a_%b", escape).isMatchedWith("addb")).isTrue();
              assertThat(prepareLike("a" + escape + "__b", escape).isMatchedWith("addb")).isFalse();
              assertThat(prepareLike("a%" + escape + "%b", escape).isMatchedWith("addb")).isFalse();
              assertThat(prepareLike("a%" + escape + "%b", escape).isMatchedWith("a_%b")).isTrue();
              assertThat(prepareLike("a%", escape).isMatchedWith("addb")).isTrue();
              assertThat(prepareLike("**", escape).isMatchedWith("addb")).isFalse();
              assertThat(prepareLike("a%", escape).isMatchedWith("abc")).isTrue();
              assertThat(prepareLike("b%", escape).isMatchedWith("abc")).isFalse();
              assertThat(prepareLike("bc%", escape).isMatchedWith("abc")).isFalse();
              assertThat(prepareLike("a_b", escape).isMatchedWith("a\nb")).isTrue();
              assertThat(prepareLike("a%b", escape).isMatchedWith("ab")).isTrue();
              assertThat(prepareLike("a%b", escape).isMatchedWith("a\nb")).isTrue();

              // empty input
              assertThat(prepareLike("", escape).isMatchedWith("")).isTrue();
              assertThat(prepareLike("", escape).isMatchedWith("a")).isFalse();
              assertThat(prepareLike("a", escape).isMatchedWith("")).isFalse();

              // SI-17647 double-escaping backslash
              assertThat(
                      prepareLike(String.format("%%%s%s%%", escape, escape), escape)
                          .isMatchedWith(String.format("%s%s%s%s", escape, escape, escape, escape)))
                  .isTrue();
              assertThat(prepareLike("%%", escape).isMatchedWith("%%")).isTrue();
              assertThat(
                      prepareLike(String.format("%s%s%s__", escape, escape, escape), escape)
                          .isMatchedWith(String.format("%s__", escape)))
                  .isTrue();
              assertThat(
                      prepareLike(String.format("%%%s%s%%%s%%", escape, escape, escape), escape)
                          .isMatchedWith(String.format("%s%s%s__", escape, escape, escape)))
                  .isFalse();
              assertThat(
                      prepareLike(String.format("%%%s%s", escape, escape), escape)
                          .isMatchedWith(String.format("_%s%s%s%%", escape, escape, escape)))
                  .isFalse();

              // unicode
              assertThat(prepareLike("_\u20AC_", escape).isMatchedWith("a\u20ACa")).isTrue();
              assertThat(prepareLike("_€_", escape).isMatchedWith("a€a")).isTrue();
              assertThat(prepareLike("_\u20AC_", escape).isMatchedWith("a€a")).isTrue();
              assertThat(prepareLike("_€_", escape).isMatchedWith("a\u20ACa")).isTrue();

              // case
              assertThat(prepareLike("a%", escape).isMatchedWith("A")).isFalse();
              assertThat(prepareLike("A%", escape).isMatchedWith("a")).isFalse();
              assertThat(prepareLike("_a_", escape).isMatchedWith("AaA")).isTrue();

              // example
              assertThat(
                      prepareLike(
                              String.format(
                                  "%s%%SystemDrive%s%%%s%sUsers%%", escape, escape, escape, escape),
                              escape)
                          .isMatchedWith(
                              String.format("%%SystemDrive%%%sUsers%sJohn", escape, escape)))
                  .isTrue();
            });
  }

  @Test
  public void isMatchWith_IsNotLikeOperatorWithSomePatternsGiven_ShouldReturnBooleanProperly() {
    // Arrange Act Assert
    assertThat(prepareNotLike("abdef").isMatchedWith("abdef")).isFalse();
    assertThat(prepareNotLike("a\\__b").isMatchedWith("a_%b")).isFalse();
    assertThat(prepareNotLike("a_%b").isMatchedWith("addb")).isFalse();
    assertThat(prepareNotLike("a\\__b").isMatchedWith("addb")).isTrue();
    ImmutableList.of("/", "#", "\"")
        .forEach(
            escape -> {
              assertThat(prepareNotLike("abdef", escape).isMatchedWith("abdef")).isFalse();
              assertThat(prepareNotLike("a" + escape + "__b", escape).isMatchedWith("a_%b"))
                  .isFalse();
              assertThat(prepareNotLike("a_%b", escape).isMatchedWith("addb")).isFalse();
              assertThat(prepareNotLike("a" + escape + "__b", escape).isMatchedWith("addb"))
                  .isTrue();
            });
  }
}
