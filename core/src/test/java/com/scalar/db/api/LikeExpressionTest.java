package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.io.TextColumn;
import org.junit.jupiter.api.Test;

public class LikeExpressionTest {
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
    LikeExpression actual5 =
        new LikeExpression(TextColumn.of("col1", "++text"), Operator.LIKE, "+");
    LikeExpression actual6 =
        new LikeExpression(TextColumn.of("col1", "text++"), Operator.LIKE, "+");

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
    assertThat(actual5.getColumn()).isEqualTo(TextColumn.of("col1", "++text"));
    assertThat(actual5.getOperator()).isEqualTo(Operator.LIKE);
    assertThat(actual5.getEscape()).isEqualTo("+");
    assertThat(actual6.getColumn()).isEqualTo(TextColumn.of("col1", "text++"));
    assertThat(actual6.getOperator()).isEqualTo(Operator.LIKE);
    assertThat(actual6.getEscape()).isEqualTo("+");
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
    Throwable thrown5 =
        catchThrowable(
            () -> new LikeExpression(TextColumn.of("col1", "+++text"), Operator.LIKE, "+"));
    Throwable thrown6 =
        catchThrowable(
            () -> new LikeExpression(TextColumn.of("col1", "text+++"), Operator.LIKE, "+"));

    // Assert
    assertThat(thrown1).isInstanceOf(IllegalArgumentException.class);
    assertThat(thrown2).isInstanceOf(IllegalArgumentException.class);
    assertThat(thrown3).isInstanceOf(IllegalArgumentException.class);
    assertThat(thrown4).isInstanceOf(IllegalArgumentException.class);
    assertThat(thrown5).isInstanceOf(IllegalArgumentException.class);
    assertThat(thrown6).isInstanceOf(IllegalArgumentException.class);
  }
}
