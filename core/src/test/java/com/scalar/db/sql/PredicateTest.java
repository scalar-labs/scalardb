package com.scalar.db.sql;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.sql.Predicate.Operator;
import org.junit.jupiter.api.Test;

public class PredicateTest {

  @Test
  public void column_ShouldBuildProperly() {
    // Arrange

    // Act
    Predicate predicate1 = Predicate.column("col1").isEqualTo(Value.ofInt(10));
    Predicate predicate2 = Predicate.column("col2").isGreaterThan(Value.ofInt(20));
    Predicate predicate3 = Predicate.column("col3").isGreaterThanOrEqualTo(Value.ofInt(30));
    Predicate predicate4 = Predicate.column("col4").isLessThan(Value.ofInt(40));
    Predicate predicate5 = Predicate.column("col5").isLessThanOrEqualTo(Value.ofInt(50));

    // Assert
    assertThat(predicate1.columnName).isEqualTo("col1");
    assertThat(predicate1.operator).isEqualTo(Operator.EQUAL_TO);
    assertThat(predicate1.value).isEqualTo(Value.ofInt(10));
    assertThat(predicate2.columnName).isEqualTo("col2");
    assertThat(predicate2.operator).isEqualTo(Operator.GREATER_THAN);
    assertThat(predicate2.value).isEqualTo(Value.ofInt(20));
    assertThat(predicate3.columnName).isEqualTo("col3");
    assertThat(predicate3.operator).isEqualTo(Operator.GREATER_THAN_OR_EQUAL_TO);
    assertThat(predicate3.value).isEqualTo(Value.ofInt(30));
    assertThat(predicate4.columnName).isEqualTo("col4");
    assertThat(predicate4.operator).isEqualTo(Operator.LESS_THAN);
    assertThat(predicate4.value).isEqualTo(Value.ofInt(40));
    assertThat(predicate5.columnName).isEqualTo("col5");
    assertThat(predicate5.operator).isEqualTo(Operator.LESS_THAN_OR_EQUAL_TO);
    assertThat(predicate5.value).isEqualTo(Value.ofInt(50));
  }

  @Test
  public void replaceValue_ShouldBuildProperly() {
    // Arrange
    Predicate predicate = Predicate.column("col1").isEqualTo(BindMarker.positional());

    // Act
    Predicate actual = predicate.replaceValue(Value.ofInt(20));

    // Assert
    assertThat(actual.columnName).isEqualTo("col1");
    assertThat(actual.operator).isEqualTo(Operator.EQUAL_TO);
    assertThat(actual.value).isEqualTo(Value.ofInt(20));
  }
}
