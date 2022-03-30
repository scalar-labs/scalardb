package com.scalar.db.sql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class AssignmentTest {

  @Test
  public void column_ShouldBuildProperly() {
    // Arrange

    // Act
    Assignment actual = Assignment.column("col").value(Value.ofInt(10));

    // Assert
    assertThat(actual.columnName).isEqualTo("col");
    assertThat(actual.value).isEqualTo(Value.ofInt(10));
  }
}
