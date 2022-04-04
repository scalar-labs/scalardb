package com.scalar.db.sql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class ProjectionTest {

  @Test
  public void column_ShouldBuildProperly() {
    // Arrange

    // Act
    Projection actual = Projection.column("col");

    // Assert
    assertThat(actual.columnName).isEqualTo("col");
    assertThat(actual.alias).isNull();
  }

  @Test
  public void as_ShouldBuildProperly() {
    // Arrange

    // Act
    Projection actual = Projection.column("col1").as("col2");

    // Assert
    assertThat(actual.columnName).isEqualTo("col1");
    assertThat(actual.alias).isEqualTo("col2");
  }
}
