package com.scalar.db.graphql.instrumentation.validation;

import static org.assertj.core.api.Assertions.assertThat;

import graphql.ErrorType;
import graphql.GraphQLError;

public abstract class FieldValidationTestBase {
  protected void assertValidationError(GraphQLError error, int line, int column) {
    assertThat(error.getErrorType()).isEqualTo(ErrorType.ValidationError);
    assertThat(error.getLocations())
        .hasOnlyOneElementSatisfying(
            location -> {
              assertThat(location.getLine()).isEqualTo(line);
              assertThat(location.getColumn()).isEqualTo(column);
            });
  }
}
