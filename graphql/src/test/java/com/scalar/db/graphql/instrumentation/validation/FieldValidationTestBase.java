package com.scalar.db.graphql.instrumentation.validation;

import static org.assertj.core.api.Assertions.assertThat;

import graphql.ErrorType;
import graphql.GraphQLError;
import graphql.language.SourceLocation;

public abstract class FieldValidationTestBase {
  protected void assertValidationError(GraphQLError error, int line, int column) {
    assertThat(error.getErrorType()).isEqualTo(ErrorType.ValidationError);
    assertThat(error.getLocations()).hasSize(1);
    SourceLocation location = error.getLocations().get(0);
    assertThat(location.getLine()).isEqualTo(line);
    assertThat(location.getColumn()).isEqualTo(column);
  }
}
